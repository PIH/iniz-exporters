#!/usr/bin/env python3
#
# Pulls all of the concept data from the database and works on it
# in-memory. This ends up being quite fast, but could be a problem
# for a large concept database and constrained memory.
#

import argparse
import csv
import os
import queue
from typing import List, Set, Optional
from collections import OrderedDict
import subprocess as sp
from packaging import version

DESCRIPTION = """
A program for exporting concepts from an OpenMRS MySQL database to
CSVs that can be loaded by the OpenMRS Initializer module.
"""

# Globals -- modified only during initialization
VERBOSE = False
DOCKER = False
VERSION = "2.3"
LOCALES = ["en"]
DEFAULT_LOCALE = "en"
ENCODING = "UTF-8"
CONCEPT_KEY_MAPPING: Optional[str] = None
MAPPING_TYPES: List[str] = [
    "SAME-AS",
    "NARROWER-THAN",
    "BROADER-THAN",
    "Associated finding",
    "Associated morphology",
    "Associated procedure",
    "Associated with",
    "Causative agent",
    "Finding site",
    "Has specimen",
    "Laterality",
    "Severity",
]  # taken from Version 1 of https://wiki.openmrs.org/pages/viewpage.action?pageId=235276418

# These must be set before running run_sql
DB_NAME = ""
USER = ""
PASSWORD = ""

# Defaults
OUTFILE_DEFAULT_BASENAME = os.path.expanduser("~/Downloads/concepts")
NAME_TYPES_DEFAULT = ["full", "short"]

# Constants
NAME_TYPE_INIZ_NAMES = {"full": "Fully specified name", "short": "Short name"}


def set_globals(
    database: str,
    verbose: bool = VERBOSE,
    docker: bool = DOCKER,
    runtime_properties_path: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    version: str = VERSION,
    locales: list = LOCALES,
    encoding: str = ENCODING,
    concept_key_mapping: Optional[str] = CONCEPT_KEY_MAPPING,
):
    """
    Initializes the global variables used in this script.
    Defaults are as described in `concept_csv_export.py --help`.
    """
    global VERBOSE, DB_NAME, DOCKER, USER, PASSWORD, VERSION, DEFAULT_LOCALE, LOCALES, ENCODING, CONCEPT_KEY_MAPPING
    VERBOSE = verbose
    DB_NAME = database
    DOCKER = docker
    VERSION = version
    LOCALES = locales
    DEFAULT_LOCALE = locales[0]
    ENCODING = encoding
    CONCEPT_KEY_MAPPING = concept_key_mapping

    USER = user or get_command_output(
        'grep connection.username {} | cut -f2 -d"="'.format(
            runtime_properties_path
            or ("~/openmrs/" + DB_NAME + "/openmrs-runtime.properties")
        )
    )
    assert (
        USER != ""
    ), "Failed to extract connection.username from openmrs-runtime.properties, and it was not provided"

    PASSWORD = password or get_command_output(
        'grep connection.password {} | cut -f2 -d"="'.format(
            runtime_properties_path
            or "~/openmrs/" + DB_NAME + "/openmrs-runtime.properties"
        )
    )
    assert (
        PASSWORD != ""
    ), "Failed to extract connection.password from openmrs-runtime.properties, and it was not provided"


def main(
    database: str,
    set_name: Optional[str],
    docker: bool = DOCKER,
    locales: list = LOCALES,
    name_types: list = NAME_TYPES_DEFAULT,
    outfile: str = "",  # default is set in the function
    verbose: bool = VERBOSE,
    runtime_properties_path: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    version: str = VERSION,
    exclude_files: List[str] = None,
    encoding: str = ENCODING,
    concept_key_mapping=CONCEPT_KEY_MAPPING,
):
    set_globals(
        database=database,
        verbose=verbose,
        docker=docker,
        runtime_properties_path=runtime_properties_path,
        user=user,
        password=password,
        version=version,
        locales=locales,
        encoding=encoding,
        concept_key_mapping=concept_key_mapping,
    )
    if not outfile:
        outfile = (
            OUTFILE_DEFAULT_BASENAME
            + ("-" + squish_name(set_name) if set_name else "")
            + ".csv"
        )

    check_data_for_stop_characters()

    limit = None  # just here in case needed for experimenting
    all_concepts = get_all_concepts(name_types=name_types, limit=limit)
    print("  There are {} total concepts".format(len(all_concepts)))
    if set_name:
        concepts = get_all_concepts_in_tree(all_concepts, set_name)
        print("  There are {} concepts in this tree".format(len(concepts)))
    else:
        concepts = all_concepts
    detect_cycles(concepts)
    print("Reordering")
    concepts = move_referring_concepts_down(concepts, get_key())
    if exclude_files:
        print("Filtering out excludes")
        excludes = get_excludes_from_files(exclude_files)
        concepts = exclude(concepts, excludes)
    print("Writing {} concepts to output file {}".format(len(concepts), outfile))
    with open(outfile, "w") as f:
        keys = get_columns(name_types, concepts)
        filtered_concepts = [
            {k: (c[k] if k != "Void/Retire" else None) for k in keys} for c in concepts
        ]
        writer = csv.DictWriter(f, keys)
        writer.writeheader()
        writer.writerows(filtered_concepts)


def check_data_for_stop_characters():
    """Warns the user if, for fields that can be stored in CSV with multiple
    values with some delimiter, some entry for that field contains the
    delimiter.
    """
    crt_query = (
        "SELECT crt.concept_reference_term_id, crs.name, crt.code "
        "FROM concept_reference_term crt "
        "JOIN concept_reference_source crs "
        "  ON crt.concept_source_id = crs.concept_source_id "
        "WHERE crt.code LIKE '%;%';"
    )
    result = sql_result_to_list_of_ordered_dicts(run_sql(crt_query))
    if result:
        print(
            "WARNING: The following concept reference terms contain "
            "the Initializer stop character ';' (semicolon). If the "
            "corresponding concepts appear in your CSV export, they "
            "will fail to be loaded because the 'Mappings' "
            "field will be malformed."  # TODO: replace them with periods
        )
        for item in result:
            print(item)

    name_query = (
        "SELECT concept_id, name "
        "FROM concept_name "
        "WHERE locale = 'en' "
        "  AND concept_name_type = 'FULLY_SPECIFIED' "
        "  AND voided = 0 "
        "  AND name LIKE '%;%';"
    )
    result = sql_result_to_list_of_ordered_dicts(run_sql(name_query))
    if result:
        print(
            "WARNING: The following concept's fully specified English "
            "names contain the Initializer stop character ';' (semicolon). "
            "If they are members or answers of any concept that is being "
            "exported, they will cause export to fail."
            # TODO: replace them with periods
        )
        for item in result:
            print(item)


def get_all_concepts(name_types: list, limit: Optional[int]) -> list:
    """Queries all concepts from the database and sticks them into a list."""
    sql_code = get_sql_code(name_types=name_types, limit=limit)
    if VERBOSE:
        print(sql_code)
        input("Press any key to continue...")

    print("Querying concepts...")
    sql_result = run_sql(sql_code)
    if VERBOSE:
        print(sql_result)
        input("Press any key to continue...")
    print("Parsing results...")
    all_concepts = sql_result_to_list_of_ordered_dicts(sql_result)
    append_key_mapping(all_concepts)
    return all_concepts


def get_sql_code(name_types: list, limit: Optional[int] = None, where: str = "") -> str:
    """Produces the SQL query to run to get all the concepts."""

    def locale_select_snippet(name_types: list, locale: str):

        snippets = []
        for name_type in name_types:
            snippets.append(
                " MAX(cn_{l}_{t}.name) '{iniz_name}:{l}' ".format(
                    l=locale, t=name_type, iniz_name=NAME_TYPE_INIZ_NAMES[name_type]
                )
            )
        return ", ".join(snippets)

    def locale_join_snippet(name_types: list, locale: str):
        name_type_sql_names = {"full": "FULLY_SPECIFIED", "short": "SHORT"}

        snippets = []
        for name_type in name_types:
            snippets.append(
                " LEFT JOIN concept_name cn_{l}_{t} "
                "ON c.concept_id = cn_{l}_{t}.concept_id "
                "AND cn_{l}_{t}.locale = '{l}' "
                "AND cn_{l}_{t}.concept_name_type = '{sql_name}' "
                "AND cn_{l}_{t}.voided = 0".format(
                    l=locale,
                    t=name_type,
                    sql_name=name_type_sql_names[name_type],
                )
            )

        return "\n    ".join(snippets)

    def type_name_transform(mapping_type: str):
        return mapping_type.replace("-", "_").replace(" ", "_")

    def mapping_select_snippet(mapping_type: str):
        return "GROUP_CONCAT(DISTINCT term_source_name_{tn}, ':', term_code_{tn} SEPARATOR ';') 'Mappings|{t}'\n".format(
            t=mapping_type, tn=type_name_transform(mapping_type)
        )

    def mapping_join_snippet(mapping_type: str):
        return (
            "LEFT JOIN (SELECT crm.concept_id, source.name term_source_name_{tn}, crt.code term_code_{tn} FROM concept_reference_map crm \n"
            "           JOIN concept_map_type map_type ON crm.concept_map_type_id = map_type.concept_map_type_id AND map_type.name = '{t}' \n"
            "           JOIN concept_reference_term crt ON crm.concept_reference_term_id = crt.concept_reference_term_id AND crt.retired = 0 \n"
            "           JOIN concept_reference_source source ON crt.concept_source_id = source.concept_source_id) term_{tn} \n"
            "   ON c.concept_id = term_{tn}.concept_id \n"
        ).format(t=mapping_type, tn=type_name_transform(mapping_type))

    select = (
        "SET SESSION group_concat_max_len = 1000000; "
        "SELECT c.uuid, MAX(cd.description) 'Description:"
        + DEFAULT_LOCALE
        + "', MAX(cl.name) 'Data class', MAX(dt.name) 'Data type', "
        + ", ".join([mapping_select_snippet(mapping_type=t) for t in MAPPING_TYPES])
        + ", "
        + ", ".join(
            [locale_select_snippet(name_types=name_types, locale=l) for l in LOCALES]
        )
        + ", MAX(c_num.hi_absolute) 'Absolute high'"
        ", MAX(c_num.hi_critical) 'Critical high'"
        ", MAX(c_num.hi_normal) 'Normal high'"
        ", MAX(c_num.low_absolute) 'Absolute low'"
        ", MAX(c_num.low_critical) 'Critical low'"
        ", MAX(c_num.low_normal) 'Normal low'"
        ", MAX(c_num.units) 'Units'"
        + (
            (
                ", MAX(c_num.display_precision) 'Display precision'"
                ", MAX(c_num."
                + (
                    "allow_decimal"
                    if version.parse(VERSION) >= version.parse("2.2")
                    else "precise"
                )
                + ") 'Allow decimals'"
            )
            if version.parse(VERSION) >= version.parse("1.11")
            else ""
        )
        + ", MAX(c_cx.handler) 'Complex data handler'"
        ", GROUP_CONCAT(DISTINCT set_mem_name.name ORDER BY c_set.sort_weight ASC SEPARATOR ';') 'Members' "
        ", GROUP_CONCAT(DISTINCT ans_name.name ORDER BY c_ans.sort_weight ASC SEPARATOR ';') 'Answers' "
    )

    tables = (
        "FROM concept c \n"
        "JOIN concept_class cl ON c.class_id = cl.concept_class_id \n"
        "JOIN concept_datatype dt ON c.datatype_id = dt.concept_datatype_id \n"
        "LEFT JOIN concept_description cd ON c.concept_id = cd.concept_id AND cd.locale = '"
        + DEFAULT_LOCALE
        + "' \n"
        + "\n ".join([mapping_join_snippet(mapping_type=t) for t in MAPPING_TYPES])
        + "\n ".join(
            [locale_join_snippet(name_types=name_types, locale=l) for l in LOCALES]
        )
        + "\nLEFT JOIN concept_numeric c_num ON c.concept_id = c_num.concept_id "
        "LEFT JOIN concept_complex c_cx ON c.concept_id = c_cx.concept_id \n"
        "LEFT JOIN concept_set c_set ON c.concept_id = c_set.concept_set \n"
        "  LEFT JOIN concept c_set_c ON c_set.concept_id = c_set_c.concept_id AND c_set_c.retired = 0 \n"  # we look up the concept to filter out the retired members
        "  LEFT JOIN concept_name set_mem_name ON c_set_c.concept_id = set_mem_name.concept_id \n"
        "    AND set_mem_name.locale = '"
        + DEFAULT_LOCALE
        + "' AND set_mem_name.concept_name_type = 'FULLY_SPECIFIED' AND set_mem_name.voided = 0 \n"
        "LEFT JOIN concept_answer c_ans ON c.concept_id = c_ans.concept_id \n"
        "  LEFT JOIN concept c_ans_c ON c_ans.answer_concept = c_ans_c.concept_id AND c_ans_c.retired = 0 \n"  # we look up the concept to filter out the retired answers
        "  LEFT JOIN concept_name ans_name ON c_ans_c.concept_id = ans_name.concept_id \n"
        "    AND ans_name.locale = '"
        + DEFAULT_LOCALE
        + "' AND ans_name.concept_name_type = 'FULLY_SPECIFIED' AND ans_name.voided = 0 \n"
    )

    ending = (
        "WHERE c.retired = 0  {where_part} "
        "GROUP BY c.concept_id, c.is_set, c.uuid "
        "ORDER BY c.is_set {limit_part} "
    ).format(
        limit_part="LIMIT {}".format(limit) if limit != None else "",
        where_part="AND {}".format(where) if where != "" else "",
    )

    sql_code = select + "\n" + tables + "\n" + ending + ";"
    return sql_code


def append_key_mapping(all_concepts: list):
    if CONCEPT_KEY_MAPPING:
        for concept in all_concepts:
            for mapping in concept["Mappings|SAME-AS"].split(";"):
                if mapping:
                    mapping_parts = mapping.split(":")
                    if mapping_parts[0] == CONCEPT_KEY_MAPPING:
                        concept["_mapping:" + mapping_parts[0]] = mapping_parts[1]
            if "_mapping:" + CONCEPT_KEY_MAPPING not in concept:
                raise IndexError(
                    "The following concept does not have a non-retired mapping for source '"
                    + CONCEPT_KEY_MAPPING
                    + "': "
                    + str(concept)
                )


def get_all_concepts_in_tree(all_concepts: list, set_name: str) -> list:
    """Filters a list of concepts for decendants of set_name

    "Descendants" means answers and set members (or members of members, etc.)
    """
    key = get_key()
    all_concepts_by_name = {c[key]: c for c in all_concepts}
    concept_names_to_add: queue.SimpleQueue[str] = queue.SimpleQueue()
    concept_names_to_add.put(set_name)
    concept_names_in_tree: Set[str] = set()
    iteration = 0
    while True:
        if VERBOSE:
            iteration += 1
            print(
                "Iteration {}. {} concepts in tree.".format(
                    iteration, len(concept_names_in_tree)
                )
            )
        try:
            concept_name = concept_names_to_add.get_nowait()
        except queue.Empty:
            break
        concept_names_in_tree.add(concept_name)
        concept = all_concepts_by_name[concept_name]
        members = concept["Members"].split(";")
        answers = concept["Answers"].split(";")
        for name in members + answers:
            if name != "" and name not in concept_names_in_tree:
                concept_names_to_add.put(name)

    return [all_concepts_by_name[cn] for cn in concept_names_in_tree]


def get_excludes_from_files(excludes_files: List[str]) -> List[str]:
    key = get_key()
    excludes = set()
    for exclude_file in excludes_files:
        with open(exclude_file, "r") as f:
            reader = csv.DictReader(f)
            for line in reader:
                excludes.add(line[key])
    return list(excludes)


def exclude(concepts: List[OrderedDict], excludes: List[str]) -> List[OrderedDict]:
    key = get_key()
    return [c for c in concepts if c[key] not in excludes]


def detect_cycles(concepts: List[OrderedDict]):
    """Throws an exception if concepts reference each other cyclically"""
    key = get_key()
    all_concepts_by_name = {c[key]: c for c in concepts}

    def get_cycle(concept: OrderedDict, visited=set(), this_branch=[]) -> Optional[set]:
        if concept[key] in this_branch:
            return this_branch
        if concept[key] in visited:
            return None
        visited.add(concept[key])
        this_branch.append(concept[key])

        members = concept["Members"].split(";")
        answers = concept["Answers"].split(";")
        for name in members + answers:
            if name != "":
                if get_cycle(all_concepts_by_name[name], visited, this_branch):
                    return this_branch + [name]

        this_branch.remove(concept[key])
        return None

    # Check all possible trees
    cycle_strings: List[str] = []
    for concept in concepts:
        cycle = get_cycle(concept)
        if cycle:
            cycle_string = " --> ".join(cycle)
            # Check that this isn't a substring of any existing string
            if all([cycle_string not in c for c in cycle_strings]):
                cycle_strings.append(cycle_string)

    if cycle_strings:
        raise Exception(
            "Some concepts in the specified set refer circularly to each other. "
            "The concepts therefore cannot be ordered in a CSV. Cylces of "
            "dependencies are printed below.\n\t"
            + "\n\t".join(c for c in cycle_strings)
        )


def move_referring_concepts_down(concepts: list, key: str) -> list:
    """Moves concepts below their answers or set members

    Precondition: concepts must be free of cycles
    """

    # We keep a dict for the order. The values in the order dict do not
    # have to be sequential.
    concept_order = {c[key]: float(i) for i, c in enumerate(concepts)}
    needs_more_ordering = True
    count = 0
    while needs_more_ordering:
        count += 1
        print("  Sorting: pass #{}".format(count))
        needs_more_ordering = False
        for concept in concepts:
            members = concept["Members"].split(";")
            answers = concept["Answers"].split(";")
            referants = members + answers
            referants = [r for r in referants if r != ""]
            if referants:
                ref_indices = [concept_order[r] for r in referants]
                if concept_order[concept[key]] <= max(ref_indices):
                    # We increment by 0.5 so as not to collide with what might
                    # be a a containing set
                    concept_order[concept[key]] = max(ref_indices) + 0.5
                    needs_more_ordering = True
    key_index_pairs = concept_order.items()
    sorted_key_index_pairs = sorted(key_index_pairs, key=lambda x: x[1])
    indexed_concepts = {c[key]: c for c in concepts}
    ordered_concepts = [indexed_concepts[pair[0]] for pair in sorted_key_index_pairs]
    return ordered_concepts


def run_sql(sql_code: str) -> str:
    """Connects to the database and runs the given SQL code.

    Globals:
        DB_NAME: str
            The name of the database.
        USER: str
            The username to use to log into the database.
        PASSWORD: str
            The password to use to log into the database.
        DOCKER: bool
            Whether or not the MySQL database is in a docker container.

    The SQL code composed must not contain double-quotes (")
    """

    mysql_args = '-e "{}"'.format(sql_code)

    command = "mysql -u {} --password='{}' {} {}".format(
        USER, PASSWORD, mysql_args, DB_NAME
    )

    if DOCKER:
        container_id = get_command_output(
            "docker ps | grep openmrs-sdk-mysql | cut -f1 -d' '"
        )
        command = "docker exec {} {}".format(container_id, command)

    return get_command_output(command)


def get_command_output(command):
    result = sp.run(command, capture_output=True, shell=True, encoding=ENCODING)
    if result.returncode != 0:
        raise Exception(
            "Command {}\nexited {}. Stderr:\n{}".format(
                command, result.returncode, result.stderr
            )
        )
    line = result.stdout.strip()
    return line


def sql_result_to_list_of_ordered_dicts(sql_result: str) -> list:
    # TODO: this replacement should be regex that looks for whitespace around NULL
    #   otherwise we might accidentally replace some part of a field that includes the
    #   string literal "NULL" for whatever reason
    sql_result = sql_result.replace("NULL", "")
    newline_text = "\n\\n"
    newline_replacement = "~~NEWLINE~~"
    sql_result = sql_result.replace(newline_text, newline_replacement)
    # Quote all fields
    sql_result = sql_result.replace('"', '""')
    sql_result = '"' + sql_result
    sql_result = sql_result.replace("\t", '"\t"')
    sql_result = sql_result.replace("\n", '"\n"')
    sql_result = sql_result[:-1]
    sql_lines = [
        l.replace(newline_replacement, newline_text) for l in sql_result.splitlines()
    ]
    return list(csv.DictReader(sql_lines, delimiter="\t"))


def squish_name(name: str):
    """Takes a string with spaces and makes it more appropriate for a filename"""
    return name.replace(" ", "-")


def get_key():
    return (
        "_mapping:" + CONCEPT_KEY_MAPPING
        if CONCEPT_KEY_MAPPING
        else "Fully specified name:" + DEFAULT_LOCALE
    )


def get_columns(name_types: List[str], concepts: List[OrderedDict]) -> List[str]:
    names = name_column_headers(name_types)
    initial_keys = (
        ["uuid", "Void/Retire"]
        + names
        + [
            "Description:en",
            "Data class",
            "Data type",
            "Answers",
            "Members",
        ]
    )
    other_keys = [
        k
        for k in concepts[0].keys()
        if k not in initial_keys and not k.startswith("_mapping")
    ]
    all_keys = initial_keys + other_keys
    filtered_keys = [
        k for k in all_keys if k == "Void/Retire" or any([c[k] for c in concepts])
    ]
    return filtered_keys


def name_column_headers(name_types: List[str]) -> List[str]:
    return [
        "{nt_long}:{l}".format(nt_long=NAME_TYPE_INIZ_NAMES[nt], l=l)
        for l in LOCALES
        for nt in name_types
    ]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        "database",
        help="The name of the OpenMRS MySQL database from which to pull concepts.",
    )
    parser.add_argument(
        "-o",
        "--outfile",
        help="The path of the CSV file to write. If -c (--concept-set) is provided, the set ID is appended to the default file name. Default: {}".format(
            OUTFILE_DEFAULT_BASENAME + ".csv"
        ),
    )
    parser.add_argument(
        "-c",
        "--set-name",
        nargs="+",
        help="The fully specified English name of a concept set for which to pull concepts. By default, all concepts are exported into one CSV file.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=VERBOSE,
        help="More verbose output.",
    )
    parser.add_argument(
        "-d",
        "--docker",
        action="store_true",
        default=DOCKER,
        help="Whether the OpenMRS MySQL database is dockerized. The container must be named 'openmrs-sdk-mysql'.",
    )
    parser.add_argument(
        "--version",
        default=VERSION,
        help="The OpenMRS database/platform version.",
    )
    parser.add_argument(
        "-l",
        "--locales",
        default=",".join(LOCALES),
        help="A comma-separated list of locales for which to extract concept names. The first must be the default locale.",
    )
    parser.add_argument(
        "--name-types",
        default=",".join(NAME_TYPES_DEFAULT),
        help="A comma-separated list of name types for which to extract concept names.",
    )
    parser.add_argument(
        "-r",
        "--props-path",
        help="The path to the openmrs-runtime.properties file. Used for extracting username and password. Defaults to ~/openmrs/<database>/openmrs-runtime.properties.",
    )
    parser.add_argument(
        "-u",
        "--user",
        help="The username for the database. Defaults to the one stored in openmrs-runtime.properties.",
    )
    parser.add_argument(
        "-p",
        "--password",
        help="The password for the database. Defaults to the one stored in openmrs-runtime.properties.",
    )
    parser.add_argument(
        "-e",
        "--exclude-files",
        help="CSV files of concepts to exclude from this export.",
        nargs="+",
    )
    parser.add_argument(
        "-E",
        "--encoding",
        default=ENCODING,
        help="The encoding to use for reading output from commands, including results from the MySQL database.",
    )
    parser.add_argument(
        "-k",
        "--concept-key-mapping",
        help="By default, concepts refer to each other by Fully Specified Name in the default locale. Using this argument, concepts will refer to each other with a concept mapping instead. For example `-k CIEL`.",
    )
    args = parser.parse_args()

    main(
        database=args.database,
        set_name=" ".join(args.set_name) if args.set_name else None,
        outfile=args.outfile,
        verbose=args.verbose,
        docker=args.docker,
        locales=args.locales.split(","),
        name_types=args.name_types.split(","),
        user=args.user,
        password=args.password,
        runtime_properties_path=args.props_path,
        version=args.version,
        exclude_files=args.exclude_files,
        encoding=args.encoding,
        concept_key_mapping=args.concept_key_mapping,
    )
