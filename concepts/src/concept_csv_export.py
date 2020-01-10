#!/usr/bin/env python3
#
# A program for exporting concepts from an OpenMRS MySQL database to
# CSVs that can be loaded by the OpenMRS Initializer module.
#
# SQL below must not contain double-quotes.
#

import argparse
import csv
import os
import subprocess as sp
from typing import Optional

DB_NAME = None
SERVER_NAME = None
VERBOSE = False
DOCKER = True
OUTFILE = os.path.expanduser("~/Downloads/concepts.csv")

LOCALES = ["en", "es", "fr", "ht"]
NAME_TYPES = ["full", "short"]


def set_globals(
    database: str,
    verbose: bool,
    docker: bool,
    server_name: Optional[str],
    locales: list,
    name_types: list,
):
    global DB_NAME, VERBOSE, DOCKER, SERVER_NAME, LOCALES, NAME_TYPES
    DB_NAME = database
    VERBOSE = verbose
    DOCKER = docker
    SERVER_NAME = server_name if server_name else database
    LOCALES = locales
    NAME_TYPES = name_types


def main(
    database: str,
    verbose: bool = VERBOSE,
    docker: bool = DOCKER,
    server_name: Optional[str] = SERVER_NAME,
    locales: list = LOCALES,
    name_types: list = NAME_TYPES,
):

    set_globals(database, verbose, docker, server_name, locales, name_types)

    check_data_for_stop_characters()

    limit = None
    sql_code = get_sql_code(limit)
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
    print("  We have {} concepts".format(len(all_concepts)))
    print("Reordering...")
    ordered_concepts = move_referring_concepts_down(
        all_concepts, "Fully specified name:en"
    )
    print("Writing output file " + OUTFILE)
    with open(OUTFILE, "w") as f:
        writer = csv.DictWriter(f, ordered_concepts[0].keys())
        writer.writeheader()
        writer.writerows(ordered_concepts)


def check_data_for_stop_characters():
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
            "the Initializer stop character ';' (semicolon). This will "
            "break things."  # TODO: replace them with periods
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
            "This will break things."  # TODO: replace them with periods
        )
        for item in result:
            print(item)


def get_sql_code(limit: Optional[int] = None, where="") -> str:
    select = (
        "SET SESSION group_concat_max_len = 1000000; "
        "SELECT c.uuid, cd_en.description 'Description:en', cl.name 'Data class', dt.name 'Data type', "
        "GROUP_CONCAT(DISTINCT source.name, ':', crt.code SEPARATOR ';') 'Same as concept mappings', "
        + ", ".join([locale_select_snippet(l) for l in LOCALES])
        + ", c_num.hi_absolute 'Absolute high'"
        ", c_num.hi_critical 'Critical high'"
        ", c_num.hi_normal 'Normal high'"
        ", c_num.low_absolute 'Absolue low'"
        ", c_num.low_critical 'Critical low'"
        ", c_num.low_normal 'Normal low'"
        ", c_num.units 'Units'"
        ", c_num.allow_decimal 'Allow decimals'"
        ", c_num.display_precision 'Display precision'"
        ", c_cx.handler 'Complex data handler'"
        ", GROUP_CONCAT(DISTINCT set_mem_name.name SEPARATOR ';') 'Members' "
        ", GROUP_CONCAT(DISTINCT ans_name.name SEPARATOR ';') 'Answers' "
    )

    tables = (
        "FROM concept c \n"
        "JOIN concept_class cl ON c.class_id = cl.concept_class_id \n"
        "JOIN concept_datatype dt ON c.datatype_id = dt.concept_datatype_id \n"
        "LEFT JOIN concept_description cd_en ON c.concept_id = cd_en.concept_id AND cd_en.locale = 'en' \n"
        "LEFT JOIN concept_reference_map crm ON c.concept_id = crm.concept_id \n"
        "  LEFT JOIN concept_reference_term crt ON crm.concept_reference_term_id = crt.concept_reference_term_id AND crt.retired = 0 \n"
        "  LEFT JOIN concept_map_type map_type ON crm.concept_map_type_id = map_type.concept_map_type_id AND map_type.name = 'SAME-AS' \n"
        "  LEFT JOIN concept_reference_source source ON crt.concept_source_id = source.concept_source_id \n"
        + "\n ".join([locale_join_snippet(l) for l in LOCALES])
        + "\nLEFT JOIN concept_numeric c_num ON c.concept_id = c_num.concept_id "
        "LEFT JOIN concept_complex c_cx ON c.concept_id = c_cx.concept_id \n"
        "LEFT JOIN concept_set c_set ON c.concept_id = c_set.concept_set \n"
        "  LEFT JOIN concept c_set_c ON c_set.concept_id = c_set_c.concept_id AND c_set_c.retired = 0 \n"  # we look up the concept to filter out the retired members
        "  LEFT JOIN concept_name set_mem_name ON c_set_c.concept_id = set_mem_name.concept_id \n"
        "    AND set_mem_name.locale = 'en' AND set_mem_name.concept_name_type = 'FULLY_SPECIFIED' AND set_mem_name.voided = 0 \n"
        "LEFT JOIN concept_answer c_ans ON c.concept_id = c_ans.concept_id \n"
        "  LEFT JOIN concept c_ans_c ON c_ans.answer_concept = c_ans_c.concept_id AND c_ans_c.retired = 0 \n"  # we look up the concept to filter out the retired answers
        "  LEFT JOIN concept_name ans_name ON c_ans_c.concept_id = ans_name.concept_id \n"
        "    AND ans_name.locale = 'en' AND ans_name.concept_name_type = 'FULLY_SPECIFIED' AND ans_name.voided = 0 \n"
    )

    ending = (
        "WHERE c.retired = 0  {where_part} "
        "GROUP BY c.concept_id "
        "ORDER BY c.is_set {limit_part} "
    ).format(
        limit_part="LIMIT {}".format(limit) if limit != None else "",
        where_part="AND {}".format(where) if where != "" else "",
    )

    sql_code = select + "\n" + tables + "\n" + ending + ";"
    return sql_code


def locale_select_snippet(locale):
    name_type_iniz_names = {"full": "Fully specified name", "short": "Short name"}

    snippets = []
    for name_type in NAME_TYPES:
        snippets.append(
            " cn_{l}_{t}.name '{iniz_name}:{l}' ".format(
                l=locale, t=name_type, iniz_name=name_type_iniz_names[name_type]
            )
        )
    return ", ".join(snippets)


def locale_join_snippet(locale):
    name_type_sql_names = {"full": "FULLY_SPECIFIED", "short": "SHORT"}

    snippets = []
    for name_type in NAME_TYPES:
        snippets.append(
            " {join_type} JOIN concept_name cn_{l}_{t} "
            "ON c.concept_id = cn_{l}_{t}.concept_id "
            "AND cn_{l}_{t}.locale = '{l}' "
            "AND cn_{l}_{t}.concept_name_type = '{sql_name}' "
            "AND cn_{l}_{t}.voided = 0".format(
                join_type=("" if name_type == "full" and locale == "en" else "LEFT"),
                l=locale,
                t=name_type,
                sql_name=name_type_sql_names[name_type],
            )
        )

    return "\n    ".join(snippets)


def run_sql(sql_code):
    """ The SQL code composed must not contain double-quotes (") """

    mysql_args = '-e "{}"'.format(sql_code)

    root_pass = get_command_output(
        "grep connection.password ~/openmrs/"
        + SERVER_NAME
        + '/openmrs-server.properties | cut -f2 -d"="'
    )

    command = "mysql -u root --password='{}' {} {}".format(
        root_pass, mysql_args, DB_NAME
    )

    if DOCKER:
        container_id = get_command_output(
            "docker ps | grep openmrs-sdk-mysql | cut -f1 -d' '"
        )
        command = "docker exec {} {}".format(container_id, command)

    result = sp.run(
        command,
        stdout=sp.PIPE,
        stderr=sp.PIPE,
        check=True,
        shell=True,
        encoding="latin-1",
    )
    if VERBOSE:
        print(result.stderr)
    return result.stdout


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


def move_referring_concepts_down(concepts: list, key: str) -> list:
    # We keep a dict for the order
    # the values in the order dict do not have to be sequential
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


def get_command_output(command):
    result = sp.run(
        command, capture_output=True, check=True, shell=True, encoding="utf8"
    )
    line = result.stdout.strip()
    return line


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "database",
        help="The name of the OpenMRS MySQL database from which to pull concepts.",
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
        "-s",
        "--server",
        help="The name of the server. Used to fetch credentials for MySQL from openmrs-server.properties. If not provided, defaults to the database name.",
    )
    parser.add_argument(
        "-l",
        "--locales",
        default=",".join(LOCALES),
        help="A comma-separated list of locales for which to extract concept names.",
    )
    parser.add_argument(
        "--name-types",
        default=",".join(NAME_TYPES),
        help="A comma-separated list of name types for which to extract concept names.",
    )
    args = parser.parse_args()

    main(
        database=args.database,
        verbose=args.verbose,
        docker=args.docker,
        server_name=args.server if args.server else args.database,
        locales=args.locales.split(","),
        name_types=args.name_types.split(","),
    )
