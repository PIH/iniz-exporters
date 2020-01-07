#!/usr/bin/env python3
#
#
# SQL below must not contain double-quotes.
#

import csv
from collections import OrderedDict
import os
import subprocess as sp
import time
from typing import Optional

DEBUG = False


DOCKER = True
SERVER_NAME = "ces"
DB_NAME = "ces"
TMP_FILE = "/var/lib/mysql-files/concept-export-" + str(int(time.time())) + ".csv"

LOCALES = ["en", "es", "fr", "ht"]
NAME_TYPES = ["full", "short"]


def main():
    check_data_for_stop_characters()

    limit = None
    sql_code = get_sql_code(limit)
    if DEBUG:
        print(sql_code)

    print("Querying concepts...")
    os.remove(TMP_FILE)
    run_sql(sql_code)
    print("Parsing results...")
    with open(TMP_FILE, "r") as f:
        all_concepts = list(csv.DictReader(f))
    os.remove(TMP_FILE)
    print("  We have {} concepts".format(len(all_concepts)))
    print("Reordering...")
    ordered_concepts = move_referring_concepts_down(
        all_concepts, "Fully specified name:en"
    )
    print(ordered_concepts)


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


def get_sql_code(limit: Optional[int] = None, where="", outfile=TMP_FILE) -> str:
    select = (
        "SELECT c.concept_id, c.uuid, cd_en.description 'Description:en', cl.name 'Data class', dt.name 'Data type', "
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
        "JOIN concept_reference_map crm ON c.concept_id = crm.concept_id "
        "JOIN concept_reference_term crt ON crm.concept_reference_term_id = crt.concept_reference_term_id AND crt.retired = 0 "
        "JOIN concept_map_type map_type ON crm.concept_map_type_id = map_type.concept_map_type_id AND map_type.name = 'SAME-AS' "
        "JOIN concept_reference_source source ON crt.concept_source_id = source.concept_source_id \n"
        + "\n ".join([locale_join_snippet(l) for l in LOCALES])
        + " LEFT JOIN concept_numeric c_num ON c.concept_id = c_num.concept_id "
        "LEFT JOIN concept_complex c_cx ON c.concept_id = c_cx.concept_id "
        "LEFT JOIN concept_set c_set ON c.concept_id = c_set.concept_set "
        "  LEFT JOIN concept c_set_c ON c_set.concept_id = c_set_c.concept_id AND c_set_c.retired = 0 "  # we look up the concept to filter out the retired members
        "  LEFT JOIN concept_name set_mem_name ON c_set_c.concept_id = set_mem_name.concept_id "
        "    AND set_mem_name.locale = 'en' AND set_mem_name.concept_name_type = 'FULLY_SPECIFIED' AND set_mem_name.voided = 0 "
        "LEFT JOIN concept_answer c_ans ON c.concept_id = c_ans.concept_id "
        "  LEFT JOIN concept c_ans_c ON c_ans.answer_concept = c_ans_c.concept_id AND c_ans_c.retired = 0 "  # we look up the concept to filter out the retired answers
        "  LEFT JOIN concept_name ans_name ON c_ans_c.concept_id = ans_name.concept_id "
        "    AND ans_name.locale = 'en' AND ans_name.concept_name_type = 'FULLY_SPECIFIED' AND ans_name.voided = 0 "
    )

    ending = (
        "WHERE c.retired = 0 {where_part} "
        "GROUP BY c.concept_id "
        "ORDER BY c.is_set {limit_part} "
        "INTO OUTFILE '" + outfile + "' "
        "FIELDS TERMINATED BY ',' "
        "ENCLOSED BY '\\\"' "
        "LINES TERMINATED BY '\\n' "
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
    if DEBUG:
        print(result.stderr)
        print(result.stdout)
    return result.stdout


def sql_result_to_list_of_ordered_dicts(sql_result: str) -> list:
    # TODO: this replacement should be regex that looks for whitespace around NULL
    #   otherwise we might accidentally replace some part of a field that includes the
    #   string literal "NULL" for whatever reason
    sql_result = sql_result.replace("NULL", "")
    newline_text = "\n\\n"
    newline_replacement = "~~NEWLINE~~"
    sql_result = sql_result.replace(newline_text, newline_replacement)
    sql_result = sql_result.replace("\t", newline_replacement)
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
    main()
