#!/usr/bin/env python3
#
#
# SQL below must not contain double-quotes.
#

import subprocess as sp, shlex


DOCKER = True
SERVER_NAME = "ces"
DB_NAME = "ces"

LOCALES = ["en", "es", "fr", "ht"]
NAME_TYPES = ["full", "short"]


def main():

    select = (
        "SELECT c.uuid, cd_en.description 'Description:en', cl.name 'Dataclass', dt.name 'Datatype', "
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
        ", GROUP_CONCAT(DISTINCT set_mem_name.name SEPARATOR ',') 'Set members' "
        ", GROUP_CONCAT(DISTINCT ans_name.name SEPARATOR ',') 'Answers' "
    )

    tables = (
        "FROM concept c \n"
        "JOIN concept_class cl ON c.class_id = cl.concept_class_id \n"
        "JOIN concept_datatype dt ON c.datatype_id = dt.concept_datatype_id \n"
        "LEFT JOIN concept_description cd_en ON c.concept_id = cd_en.concept_id AND cd_en.locale = 'en' \n"
        "JOIN concept_reference_map crm ON c.concept_id = crm.concept_id "
        "JOIN concept_reference_term crt ON crm.concept_reference_term_id = crt.concept_reference_term_id "
        "JOIN concept_map_type map_type ON crm.concept_map_type_id = map_type.concept_map_type_id AND map_type.name = 'SAME-AS' "
        "JOIN concept_reference_source source ON crt.concept_source_id = source.concept_source_id "
        + "\n ".join([locale_join_snippet(l) for l in LOCALES])
        + " LEFT JOIN concept_numeric c_num ON c.concept_id = c_num.concept_id "
        "LEFT JOIN concept_complex c_cx ON c.concept_id = c_cx.concept_id "
        "LEFT JOIN concept_set c_set ON c.concept_id = c_set.concept_set "
        "LEFT JOIN concept_name set_mem_name ON c_set.concept_id = set_mem_name.concept_id "
        "   AND set_mem_name.locale = 'en' AND set_mem_name.concept_name_type = 'FULLY_SPECIFIED' "
        "LEFT JOIN concept_answer c_ans ON c.concept_id = c_ans.concept_id "
        "LEFT JOIN concept_name ans_name ON c_ans.answer_concept = ans_name.concept_id "
        "   AND ans_name.locale = 'en' AND ans_name.concept_name_type = 'FULLY_SPECIFIED' "
    )

    ending = "GROUP BY c.concept_id LIMIT 10;"

    sql_code = select + "\n" + tables + "\n" + ending

    # print(sql_code)

    run_sql(sql_code)


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
            " LEFT JOIN concept_name cn_{l}_{t} "
            "ON c.concept_id = cn_{l}_{t}.concept_id "
            "AND cn_{l}_{t}.locale = '{l}' "
            "AND cn_{l}_{t}.concept_name_type = '{sql_name}'".format(
                l=locale, t=name_type, sql_name=name_type_sql_names[name_type]
            )
        )

    return "\n    ".join(snippets)


def run_sql(sql_code):
    """ The SQL code composed must not contain double-quotes (") """

    mysql_args = '-e "{}" 2>&1'.format(sql_code)

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

    process = sp.Popen(command, shell=True)
    process.communicate()


def get_command_output(command):
    result = sp.run(
        command, capture_output=True, check=True, shell=True, encoding="utf8"
    )
    line = result.stdout.strip()
    return line


if __name__ == "__main__":
    main()
