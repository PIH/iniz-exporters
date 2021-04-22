#!/usr/bin/env python3
#
# Pulls all of the location data from the database and works on it
# in-memory.
#

import argparse
import csv
import os
import queue
from typing import List, Set, Optional
from collections import OrderedDict
import subprocess as sp

DESCRIPTION = """
A program for exporting locations from an OpenMRS MySQL database to
a CSV that can be loaded by the OpenMRS Initializer module.
"""


# Globals -- modified only during initialization
VERBOSE = False
DOCKER = False
VERSION = 2.3
# These must be set before running run_sql
DB_NAME = ""
USER = ""
PASSWORD = ""

# Defaults
OUTFILE_DEFAULT = os.path.expanduser("~/Downloads/locations.csv")


def set_globals(
    database: str,
    verbose: bool = VERBOSE,
    docker: bool = DOCKER,
    runtime_properties_path: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    version: float = VERSION,
):
    """
    Initializes the global variables used in this script.
    Defaults are as described in `concept_csv_export.py --help`.
    """
    global VERBOSE, DB_NAME, DOCKER, USER, PASSWORD, VERSION
    VERBOSE = verbose
    DB_NAME = database
    DOCKER = docker
    VERSION = version

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
    docker: bool = DOCKER,
    outfile: str = "",  # default is set in the function
    verbose: bool = VERBOSE,
    runtime_properties_path: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    version: float = VERSION,
):
    set_globals(
        database=database,
        verbose=verbose,
        docker=docker,
        runtime_properties_path=runtime_properties_path,
        user=user,
        password=password,
        version=version,
    )

    locations = get_locations()
    locations = spread_tags(locations)
    locations = spread_attributes(locations)
    print("Writing {} locations to output file {}".format(len(locations), outfile))
    with open(outfile, "w") as f:
        keys = get_columns(locations)
        writer = csv.DictWriter(f, keys)
        writer.writeheader()
        writer.writerows(locations)


def get_locations() -> list:
    """Gets all the location data, including tags and attributes"""
    sql_code = """
        select      l.uuid as 'UUID',
                    l.retired as 'Void/Retire',
                    l.name as 'Name',
                    l.description as 'Description',
                    p.name as 'Parent',
                    group_concat(distinct lt.name) as 'Tags',
                    group_concat(distinct concat(lat.name, ':', la.value_reference)) as 'Attributes'
        from        location l
        left join   location p on l.parent_location = p.location_id
        left join   location_tag_map ltm on l.location_id = ltm.location_id
        left join   location_tag lt on ltm.location_tag_id = lt.location_tag_id
        left join   location_attribute la on l.location_id = la.location_id
        left join   location_attribute_type lat on la.attribute_type_id = lat.location_attribute_type_id
        group by    l.location_id
        order by    l.location_id asc;
    """
    if VERBOSE:
        print(sql_code)
        input("Press any key to continue...")

    sql_result = run_sql(sql_code)
    if VERBOSE:
        print(sql_result)
        input("Press any key to continue...")
    print("Parsing results...")
    result = sql_result_to_list_of_ordered_dicts(sql_result)
    return result


def spread_tags(locations: list) -> list:
    for location in locations:
        for tag in location['Tags'].split(','):
            if tag:
                location['Tag|' + tag] = "TRUE"
        del location['Tags']
    return locations


def spread_attributes(locations: list) -> list:
    for location in locations:
        for attribute in location['Attributes'].split(','):
            if attribute:
                attrName = attribute.split(':')[0]
                attrValue = attribute.split(':')[1]
                location['Attribute|' + attrName] = attrValue
        del location['Attributes']
    return locations

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
    result = sp.run(command, capture_output=True, shell=True, encoding="latin-1")
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


def get_columns(locations: list) -> List[str]:
    tags = set()
    attributes = set()
    for loc in locations:
        tags.update([k for k in loc.keys() if k.startswith("Tag|")])
        attributes.update([k for k in loc.keys() if k.startswith("Attribute|")])
    keys = (
        ["UUID", "Void/Retire", "Name", "Description", "Parent"]
        + sorted(attributes)
        + sorted(tags)
    )
    return keys


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        "database",
        help="The name of the OpenMRS MySQL database from which to pull concepts.",
    )
    parser.add_argument(
        "-o",
        "--outfile",
        help="The path of the CSV file to write. If -c (--concept-set) is provided, the set ID is appended to the default file name.",
        default=OUTFILE_DEFAULT
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
        type=float,
        default=VERSION,
        help="The OpenMRS database/platform version.",
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
    args = parser.parse_args()

    main(
        database=args.database,
        outfile=args.outfile,
        verbose=args.verbose,
        docker=args.docker,
        user=args.user,
        password=args.password,
        runtime_properties_path=args.props_path,
        version=args.version,
    )
