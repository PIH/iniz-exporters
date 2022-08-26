#!/usr/bin/env python3
#
# Creates a CSV file for the concept_sets Iniz given an input CSV file for the concepts domain
#
# Assumptions: the input file is a CSV file for the concepts domain, where
#  1) the first row in the file is the concept set
#  2) all other rows with the Void/Retire set to false (or empty) should added the new output file as members of the top-level set
#  3) all rows with the Void/Retire set to true, should be added to the output file but with Void/Retired set to true (so they are removed from the set)
#
#
# Output:
#  * A standard concept_sets.csv with the "Concept", "Member", "Member Type", "Sort Weight" and "Void/Retired" column,
#       and with an additional "comment" column with the first fully-specified name comment found in the input file
#       Note that, currently, the referneces in "Concept" and "Member" are by uuid
#

import argparse
import csv

DESCRIPTION = """
A program creating an Iniz concept set csv based on a concept csv
"""

def set_globals(
    infile: str,
    outfile: str
):
    """
    Initializes the global variables used in this script.
    Defaults are as described in `concept_set_csv_creator.py --help`.
    """
    global INFILE, OUTFILE
    INFILE = infile

    if outfile:
        OUTFILE = outfile
    else:
        OUTFILE = "output.csv"

def main(
    infile: str,
    outfile: str
):
    set_globals(
        infile=infile,
        outfile=outfile
    )

    # load in file
    concepts = []
    with open(INFILE) as concept_file:
        concepts = list(csv.DictReader(concept_file, dialect='excel'))

    # grab the set concept uuid
    concept_set_uuid = concepts[0]['uuid']

    # set get the names to publish
    names = []
    for key in concepts[0].keys():
        if "Fully specified name:" in key:
            names.append(key)

    # chop off the first row (which defines the set)
    concepts.pop(0)

    # create the output file
    with open(OUTFILE, 'w') as concept_set_file:
        fieldnames = ['Concept', 'Member'] + list(map(lambda name: '#' + name, names)) \
                     + ['Member Type','Sort Weight','Void/Retire']
        writer = csv.DictWriter(concept_set_file, fieldnames=fieldnames)
        writer.writeheader()
        for idx, concept in enumerate(concepts):
            set_member = dict({'Concept': concept_set_uuid, 'Member': concept['uuid'], 'Member Type': 'CONCEPT-SET',
                             'Sort Weight': idx+1, 'Void/Retire': concept['Void/Retire']})
            for name in names:
                set_member['#' + name] = concept[name]
            writer.writerow(set_member)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        "infile",
        help="The psth of input concepts CSV file",
    )
    parser.add_argument(
        "-o",
        "--outfile",
        help="The path of the CSV file to write."
    )

    args = parser.parse_args()

    main(
        infile=args.infile,
        outfile=args.outfile
    )
