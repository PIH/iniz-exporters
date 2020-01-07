#!/usr/bin/env pytest

import csv
import concept_csv_export as cce
from collections import OrderedDict
import os

cce.DEBUG = False


def test_get_concepts_results_have_uuid_and_match_limit():
    limit = 10
    os.remove(cce.TMP_FILE)
    sql_code = cce.get_sql_code(limit, outfile=cce.TMP_FILE)
    sql_result = cce.run_sql(sql_code)
    all_concepts = cce.sql_result_to_list_of_ordered_dicts(sql_result)
    with open(cce.TMP_FILE, "r") as f:
        all_concepts = list(csv.DictReader(f))
    os.remove(cce.TMP_FILE)
    assert all_concepts[0]["uuid"] != ""
    assert len(all_concepts) == limit


def test_move_referring_concepts_down():
    key = "Fully specified name:en"
    concepts = [
        OrderedDict([(key, "a"), ("Answers", ""), ("Members", "b;c")]),
        OrderedDict([(key, "b"), ("Answers", ""), ("Members", "d;e")]),
        OrderedDict([(key, "c"), ("Answers", "d;e"), ("Members", "")]),
        OrderedDict([(key, "d"), ("Answers", ""), ("Members", "")]),
        OrderedDict([(key, "e"), ("Answers", ""), ("Members", "")]),
    ]

    sorted_concepts = cce.move_referring_concepts_down(concepts, key)
    res = [c[key] for c in sorted_concepts]
    assert res.index("a") > res.index("b")
    assert res.index("a") > res.index("c")
    assert res.index("b") > res.index("d")
    assert res.index("b") > res.index("e")
    assert res.index("c") > res.index("d")
    assert res.index("c") > res.index("e")
