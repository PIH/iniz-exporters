#!/usr/bin/env pytest

from pprint import pprint
from concept_csv_export import (
    set_globals,
    get_sql_code,
    move_referring_concepts_down,
    run_sql,
    sql_result_to_list_of_ordered_dicts,
    get_all_concepts_in_tree,
    detect_cycles,
)
from collections import OrderedDict

NAME_TYPES = ["full", "short"]
LOCALES = ["en", "es", "fr", "ht"]

set_globals(database="ces", docker=True)


def test_get_concepts_results_have_uuid_and_match_limit():
    limit = 10
    sql_code = get_sql_code(name_types=NAME_TYPES, locales=LOCALES, limit=limit)
    sql_result = run_sql(sql_code)
    all_concepts = sql_result_to_list_of_ordered_dicts(sql_result)
    pprint(all_concepts)
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

    sorted_concepts = move_referring_concepts_down(concepts, key)
    res = [c[key] for c in sorted_concepts]
    assert res.index("a") > res.index("b")
    assert res.index("a") > res.index("c")
    assert res.index("b") > res.index("d")
    assert res.index("b") > res.index("e")
    assert res.index("c") > res.index("d")
    assert res.index("c") > res.index("e")


def test_get_all_concepts_in_tree():
    key = "Fully specified name:en"
    concepts = [
        OrderedDict([(key, "a"), ("Answers", ""), ("Members", "b;c")]),
        OrderedDict([(key, "b"), ("Answers", ""), ("Members", "d;e")]),
        OrderedDict([(key, "c"), ("Answers", "d;e"), ("Members", "")]),
        OrderedDict([(key, "d"), ("Answers", ""), ("Members", "")]),
        OrderedDict([(key, "e"), ("Answers", ""), ("Members", "")]),
    ]
    a_tree_concepts = set([c[key] for c in get_all_concepts_in_tree(concepts, "a")])
    assert a_tree_concepts == set(["a", "b", "c", "d", "e"])

    b_tree_concepts = set([c[key] for c in get_all_concepts_in_tree(concepts, "b")])
    assert b_tree_concepts == set(["b", "d", "e"])

    d_tree_concepts = set([c[key] for c in get_all_concepts_in_tree(concepts, "d")])
    assert d_tree_concepts == set(["d"])


def test_detect_cycles():
    key = "Fully specified name:en"
    concepts = [
        OrderedDict([(key, "a"), ("Answers", ""), ("Members", "b;c")]),
        OrderedDict([(key, "b"), ("Answers", ""), ("Members", "")]),
        OrderedDict([(key, "c"), ("Answers", "d;e"), ("Members", "")]),
        OrderedDict([(key, "d"), ("Answers", "e;f"), ("Members", "")]),
        OrderedDict([(key, "e"), ("Answers", ""), ("Members", "")]),
        OrderedDict([(key, "f"), ("Answers", "c;e"), ("Members", "")]),
    ]
    try:
        detect_cycles(concepts)
        assert False, "detect_cycles should have detected the cycle c-d-f-c"
    except Exception as e:
        assert "c --> d --> f --> c" in str(e)
        assert str(e).count("\n\t") == 1  # only should print one cycle
