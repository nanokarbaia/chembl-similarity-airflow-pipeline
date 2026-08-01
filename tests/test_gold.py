"""Tests for gold data mart helper logic."""

from __future__ import annotations

import pandas as pd
import pytest

from lib.chembl.gold import (
    build_pivot_view_sql,
    dataframe_to_fact_records,
    validate_top10_dataframe,
)
from lib.utils.parsing import normalize_chembl_id


def make_top10_dataframe() -> pd.DataFrame:
    """Create valid top-N-like dataframe for tests."""
    return pd.DataFrame(
        {
            'source_chembl_id': [' chembl10 ', 'CHEMBL10'],
            'target_chembl_id': ['CHEMBL6252', 'CHEMBL269339'],
            'similarity_score': [0.612244898, 0.406779661],
            'similarity_rank': [1, 2],
            'has_duplicates_of_last_largest_score': [False, False],
        }
    )


def test_normalize_chembl_id() -> None:
    assert normalize_chembl_id(' chembl10 ') == 'CHEMBL10'
    assert normalize_chembl_id(None) is None
    assert normalize_chembl_id('') is None
    assert normalize_chembl_id('NULL') is None


def test_validate_top10_dataframe_accepts_valid_data() -> None:
    result = validate_top10_dataframe(make_top10_dataframe())

    assert result['source_chembl_id'].tolist() == ['CHEMBL10', 'CHEMBL10']
    assert result['target_chembl_id'].tolist() == ['CHEMBL6252', 'CHEMBL269339']
    assert result['similarity_rank'].tolist() == [1, 2]
    assert result['has_duplicates_of_last_largest_score'].dtype == bool


def test_validate_top10_dataframe_rejects_missing_columns() -> None:
    dataframe = make_top10_dataframe().drop(columns=['similarity_score'])

    with pytest.raises(ValueError, match='Missing required columns'):
        validate_top10_dataframe(dataframe)


def test_validate_top10_dataframe_rejects_null_values() -> None:
    dataframe = make_top10_dataframe()
    dataframe.loc[0, 'source_chembl_id'] = None

    with pytest.raises(ValueError, match='contains null values'):
        validate_top10_dataframe(dataframe)


def test_validate_top10_dataframe_rejects_invalid_chembl_ids() -> None:
    dataframe = make_top10_dataframe()
    dataframe.loc[0, 'target_chembl_id'] = 'INVALID_ID'

    with pytest.raises(ValueError, match='Invalid ChEMBL IDs'):
        validate_top10_dataframe(dataframe)


def test_validate_top10_dataframe_rejects_duplicate_pairs() -> None:
    dataframe = pd.DataFrame(
        {
            'source_chembl_id': ['CHEMBL10', 'CHEMBL10'],
            'target_chembl_id': ['CHEMBL6252', 'CHEMBL6252'],
            'similarity_score': [0.61, 0.61],
            'similarity_rank': [1, 2],
            'has_duplicates_of_last_largest_score': [False, False],
        }
    )

    with pytest.raises(ValueError, match='Duplicate source-target'):
        validate_top10_dataframe(dataframe)


def test_validate_top10_dataframe_rejects_invalid_similarity_score() -> None:
    dataframe = make_top10_dataframe()
    dataframe.loc[0, 'similarity_score'] = 1.5

    with pytest.raises(ValueError, match='between 0 and 1'):
        validate_top10_dataframe(dataframe)


def test_validate_top10_dataframe_rejects_duplicate_ranks() -> None:
    dataframe = make_top10_dataframe()
    dataframe.loc[1, 'similarity_rank'] = 1

    with pytest.raises(ValueError, match='Duplicate similarity ranks'):
        validate_top10_dataframe(dataframe)


def test_validate_top10_dataframe_rejects_non_sequential_ranks() -> None:
    dataframe = make_top10_dataframe()
    dataframe.loc[1, 'similarity_rank'] = 3

    with pytest.raises(ValueError, match='invalid rank sequence'):
        validate_top10_dataframe(dataframe)


def test_dataframe_to_fact_records() -> None:
    dataframe = validate_top10_dataframe(make_top10_dataframe())

    records = dataframe_to_fact_records(dataframe)

    assert records == [
        ('CHEMBL10', 'CHEMBL6252', 0.612244898, 1, False),
        ('CHEMBL10', 'CHEMBL269339', 0.406779661, 2, False),
    ]


def test_build_pivot_view_sql_contains_expected_columns() -> None:
    pivot_sql = build_pivot_view_sql(['CHEMBL10', 'CHEMBL11'])

    assert 'CREATE OR REPLACE VIEW gold.vw_similarity_pivot_10_sources' in pivot_sql
    assert 'target_chembl_id' in pivot_sql
    assert 'AS "CHEMBL10"' in pivot_sql
    assert 'AS "CHEMBL11"' in pivot_sql
    assert "'CHEMBL10'" in pivot_sql
    assert "'CHEMBL11'" in pivot_sql


def test_build_pivot_view_sql_rejects_invalid_source_id() -> None:
    with pytest.raises(ValueError, match='Invalid source molecule IDs'):
        build_pivot_view_sql(['CHEMBL10', 'BAD_ID'])
