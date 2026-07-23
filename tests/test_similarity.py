"""Tests for molecule similarity logic."""

from __future__ import annotations

from collections import Counter

import pandas as pd
import pytest
from rdkit import Chem, DataStructs
from rdkit.Chem import rdFingerprintGenerator

from lib.chembl.similarity import (
    FINGERPRINT_COLUMNS,
    add_top10_metadata,
    calculate_similarity_chunk,
    fingerprint_from_binary,
    get_source_fingerprints_from_files,
    normalize_chembl_id,
    update_top_candidates,
)


def make_fingerprint_binary(smiles: str) -> bytes:
    """Create Morgan fingerprint binary for a test molecule."""
    molecule = Chem.MolFromSmiles(smiles)
    generator = rdFingerprintGenerator.GetMorganGenerator(
        radius=2,
        fpSize=2048,
    )
    fingerprint = generator.GetFingerprint(molecule)
    return DataStructs.BitVectToBinaryText(fingerprint)


def test_normalize_chembl_id() -> None:
    assert normalize_chembl_id(' chembl10 ') == 'CHEMBL10'
    assert normalize_chembl_id('') is None
    assert normalize_chembl_id(None) is None
    assert normalize_chembl_id(float('nan')) is None


def test_update_top_candidates_keeps_best_rows_with_deterministic_tie_order() -> None:
    current_top = pd.DataFrame(
        {
            'source_chembl_id': ['CHEMBL1', 'CHEMBL1'],
            'target_chembl_id': ['CHEMBL3', 'CHEMBL1'],
            'similarity_score': [0.30, 0.50],
        }
    )

    new_rows = pd.DataFrame(
        {
            'source_chembl_id': ['CHEMBL1', 'CHEMBL1'],
            'target_chembl_id': ['CHEMBL2', 'CHEMBL4'],
            'similarity_score': [0.50, 0.70],
        }
    )

    result = update_top_candidates(
        current_top=current_top,
        similarity_dataframe=new_rows,
        top_n=3,
    )

    assert result['target_chembl_id'].tolist() == [
        'CHEMBL4',
        'CHEMBL1',
        'CHEMBL2',
    ]
    assert result['similarity_score'].tolist() == [0.70, 0.50, 0.50]


def test_add_top10_metadata_marks_only_cutoff_ties() -> None:
    top_candidates = pd.DataFrame(
        {
            'source_chembl_id': ['CHEMBL1'] * 10,
            'target_chembl_id': [f'CHEMBL{index}' for index in range(2, 12)],
            'similarity_score': [
                0.90,
                0.80,
                0.70,
                0.60,
                0.55,
                0.54,
                0.53,
                0.52,
                0.50,
                0.50,
            ],
        }
    )

    score_counts = Counter(top_candidates['similarity_score'].tolist())
    score_counts.update([0.50])

    result = add_top10_metadata(
        top_candidates=top_candidates,
        score_counts=score_counts,
    )

    assert result['similarity_rank'].tolist() == list(range(1, 11))
    assert result['has_duplicates_of_last_largest_score'].tolist() == [
        False,
        False,
        False,
        False,
        False,
        False,
        False,
        False,
        True,
        True,
    ]


def test_add_top10_metadata_does_not_mark_when_no_cutoff_duplicates() -> None:
    top_candidates = pd.DataFrame(
        {
            'source_chembl_id': ['CHEMBL1'] * 10,
            'target_chembl_id': [f'CHEMBL{index}' for index in range(2, 12)],
            'similarity_score': [
                0.90,
                0.80,
                0.70,
                0.60,
                0.55,
                0.54,
                0.53,
                0.52,
                0.51,
                0.50,
            ],
        }
    )

    score_counts = Counter(top_candidates['similarity_score'].tolist())

    result = add_top10_metadata(
        top_candidates=top_candidates,
        score_counts=score_counts,
    )

    assert not result['has_duplicates_of_last_largest_score'].any()


def test_calculate_similarity_chunk_excludes_source_and_calculates_score() -> None:
    source_binary = make_fingerprint_binary('CCO')
    source_fingerprint = fingerprint_from_binary(source_binary)

    target_dataframe = pd.DataFrame(
        {
            'chembl_id': ['CHEMBL1', 'CHEMBL2', 'CHEMBL3'],
            'canonical_smiles': ['CCO', 'CCO', 'c1ccccc1'],
            'fingerprint_binary': [
                source_binary,
                make_fingerprint_binary('CCO'),
                make_fingerprint_binary('c1ccccc1'),
            ],
        }
    )

    result = calculate_similarity_chunk(
        source_chembl_id='CHEMBL1',
        source_fingerprint=source_fingerprint,
        target_dataframe=target_dataframe,
    )

    assert result['target_chembl_id'].tolist() == ['CHEMBL2', 'CHEMBL3']

    identical_score = result.loc[
        result['target_chembl_id'] == 'CHEMBL2',
        'similarity_score',
    ].iloc[0]

    assert identical_score == pytest.approx(1.0)


def test_get_source_fingerprints_from_files_uses_input_order(tmp_path) -> None:
    fingerprint_data = pd.DataFrame(
        {
            'chembl_id': ['CHEMBL1', 'CHEMBL2', 'CHEMBL3'],
            'canonical_smiles': ['CCO', 'CCC', 'CCN'],
            'fingerprint_binary': [b'1', b'2', b'3'],
        }
    )

    parquet_path = tmp_path / 'fingerprints.parquet'
    fingerprint_data[FINGERPRINT_COLUMNS].to_parquet(parquet_path, index=False)

    result = get_source_fingerprints_from_files(
        fingerprint_file_paths=[parquet_path],
        source_chembl_ids=['CHEMBL3', 'CHEMBL1'],
        source_molecule_limit=10,
    )

    assert result['chembl_id'].tolist() == ['CHEMBL3', 'CHEMBL1']


def test_get_source_fingerprints_from_files_fallback_uses_limit(tmp_path) -> None:
    fingerprint_data = pd.DataFrame(
        {
            'chembl_id': ['CHEMBL1', 'CHEMBL2', 'CHEMBL3'],
            'canonical_smiles': ['CCO', 'CCC', 'CCN'],
            'fingerprint_binary': [b'1', b'2', b'3'],
        }
    )

    parquet_path = tmp_path / 'fingerprints.parquet'
    fingerprint_data[FINGERPRINT_COLUMNS].to_parquet(parquet_path, index=False)

    result = get_source_fingerprints_from_files(
        fingerprint_file_paths=[parquet_path],
        source_chembl_ids=[],
        source_molecule_limit=2,
    )

    assert result['chembl_id'].tolist() == ['CHEMBL1', 'CHEMBL2']
    