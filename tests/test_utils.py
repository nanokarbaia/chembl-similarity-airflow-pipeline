"""Tests for shared utility helpers."""

from __future__ import annotations

import pytest

from lib.utils.parsing import (
    normalize_chembl_id,
    normalize_text,
    parse_bool_value,
    parse_positive_int,
)
from lib.utils.s3 import (
    build_s3_folder_prefix,
    normalize_s3_list_prefix,
    normalize_s3_prefix,
)


def test_normalize_text() -> None:
    assert normalize_text(' test ') == 'test'
    assert normalize_text('') is None
    assert normalize_text(None) is None
    assert normalize_text('NULL') is None
    assert normalize_text('nan') is None
    assert normalize_text('NA') is None
    assert normalize_text('N/A') is None
    assert normalize_text('<NA>') is None


def test_normalize_chembl_id() -> None:
    assert normalize_chembl_id(' chembl10 ') == 'CHEMBL10'
    assert normalize_chembl_id(None) is None
    assert normalize_chembl_id('') is None
    assert normalize_chembl_id('NULL') is None


def test_parse_bool_value() -> None:
    assert parse_bool_value(True) is True
    assert parse_bool_value(False) is False
    assert parse_bool_value('true') is True
    assert parse_bool_value('0') is False

    with pytest.raises(ValueError):
        parse_bool_value('maybe')


def test_parse_positive_int() -> None:
    assert parse_positive_int('10', 'test_param') == 10

    with pytest.raises(ValueError):
        parse_positive_int(0, 'test_param')

    with pytest.raises(ValueError):
        parse_positive_int('abc', 'test_param')

    with pytest.raises(ValueError):
        parse_positive_int(True, 'test_param')


def test_normalize_s3_prefix() -> None:
    assert normalize_s3_prefix('/final_task/karbaia_nano/') == (
        'final_task/karbaia_nano'
    )
    assert normalize_s3_prefix('') == ''


def test_normalize_s3_list_prefix() -> None:
    assert normalize_s3_list_prefix('/final_task/karbaia_nano/') == (
        'final_task/karbaia_nano/'
    )
    assert normalize_s3_list_prefix('/final_task/karbaia_nano') == (
        'final_task/karbaia_nano'
    )
    assert normalize_s3_list_prefix('') == ''


def test_build_s3_folder_prefix() -> None:
    assert build_s3_folder_prefix('/final_task/karbaia_nano') == (
        'final_task/karbaia_nano/'
    )

    with pytest.raises(ValueError):
        build_s3_folder_prefix('')
