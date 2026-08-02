"""Reusable parsing and normalization helpers."""

from __future__ import annotations

from typing import Any


TRUE_VALUES = {'1', 'TRUE', 'T', 'YES', 'Y'}
FALSE_VALUES = {'0', 'FALSE', 'F', 'NO', 'N'}
EMPTY_TEXT_VALUES = {
    '',
    'NAN',
    'NA',
    'N/A',
    'NONE',
    'NULL',
    '<NA>',
}


def normalize_text(value: Any) -> str | None:
    """Normalize text value and convert empty-like values to None."""
    if value is None:
        return None

    normalized_value = str(value).strip()

    if normalized_value.upper() in EMPTY_TEXT_VALUES:
        return None

    return normalized_value


def normalize_chembl_id(value: Any) -> str | None:
    """Normalize ChEMBL ID value."""
    normalized_value = normalize_text(value)

    if normalized_value is None:
        return None

    return normalized_value.upper()


def parse_bool_value(value: Any) -> bool:
    """Parse bool values safely from bool, integer, or string input."""
    if isinstance(value, bool):
        return value

    if value is None:
        raise ValueError('Boolean value cannot be null.')

    normalized_value = str(value).strip().upper()

    if normalized_value in TRUE_VALUES:
        return True

    if normalized_value in FALSE_VALUES:
        return False

    raise ValueError(f'Cannot parse boolean value: {value}')


def parse_positive_int(value: Any, parameter_name: str) -> int:
    """Parse and validate a positive integer."""
    if isinstance(value, bool):
        raise ValueError(
            f'{parameter_name} must be a positive integer, not a boolean.'
        )

    try:
        parsed_value = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f'{parameter_name} must be a positive integer. '
            f'Actual value: {value}'
        ) from exc

    if parsed_value < 1:
        raise ValueError(f'{parameter_name} must be at least 1.')

    return parsed_value
