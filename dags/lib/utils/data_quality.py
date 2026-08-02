"""Reusable data quality helper functions."""

from __future__ import annotations

from typing import Any

from psycopg2 import sql


def fetch_single_value(
    cursor,
    query: Any,
    parameters: tuple[Any, ...] = (),
) -> Any:
    """Execute SQL and return the first value."""
    cursor.execute(query, parameters)
    row = cursor.fetchone()

    if row is None:
        raise ValueError('Quality query returned no rows.')

    return row[0]


def assert_zero(value: int, check_name: str) -> None:
    """Assert that invalid row count is zero."""
    if value != 0:
        raise ValueError(f'{check_name} failed. Invalid rows: {value}')


def assert_positive(value: int, check_name: str) -> None:
    """Assert that row count is positive."""
    if value <= 0:
        raise ValueError(f'{check_name} failed. Value: {value}')


def assert_equal(
    actual_value: int,
    expected_value: int,
    check_name: str,
) -> None:
    """Assert that actual value equals expected value."""
    if actual_value != expected_value:
        raise ValueError(
            f'{check_name} failed. '
            f'Actual={actual_value}, expected={expected_value}'
        )


def get_relation_row_count(
    cursor,
    schema_name: str,
    relation_name: str,
) -> int:
    """Return row count for a table or view."""
    row_count = fetch_single_value(
        cursor=cursor,
        query=sql.SQL('SELECT COUNT(*) FROM {}.{}').format(
            sql.Identifier(schema_name),
            sql.Identifier(relation_name),
        ),
    )

    return int(row_count)


def check_required_columns(
    cursor,
    schema_name: str,
    table_name: str,
    required_columns: list[str],
) -> None:
    """Check that a table contains all required columns."""
    cursor.execute(
        """
        SELECT required_columns.column_name
        FROM UNNEST(%s::TEXT[]) AS required_columns(column_name)
        WHERE NOT EXISTS (
            SELECT 1
            FROM information_schema.columns existing_columns
            WHERE existing_columns.table_schema = %s
              AND existing_columns.table_name = %s
              AND existing_columns.column_name = required_columns.column_name
        )
        ORDER BY required_columns.column_name
        """,
        (required_columns, schema_name, table_name),
    )

    missing_columns = [row[0] for row in cursor.fetchall()]

    if missing_columns:
        raise ValueError(
            f'{schema_name}.{table_name} is missing required columns: '
            f'{missing_columns}'
        )
