"""Tests for ChEMBL API loader retry and pagination logic."""

from __future__ import annotations

from typing import Any

import pytest
import requests

from lib.chembl import api_loader


class FakeResponse:
    """Simple fake HTTP response for tests."""

    def __init__(self, status_code: int, payload: dict | None = None) -> None:
        self.status_code = status_code
        self.payload = payload or {}
        self.text = str(self.payload)

    def json(self) -> dict:
        """Return fake JSON payload."""
        return self.payload

    def raise_for_status(self) -> None:
        """Raise HTTP error for failed response."""
        if self.status_code >= 400:
            raise requests.HTTPError(f'{self.status_code} error')


class FakeSession:
    """Fake requests session returning predefined responses."""

    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, params: dict, timeout: Any) -> FakeResponse:
        """Return next fake response."""
        self.calls.append(
            {
                'url': url,
                'params': params,
                'timeout': timeout,
            }
        )

        return self.responses.pop(0)


def test_fetch_chembl_page_retries_retryable_status(monkeypatch) -> None:
    session = FakeSession(
        responses=[
            FakeResponse(status_code=500),
            FakeResponse(
                status_code=200,
                payload={'molecules': [{'molecule_chembl_id': 'CHEMBL1'}]},
            ),
        ]
    )

    monkeypatch.setattr(api_loader.time, 'sleep', lambda _: None)

    result = api_loader.fetch_chembl_page(
        session=session,
        resource='molecule',
        limit=100,
        offset=0,
    )

    assert result == {'molecules': [{'molecule_chembl_id': 'CHEMBL1'}]}
    assert len(session.calls) == 2


def test_fetch_chembl_page_does_not_retry_non_retryable_status(monkeypatch) -> None:
    session = FakeSession(
        responses=[
            FakeResponse(status_code=404),
        ]
    )

    monkeypatch.setattr(api_loader.time, 'sleep', lambda _: None)

    with pytest.raises(requests.HTTPError):
        api_loader.fetch_chembl_page(
            session=session,
            resource='missing_resource',
            limit=100,
            offset=0,
        )

    assert len(session.calls) == 1


def test_fetch_chembl_page_retries_timeout(monkeypatch) -> None:
    class TimeoutThenSuccessSession:
        """Fake session that times out once, then succeeds."""

        def __init__(self) -> None:
            self.calls = 0

        def get(self, url: str, params: dict, timeout: Any) -> FakeResponse:
            self.calls += 1

            if self.calls == 1:
                raise requests.Timeout('temporary timeout')

            return FakeResponse(
                status_code=200,
                payload={'molecules': [{'molecule_chembl_id': 'CHEMBL1'}]},
            )

    session = TimeoutThenSuccessSession()
    monkeypatch.setattr(api_loader.time, 'sleep', lambda _: None)

    result = api_loader.fetch_chembl_page(
        session=session,
        resource='molecule',
        limit=100,
        offset=0,
    )

    assert result == {'molecules': [{'molecule_chembl_id': 'CHEMBL1'}]}
    assert session.calls == 2


def test_fetch_chembl_records_uses_pagination(monkeypatch) -> None:
    calls = []

    def fake_fetch_chembl_page(session, resource, limit, offset):
        calls.append(
            {
                'resource': resource,
                'limit': limit,
                'offset': offset,
            }
        )

        if offset == 0:
            return {
                'molecules': [
                    {'molecule_chembl_id': 'CHEMBL1'},
                    {'molecule_chembl_id': 'CHEMBL2'},
                ],
                'page_meta': {'next': 'next-page'},
            }

        return {
            'molecules': [
                {'molecule_chembl_id': 'CHEMBL3'},
            ],
            'page_meta': {'next': None},
        }

    monkeypatch.setattr(
        api_loader,
        'fetch_chembl_page',
        fake_fetch_chembl_page,
    )

    records = list(
        api_loader.fetch_chembl_records(
            resource='molecule',
            records_key='molecules',
            record_limit=3,
            page_limit=2,
        )
    )

    assert records == [
        {'molecule_chembl_id': 'CHEMBL1'},
        {'molecule_chembl_id': 'CHEMBL2'},
        {'molecule_chembl_id': 'CHEMBL3'},
    ]

    assert calls == [
        {'resource': 'molecule', 'limit': 2, 'offset': 0},
        {'resource': 'molecule', 'limit': 1, 'offset': 2},
    ]


def test_fetch_chembl_records_rejects_invalid_limits() -> None:
    with pytest.raises(ValueError, match='record_limit'):
        list(
            api_loader.fetch_chembl_records(
                resource='molecule',
                records_key='molecules',
                record_limit=0,
                page_limit=100,
            )
        )

    with pytest.raises(ValueError, match='page_limit'):
        list(
            api_loader.fetch_chembl_records(
                resource='molecule',
                records_key='molecules',
                record_limit=100,
                page_limit=0,
            )
        )
