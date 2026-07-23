"""Tests for MS Teams notification helper."""

from __future__ import annotations

import http

from lib.utils import teams


class FakeTaskInstance:
    """Fake Airflow task instance for alert tests."""

    dag_id = 'chembl_similarity_dag'
    task_id = 'test_task'
    run_id = 'manual_test'
    try_number = 1
    log_url = 'http://localhost:8082'


class FakeResponse:
    """Fake HTTP response."""

    status_code = http.HTTPStatus.ACCEPTED
    text = 'accepted'


def test_send_teams_alert_posts_message_payload(monkeypatch) -> None:
    posted_requests = []

    monkeypatch.setattr(
        teams,
        'get_webhook_url',
        lambda: 'https://example.test/webhook',
    )

    def fake_post(url, json, headers, timeout):
        posted_requests.append(
            {
                'url': url,
                'json': json,
                'headers': headers,
                'timeout': timeout,
            }
        )
        return FakeResponse()

    monkeypatch.setattr(teams.requests, 'post', fake_post)

    teams.send_teams_alert(
        {
            'task_instance': FakeTaskInstance(),
            'exception': Exception('Test failure'),
        }
    )

    assert len(posted_requests) == 1
    assert posted_requests[0]['url'] == 'https://example.test/webhook'
    assert posted_requests[0]['headers']['Content-Type'] == 'application/json'
    assert posted_requests[0]['json']['type'] == 'message'
