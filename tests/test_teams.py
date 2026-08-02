"""Tests for MS Teams notification helper."""

from __future__ import annotations

from http import HTTPStatus

from lib.utils import teams


class FakeTaskInstance:
    """Fake Airflow task instance for alert tests."""

    dag_id = 'chembl_similarity_dag'
    task_id = 'test_task'
    run_id = 'manual_test'
    try_number = 1
    log_url = (
        'http://airflow-webserver:8080/dags/chembl_similarity_dag/'
        'runs/manual_test/tasks/test_task?try_number=1'
    )


class FakeResponse:
    """Fake HTTP response."""

    status_code = HTTPStatus.ACCEPTED
    text = 'accepted'


def test_build_public_log_url_replaces_internal_airflow_host(monkeypatch) -> None:
    monkeypatch.setenv('AIRFLOW_PUBLIC_BASE_URL', 'http://localhost:8082')

    result = teams.build_public_log_url(
        'http://airflow-webserver:8080/dags/chembl_similarity_dag/'
        'runs/manual_test/tasks/test_task?try_number=1'
    )

    assert result == (
        'http://localhost:8082/dags/chembl_similarity_dag/'
        'runs/manual_test/tasks/test_task?try_number=1'
    )


def test_build_adaptive_card_payload_contains_fun_alert_details() -> None:
    payload = teams.build_adaptive_card_payload(
        dag_id='chembl_similarity_dag',
        task_id='compute_fingerprints',
        run_id='manual_test',
        try_number=1,
        log_url='http://localhost:8082/logs',
        exception=Exception('Test failure'),
    )

    assert payload['type'] == 'message'

    payload_text = str(payload)

    assert 'Molecule by molecule... something broke!' in payload_text
    assert 'Failure alert by Nano Karbaia' in payload_text
    assert 'one molecule chose violence' in payload_text
    assert 'Plankton starts debugging production' in payload_text
    assert 'Needs human catalyst' in payload_text
    assert teams.GIF_URL in payload_text

    assert 'chembl_similarity_dag' in payload_text
    assert 'compute_fingerprints' in payload_text
    assert 'manual_test' in payload_text
    assert 'Test failure' in payload_text
    assert 'http://localhost:8082/logs' in payload_text


def test_send_teams_alert_posts_message_payload(monkeypatch) -> None:
    posted_requests = []

    monkeypatch.setenv('AIRFLOW_PUBLIC_BASE_URL', 'http://localhost:8082')

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

    payload_text = str(posted_requests[0]['json'])

    assert 'Molecule by molecule... something broke!' in payload_text
    assert 'Failure alert by Nano Karbaia' in payload_text
    assert 'chembl_similarity_dag' in payload_text
    assert 'test_task' in payload_text
    assert 'Test failure' in payload_text
    assert 'http://localhost:8082/dags/chembl_similarity_dag' in payload_text
