"""MS Teams alerting through an Airflow connection."""

from __future__ import annotations

import http
import logging
from urllib.parse import urlencode

import requests

try:
    from airflow.sdk.bases.hook import BaseHook
except ImportError:
    from airflow.hooks.base import BaseHook


logger = logging.getLogger(__name__)

WEBHOOK_CONN_ID = 'msteams_webhook'


def get_webhook_url(conn_id: str = WEBHOOK_CONN_ID) -> str:
    """Build MS Teams webhook URL from an Airflow connection."""
    conn = BaseHook.get_connection(conn_id)

    scheme = conn.conn_type or 'https'
    netloc = conn.host

    if conn.port:
        netloc = f'{netloc}:{conn.port}'

    path = conn.schema or ''
    url = f'{scheme}://{netloc}/{path}'.rstrip('/')

    extra = conn.extra_dejson
    if extra:
        url = f'{url}?{urlencode(extra)}'

    return url


def build_adaptive_card_payload(
    dag_id: str,
    task_id: str,
    run_id: str,
    try_number: int,
    log_url: str,
    exception: Exception | str | None,
) -> dict:
    """Build Teams Adaptive Card payload."""
    exception_text = str(exception) if exception else 'Unknown error'

    return {
        'type': 'message',
        'attachments': [
            {
                'contentType': 'application/vnd.microsoft.card.adaptive',
                'contentUrl': None,
                'content': {
                    '$schema': 'http://adaptivecards.io/schemas/adaptive-card.json',
                    'type': 'AdaptiveCard',
                    'version': '1.2',
                    'body': [
                        {
                            'type': 'TextBlock',
                            'text': '🚨 Airflow Task Failed',
                            'weight': 'Bolder',
                            'size': 'Large',
                            'color': 'Attention',
                        },
                        {
                            'type': 'FactSet',
                            'facts': [
                                {'title': 'DAG', 'value': dag_id},
                                {'title': 'Task', 'value': task_id},
                                {'title': 'Run ID', 'value': run_id},
                                {'title': 'Try number', 'value': str(try_number)},
                                {'title': 'Exception', 'value': exception_text},
                            ],
                        },
                        {
                            'type': 'TextBlock',
                            'text': f'[Open Airflow logs]({log_url})',
                            'wrap': True,
                        },
                    ],
                },
            }
        ],
    }


def send_teams_alert(context) -> None:
    """Send an MS Teams alert when an Airflow task fails."""
    try:
        webhook_url = get_webhook_url()

        task_instance = context['task_instance']
        dag_id = task_instance.dag_id
        task_id = task_instance.task_id
        run_id = task_instance.run_id
        try_number = task_instance.try_number
        log_url = task_instance.log_url
        exception = context.get('exception')

        payload = build_adaptive_card_payload(
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            try_number=try_number,
            log_url=log_url,
            exception=exception,
        )

        last_error = None
        response = None

        for attempt in range(1, 4):
            try:
                response = requests.post(
                    webhook_url,
                    json=payload,
                    headers={'Content-Type': 'application/json'},
                    timeout=(10, 90),
                )

                if response.status_code in (
                    http.HTTPStatus.OK,
                    http.HTTPStatus.ACCEPTED,
                ):
                    logger.info('MS Teams alert sent successfully.')
                    return

                logger.warning(
                    'MS Teams alert attempt %s/3 failed. '
                    'status_code=%s, response=%s',
                    attempt,
                    response.status_code,
                    response.text,
                )

            except requests.RequestException as exc:
                last_error = exc

                logger.warning(
                    'MS Teams alert attempt %s/3 failed with request error: %s',
                    attempt,
                    exc,
                )

        if response is not None:
            logger.error(
                'Failed to send MS Teams alert after 3 attempts. '
                'Last status_code=%s, response=%s',
                response.status_code,
                response.text,
            )
        else:
            logger.error(
                'Failed to send MS Teams alert after 3 attempts. '
                'Last error=%s',
                last_error,
            )

    except Exception as exc:
        logger.exception('Failed to send MS Teams alert. Error: %s', exc)
