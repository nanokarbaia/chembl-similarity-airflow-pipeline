"""MS Teams alerting through an Airflow connection."""

from __future__ import annotations

import logging
import os
from http import HTTPStatus
from typing import Any
from urllib.parse import urlencode, urlsplit, urlunsplit

import requests

try:
    from airflow.sdk.bases.hook import BaseHook
except ImportError:
    from airflow.hooks.base import BaseHook

from lib.chembl.constants import MSTEAMS_CONN_ID

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 3
REQUEST_TIMEOUT = (10, 90)
MAX_EXCEPTION_LENGTH = 1500

GIF_URL = 'https://media.giphy.com/media/l1EsZj5uwpdEJJSJq/giphy.gif'


def truncate_text(value: str, max_length: int = MAX_EXCEPTION_LENGTH) -> str:
    """Truncate long text for Teams message safety."""
    if len(value) <= max_length:
        return value

    return f'{value[:max_length]}...'


def build_public_log_url(log_url: str) -> str:
    """Convert internal Docker Airflow log URL to browser-accessible URL."""
    public_base_url = os.getenv(
        'AIRFLOW_PUBLIC_BASE_URL',
        'http://localhost:8082',
    ).rstrip('/')

    parsed_log_url = urlsplit(log_url)
    parsed_public_url = urlsplit(public_base_url)

    return urlunsplit(
        (
            parsed_public_url.scheme,
            parsed_public_url.netloc,
            parsed_log_url.path,
            parsed_log_url.query,
            parsed_log_url.fragment,
        )
    )


def build_url_from_connection(conn_id: str = MSTEAMS_CONN_ID) -> str:
    """Build MS Teams webhook URL from an Airflow connection."""
    conn = BaseHook.get_connection(conn_id)

    host = conn.host or ''

    if host.startswith(('http://', 'https://')):
        url = host.rstrip('/')
    else:
        scheme = conn.conn_type or 'https'
        netloc = host

        if conn.port:
            netloc = f'{netloc}:{conn.port}'

        path = (conn.schema or '').strip('/')
        url = f'{scheme}://{netloc}'

        if path:
            url = f'{url}/{path}'

    extra = conn.extra_dejson

    if extra:
        separator = '&' if urlsplit(url).query else '?'
        url = f'{url}{separator}{urlencode(extra)}'

    return url


def get_webhook_url(conn_id: str = MSTEAMS_CONN_ID) -> str:
    """Return MS Teams webhook URL from Airflow connection."""
    webhook_url = build_url_from_connection(conn_id=conn_id)

    if not webhook_url.startswith(('http://', 'https://')):
        raise ValueError(
            f'Invalid MS Teams webhook URL for connection: {conn_id}'
        )

    return webhook_url


def build_adaptive_card_payload(
    dag_id: str,
    task_id: str,
    run_id: str,
    try_number: int,
    log_url: str,
    exception: Exception | str | None,
) -> dict[str, Any]:
    """Build Teams Adaptive Card payload."""
    exception_text = truncate_text(
        str(exception) if exception else 'Unknown error'
    )

    return {
        'type': 'message',
        'attachments': [
            {
                'contentType': 'application/vnd.microsoft.card.adaptive',
                'contentUrl': None,
                'content': {
                    '$schema': (
                        'http://adaptivecards.io/schemas/'
                        'adaptive-card.json'
                    ),
                    'type': 'AdaptiveCard',
                    'version': '1.2',
                    'body': [
                        {
                            'type': 'TextBlock',
                            'text': (
                                '🧪 Molecule by molecule... '
                                'something broke!'
                            ),
                            'weight': 'Bolder',
                            'size': 'Large',
                            'color': 'Attention',
                            'wrap': True,
                        },
                        {
                            'type': 'TextBlock',
                            'text': 'Failure alert by Nano Karbaia',
                            'weight': 'Bolder',
                            'spacing': 'Small',
                            'wrap': True,
                        },
                        {
                            'type': 'Image',
                            'url': GIF_URL,
                            'size': 'Stretch',
                            'horizontalAlignment': 'Center',
                            'altText': 'Molecule by molecule failure alert',
                        },
                        {
                            'type': 'TextBlock',
                            'text': (
                                'The pipeline was processing molecule by '
                                'molecule, and then one molecule chose '
                                'violence. Please check the logs before '
                                'Plankton starts debugging production.'
                            ),
                            'wrap': True,
                        },
                        {
                            'type': 'FactSet',
                            'facts': [
                                {'title': 'DAG', 'value': dag_id},
                                {
                                    'title': 'Broken reaction step',
                                    'value': task_id,
                                },
                                {'title': 'Run ID', 'value': run_id},
                                {
                                    'title': 'Try number',
                                    'value': str(try_number),
                                },
                                {
                                    'title': 'Exception',
                                    'value': exception_text,
                                },
                            ],
                        },
                        {
                            'type': 'TextBlock',
                            'text': 'Status: Needs human catalyst ⚗️',
                            'weight': 'Bolder',
                            'wrap': True,
                        },
                    ],
                    'actions': [
                        {
                            'type': 'Action.OpenUrl',
                            'title': 'Open Airflow logs',
                            'url': log_url,
                        }
                    ],
                },
            }
        ],
    }


def send_teams_alert(context: dict[str, Any]) -> None:
    """Send an MS Teams alert when an Airflow task fails."""
    try:
        webhook_url = get_webhook_url()

        task_instance = context['task_instance']
        dag_id = task_instance.dag_id
        task_id = task_instance.task_id
        run_id = task_instance.run_id
        try_number = task_instance.try_number
        log_url = build_public_log_url(task_instance.log_url)
        exception = context.get('exception')

        payload = build_adaptive_card_payload(
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            try_number=try_number,
            log_url=log_url,
            exception=exception,
        )

        last_error: Exception | None = None
        response: requests.Response | None = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                response = requests.post(
                    webhook_url,
                    json=payload,
                    headers={'Content-Type': 'application/json'},
                    timeout=REQUEST_TIMEOUT,
                )

                if response.status_code in (
                    HTTPStatus.OK,
                    HTTPStatus.ACCEPTED,
                ):
                    logger.info('MS Teams alert sent successfully.')
                    return

                logger.warning(
                    'MS Teams alert attempt %s/%s failed. '
                    'status_code=%s, response=%s',
                    attempt,
                    MAX_ATTEMPTS,
                    response.status_code,
                    response.text,
                )

            except requests.RequestException as exc:
                last_error = exc

                logger.warning(
                    'MS Teams alert attempt %s/%s failed with request error: %s',
                    attempt,
                    MAX_ATTEMPTS,
                    exc,
                )

        if response is not None:
            logger.error(
                'Failed to send MS Teams alert after %s attempts. '
                'Last status_code=%s, response=%s',
                MAX_ATTEMPTS,
                response.status_code,
                response.text,
            )
        else:
            logger.error(
                'Failed to send MS Teams alert after %s attempts. '
                'Last error=%s',
                MAX_ATTEMPTS,
                last_error,
            )

    except Exception as exc:
        logger.exception('Failed to send MS Teams alert. Error: %s', exc)
