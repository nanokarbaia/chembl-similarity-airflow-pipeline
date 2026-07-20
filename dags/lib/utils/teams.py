"""MS Teams alerting through an Airflow connection."""

from __future__ import annotations

import http
import logging
from urllib.parse import urlencode

import requests
from airflow.hooks.base import BaseHook

WEBHOOK_CONN_ID = 'msteams_webhook'


def get_webhook_url(conn_id: str = WEBHOOK_CONN_ID) -> str:
    conn = BaseHook.get_connection(conn_id)

    scheme = conn.conn_type or 'https'
    netloc = conn.host

    if conn.port:
        netloc = f'{netloc}:{conn.port}'

    url = f'{scheme}://{netloc}/{conn.schema or ""}'

    extra = conn.extra_dejson
    if extra:
        url = f'{url}?{urlencode(extra)}'

    return url


def send_teams_alert(context) -> None:
    try:
        webhook_url = get_webhook_url()

        task_instance = context['task_instance']
        dag_id = task_instance.dag_id
        task_id = task_instance.task_id
        run_id = task_instance.run_id
        try_number = task_instance.try_number
        log_url = task_instance.log_url
        exception = context.get('exception')

        payload = {
            '@type': 'MessageCard',
            '@context': 'http://schema.org/extensions',
            'summary': f'Airflow task failed: {dag_id}.{task_id}',
            'title': '🚨 Airflow Task Failed',
            'themeColor': 'D13438',
            'sections': [
                {
                    'facts': [
                        {'name': 'DAG', 'value': dag_id},
                        {'name': 'Task', 'value': task_id},
                        {'name': 'Run ID', 'value': run_id},
                        {'name': 'Try number', 'value': str(try_number)},
                        {
                            'name': 'Exception',
                            'value': str(exception) if exception else 'Unknown error',
                        },
                        {'name': 'Log URL', 'value': log_url},
                    ],
                    'markdown': True,
                }
            ],
        }

        response = requests.post(
            webhook_url,
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=30,
        )

        if response.status_code in (
            http.HTTPStatus.OK,
            http.HTTPStatus.ACCEPTED,
        ):
            logging.info('MS Teams alert sent successfully.')
        else:
            logging.error(
                'Failed to send MS Teams alert: %s %s',
                response.status_code,
                response.text,
            )

    except Exception as exc:
        logging.exception('Failed to send MS Teams alert. Error: %s', exc)
