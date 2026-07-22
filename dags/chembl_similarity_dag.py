"""Airflow DAG for the ChEMBL molecule similarity pipeline."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param
from airflow.utils.trigger_rule import TriggerRule

from lib.chembl.constants import (
    DEFAULT_CHEMBL_PAGE_LIMIT,
    DEFAULT_FINGERPRINT_BATCH_SIZE,
    DEFAULT_SOURCE_MOLECULE_LIMIT,
    DEFAULT_TOP_N,
)
from lib.chembl.ingestion import ingest_chembl_bronze
from lib.chembl.silver import prepare_silver_molecules
from lib.chembl.fingerprints import compute_and_upload_fingerprints

from lib.utils.teams import send_teams_alert


with DAG(
    dag_id='chembl_similarity_dag',
    description='Ingest ChEMBL data, compute molecule similarities, and build data mart views.',
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    catchup=False,
    tags=['chembl', 'similarity', 'rdkit', 'de_school'],
    params={
        'chembl_version': Param(
            default=None,
            type=['null', 'string'],
            description='Optional ChEMBL version. Use null for latest available version.',
        ),
        'chembl_page_limit': Param(
            default=DEFAULT_CHEMBL_PAGE_LIMIT,
            type='integer',
            minimum=1,
            description='Kept for compatibility. Not used by SQLite dump ingestion.',
        ),
        'ingest_record_limit': Param(
            default=1000,
            type=['null', 'integer'],
            description='Optional development limit per ChEMBL table. Use null for full ingestion.',
        ),
        'source_molecule_limit': Param(
            default=DEFAULT_SOURCE_MOLECULE_LIMIT,
            type='integer',
            minimum=1,
            description='Number of source molecules for top-N similarity search.',
        ),
        'top_n': Param(
            default=DEFAULT_TOP_N,
            type='integer',
            minimum=1,
            description='Number of most similar molecules to keep per source molecule.',
        ),
        'fingerprint_batch_size': Param(
            default=DEFAULT_FINGERPRINT_BATCH_SIZE,
            type='integer',
            minimum=1,
            description='Number of silver molecules processed per fingerprint parquet file.',
        ),
    },
    dagrun_timeout=timedelta(hours=6),
    default_args={
        'owner': 'data-platform',
        'retries': 0,
        'retry_delay': timedelta(minutes=2),
        'retry_exponential_backoff': True,
        'max_retry_delay': timedelta(minutes=30),
        'on_failure_callback': send_teams_alert,
    },
) as dag:
    start_op = EmptyOperator(task_id='start')

    ingest_chembl_data_op = PythonOperator(
        task_id='ingest_chembl_bronze',
        python_callable=ingest_chembl_bronze,
    )

    prepare_silver_layer_op = PythonOperator(
        task_id='prepare_silver_layer',
        python_callable=prepare_silver_molecules,
    )

    compute_fingerprints_op = PythonOperator(
        task_id='compute_fingerprints',
        python_callable=compute_and_upload_fingerprints,
        op_kwargs={
            'batch_size': '{{ params.fingerprint_batch_size }}',
        },
    )

    compute_similarity_scores_op = EmptyOperator(task_id='compute_similarity_scores')
    extract_top10_similarities_op = EmptyOperator(task_id='extract_top10_similarities')
    build_data_mart_op = EmptyOperator(task_id='build_data_mart')
    create_views_op = EmptyOperator(task_id='create_views')

    finish_op = EmptyOperator(
        task_id='finish',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    (
        start_op
        >> ingest_chembl_data_op
        >> prepare_silver_layer_op
        >> compute_fingerprints_op
        >> compute_similarity_scores_op
        >> extract_top10_similarities_op
        >> build_data_mart_op
        >> create_views_op
        >> finish_op
    )
