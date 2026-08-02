"""Airflow DAG for the ChEMBL molecule similarity pipeline."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param

from lib.chembl.constants import (
    DEFAULT_CHEMBL_PAGE_LIMIT,
    DEFAULT_CHEMBL_VERSION,
    DEFAULT_FINGERPRINT_BATCH_SIZE,
    DEFAULT_SOURCE_MOLECULE_LIMIT,
    DEFAULT_TOP_N,
    SOURCE_INPUT_PREFIX,
)
from lib.chembl.fingerprints import compute_and_upload_fingerprints
from lib.chembl.gold import build_gold_data_mart, create_gold_views
from lib.chembl.ingestion import ingest_chembl_bronze
from lib.chembl.quality import (
    bronze_quality_checks,
    fingerprint_quality_checks,
    gold_quality_checks,
    silver_quality_checks,
    similarity_quality_checks,
    top10_quality_checks,
)
from lib.chembl.silver import prepare_silver_molecules
from lib.chembl.similarity import compute_similarity_scores_and_top10
from lib.utils.teams import send_teams_alert


DAG_ID = 'chembl_similarity_dag'

DEFAULT_ARGS = {
    'owner': 'data-platform',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'on_failure_callback': send_teams_alert,
}

DAG_DOC_MD = """
# ChEMBL molecule similarity pipeline

This DAG ingests ChEMBL molecule data, prepares cleaned molecule structures,
computes Morgan fingerprints, calculates Tanimoto similarity scores, and builds
a PostgreSQL data mart for top-10 molecule similarity analysis.

Main outputs:

- Bronze, silver, and gold DWH layers in PostgreSQL
- Morgan fingerprint parquet files in S3
- Full source-to-all similarity parquet files in S3
- Top-N similarity parquet file in S3
- Gold fact and dimension tables
- Reporting views for similarity analysis
"""


with DAG(
    dag_id=DAG_ID,
    description=(
        'Ingest ChEMBL data, compute Morgan fingerprints, calculate '
        'Tanimoto similarities, and build molecule similarity data marts.'
    ),
    doc_md=DAG_DOC_MD,
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=24),
    default_args=DEFAULT_ARGS,
    render_template_as_native_obj=True,
    tags=[
        'chembl',
        'similarity',
        'rdkit',
        's3',
        'postgres',
        'data-mart',
    ],
    params={
        'chembl_version': Param(
            default=DEFAULT_CHEMBL_VERSION,
            type='string',
            description=(
                'ChEMBL release version used for ingestion. '
                'If not overridden, the configured default version is used.'
            ),
        ),
        'chembl_page_limit': Param(
            default=DEFAULT_CHEMBL_PAGE_LIMIT,
            type='integer',
            minimum=1,
            description='Page size for paginated API ingestion requests.',
        ),
        'ingest_record_limit': Param(
            default=None,
            type=['null', 'integer'],
            minimum=1,
            description='Optional upper bound on ingested records per source table.',
        ),
        'source_molecule_limit': Param(
            default=DEFAULT_SOURCE_MOLECULE_LIMIT,
            type='integer',
            minimum=1,
            description=(
                'Maximum number of source molecules included in the similarity '
                'calculation.'
            ),
        ),
        'top_n': Param(
            default=DEFAULT_TOP_N,
            type='integer',
            minimum=1,
            description=(
                'Number of top similarity matches stored for each source molecule.'
            ),
        ),
        'fingerprint_batch_size': Param(
            default=DEFAULT_FINGERPRINT_BATCH_SIZE,
            type='integer',
            minimum=1,
            description=(
                'Batch size used during fingerprint generation and output writing.'
            ),
        ),
        'source_input_prefix': Param(
            default=SOURCE_INPUT_PREFIX,
            type='string',
            description=(
                'S3 prefix containing CSV files with source molecule identifiers.'
            ),
        ),
    },
) as dag:
    start_op = EmptyOperator(
        task_id='start',
    )

    ingest_chembl_data_op = PythonOperator(
        task_id='ingest_chembl_bronze',
        python_callable=ingest_chembl_bronze,
        op_kwargs={
            'chembl_version': '{{ params.chembl_version }}',
            'record_limit': '{{ params.ingest_record_limit }}',
            'page_limit': '{{ params.chembl_page_limit }}',
        },
    )

    bronze_quality_checks_op = PythonOperator(
        task_id='bronze_quality_checks',
        python_callable=bronze_quality_checks,
    )

    prepare_silver_layer_op = PythonOperator(
        task_id='prepare_silver_layer',
        python_callable=prepare_silver_molecules,
    )

    silver_quality_checks_op = PythonOperator(
        task_id='silver_quality_checks',
        python_callable=silver_quality_checks,
    )

    compute_fingerprints_op = PythonOperator(
        task_id='compute_fingerprints',
        python_callable=compute_and_upload_fingerprints,
        op_kwargs={
            'batch_size': '{{ params.fingerprint_batch_size }}',
        },
    )

    fingerprint_quality_checks_op = PythonOperator(
        task_id='fingerprint_quality_checks',
        python_callable=fingerprint_quality_checks,
    )

    compute_similarity_scores_op = PythonOperator(
        task_id='compute_similarity_scores_and_top10',
        python_callable=compute_similarity_scores_and_top10,
        op_kwargs={
            'source_input_prefix': '{{ params.source_input_prefix }}',
            'source_molecule_limit': '{{ params.source_molecule_limit }}',
            'top_n': '{{ params.top_n }}',
        },
    )

    similarity_quality_checks_op = PythonOperator(
        task_id='similarity_quality_checks',
        python_callable=similarity_quality_checks,
    )

    top10_quality_checks_op = PythonOperator(
        task_id='top10_quality_checks',
        python_callable=top10_quality_checks,
        op_kwargs={
            'top_n': '{{ params.top_n }}',
        },
    )

    build_data_mart_op = PythonOperator(
        task_id='build_data_mart',
        python_callable=build_gold_data_mart,
    )

    create_views_op = PythonOperator(
        task_id='create_views',
        python_callable=create_gold_views,
    )

    gold_quality_checks_op = PythonOperator(
        task_id='gold_quality_checks',
        python_callable=gold_quality_checks,
        op_kwargs={
            'top_n': '{{ params.top_n }}',
        },
    )

    finish_op = EmptyOperator(
        task_id='finish',
    )

    (
        start_op
        >> ingest_chembl_data_op
        >> bronze_quality_checks_op
        >> prepare_silver_layer_op
        >> silver_quality_checks_op
        >> compute_fingerprints_op
        >> fingerprint_quality_checks_op
        >> compute_similarity_scores_op
        >> similarity_quality_checks_op
        >> top10_quality_checks_op
        >> build_data_mart_op
        >> create_views_op
        >> gold_quality_checks_op
        >> finish_op
    )
