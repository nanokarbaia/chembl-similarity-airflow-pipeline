"""Constants and environment-based settings for the ChEMBL similarity pipeline."""

from __future__ import annotations

import os

from lib.utils.parsing import parse_positive_int


def get_env_value(name: str, default: str) -> str:
    """Read string environment variable with fallback."""
    value = os.getenv(name)

    if value is None or not value.strip():
        return default

    return value.strip()


def get_env_s3_prefix(name: str, default: str) -> str:
    """Read and normalize S3 prefix from environment variable."""
    return get_env_value(name=name, default=default).strip('/')


def get_env_int(name: str, default: int) -> int:
    """Read positive integer environment variable with fallback."""
    value = os.getenv(name)

    if value is None or not value.strip():
        return default

    return parse_positive_int(
        value=value,
        parameter_name=name,
    )


DWH_CONN_ID = 'dwh_postgres'
AWS_CONN_ID = 'aws_s3'
MSTEAMS_CONN_ID = 'msteams_webhook'

S3_BUCKET = get_env_value(
    name='S3_BUCKET',
    default='de-school-educational-data',
)

S3_BASE_PREFIX = get_env_s3_prefix(
    name='S3_BASE_PREFIX',
    default='final_task/karbaia_nano',
)

SOURCE_INPUT_PREFIX = get_env_s3_prefix(
    name='SOURCE_INPUT_PREFIX',
    default=f'{S3_BASE_PREFIX}/input',
)

S3_OUTPUT_PREFIX = f'{S3_BASE_PREFIX}/output'

FINGERPRINTS_S3_PREFIX = f'{S3_OUTPUT_PREFIX}/fingerprints'
SIMILARITY_S3_PREFIX = f'{S3_OUTPUT_PREFIX}/similarity_scores'
TOP10_S3_PREFIX = f'{S3_OUTPUT_PREFIX}/top10'

TOP10_FILE_NAME = 'top10_similar_molecules.parquet'
TOP10_S3_KEY = f'{TOP10_S3_PREFIX}/{TOP10_FILE_NAME}'

BRONZE_SCHEMA = 'bronze'
SILVER_SCHEMA = 'silver'
GOLD_SCHEMA = 'gold'

SOURCE_ID_COLUMNS = (
    'chembl_id',
    'molecule_chembl_id',
    'source_chembl_id',
    'compound_chembl_id',
)

CHEMBL_API_BASE_URL = 'https://www.ebi.ac.uk/chembl/api/data'
CHEMBL_FTP_BASE_URL = (
    'https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases'
)

CHEMBL_CACHE_DIR = get_env_value(
    name='CHEMBL_CACHE_DIR',
    default='/opt/airflow/chembl_cache',
)

DEFAULT_CHEMBL_VERSION = get_env_value(
    name='DEFAULT_CHEMBL_VERSION',
    default='37',
)

CHEMBL_API_SOURCE_SYSTEM = 'chembl_api'
CHEMBL_SQLITE_SOURCE_SYSTEM = 'chembl_sqlite_dump'

CHEMBL_ID_PATTERN = r'^CHEMBL[0-9]+$'

FINGERPRINT_RADIUS = 2
FINGERPRINT_N_BITS = 2048

DEFAULT_CHEMBL_PAGE_LIMIT = get_env_int(
    name='DEFAULT_CHEMBL_PAGE_LIMIT',
    default=1000,
)

DEFAULT_SOURCE_MOLECULE_LIMIT = get_env_int(
    name='DEFAULT_SOURCE_MOLECULE_LIMIT',
    default=100,
)

DEFAULT_TOP_N = get_env_int(
    name='DEFAULT_TOP_N',
    default=10,
)

DEFAULT_LOAD_BATCH_SIZE = get_env_int(
    name='DEFAULT_LOAD_BATCH_SIZE',
    default=10_000,
)

DEFAULT_FINGERPRINT_BATCH_SIZE = get_env_int(
    name='DEFAULT_FINGERPRINT_BATCH_SIZE',
    default=5_000,
)

DOWNLOAD_RETRIES = get_env_int(
    name='DOWNLOAD_RETRIES',
    default=5,
)

DOWNLOAD_CHUNK_SIZE_BYTES = get_env_int(
    name='DOWNLOAD_CHUNK_SIZE_BYTES',
    default=1024 * 1024,
)

DOWNLOAD_PROGRESS_STEP_BYTES = get_env_int(
    name='DOWNLOAD_PROGRESS_STEP_BYTES',
    default=100 * 1024 * 1024,
)

DOWNLOAD_CONNECT_TIMEOUT_SECONDS = get_env_int(
    name='DOWNLOAD_CONNECT_TIMEOUT_SECONDS',
    default=30,
)

DOWNLOAD_READ_TIMEOUT_SECONDS = get_env_int(
    name='DOWNLOAD_READ_TIMEOUT_SECONDS',
    default=300,
)

DOWNLOAD_BACKOFF_SECONDS = get_env_int(
    name='DOWNLOAD_BACKOFF_SECONDS',
    default=30,
)
