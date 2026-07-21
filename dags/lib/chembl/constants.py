"""Constants for the ChEMBL similarity pipeline."""

from __future__ import annotations

import os

DWH_CONN_ID = 'dwh_postgres'
AWS_CONN_ID = 'aws_s3'
MSTEAMS_CONN_ID = 'msteams_webhook'

S3_BUCKET = os.getenv('S3_BUCKET', 'de-school-educational-data')
S3_BASE_PREFIX = os.getenv('S3_BASE_PREFIX', 'final_task/karbaia_nano').strip('/')

BRONZE_SCHEMA = 'bronze'
SILVER_SCHEMA = 'silver'
GOLD_SCHEMA = 'gold'

CHEMBL_API_BASE_URL = 'https://www.ebi.ac.uk/chembl/api/data'
CHEMBL_FTP_BASE_URL = 'https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases'

CHEMBL_CACHE_DIR = os.getenv('CHEMBL_CACHE_DIR', '/opt/airflow/chembl_cache')
DEFAULT_CHEMBL_VERSION = '37'

FINGERPRINT_RADIUS = 2
FINGERPRINT_N_BITS = 2048

DEFAULT_CHEMBL_PAGE_LIMIT = 1000
DEFAULT_SOURCE_MOLECULE_LIMIT = 100
DEFAULT_TOP_N = 10
DEFAULT_LOAD_BATCH_SIZE = 10_000
