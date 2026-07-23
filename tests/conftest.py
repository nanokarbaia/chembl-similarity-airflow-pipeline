"""Shared pytest configuration."""

from __future__ import annotations

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = PROJECT_ROOT / 'dags'

if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

os.environ.setdefault('S3_BUCKET', 'test-bucket')
os.environ.setdefault('S3_BASE_PREFIX', 'final_task/test_user')
os.environ.setdefault('SOURCE_INPUT_PREFIX', 'input/test_user')
os.environ.setdefault('AWS_DEFAULT_REGION', 'eu-central-1')
