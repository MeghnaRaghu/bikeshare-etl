"""
conftest.py

Pytest auto-loads this file. It adds the dags/ folder to sys.path so
test files in the project root can import modules that live alongside
the DAGs (e.g. sync_helpers.py), without needing to duplicate code or
restructure the project.
"""

import os
import sys

DAGS_DIR = os.path.join(os.path.dirname(__file__), "dags")
if DAGS_DIR not in sys.path:
    sys.path.insert(0, DAGS_DIR)