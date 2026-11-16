"""Environment smoke test: the pinned toolchain is importable and on the expected Python."""

import sys


def test_python_version():
    assert sys.version_info[:2] == (3, 12)


def test_core_libraries_import():
    import duckdb
    import psycopg
    import pyiceberg

    assert pyiceberg.__version__
    assert duckdb.__version__
    assert psycopg.__version__
