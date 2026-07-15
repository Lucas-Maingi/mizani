from pathlib import Path

import pytest

from mizani.db import connect

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def con():
    connection = connect(":memory:")
    yield connection
    connection.close()


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES
