import pytest

@pytest.fixture
def lowlevel():
    import extrainterpreters
    extrainterpreters.DEBUG = True
    yield
    del extrainterpreters.DEBUG
