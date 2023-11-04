import warnings

# Filter out deprecation warnings
warnings.filterwarnings(
    "ignore", category=DeprecationWarning, module="_pytest.assertion.rewrite"
)

import pytest


@pytest.fixture
def lowlevel():
    # sets the DEBUG package attribute
    # which disables callee checking for low-level
    # methods which can segfault the Python runtime process.
    import extrainterpreters

    extrainterpreters.DEBUG = True
    yield
    del extrainterpreters.DEBUG
