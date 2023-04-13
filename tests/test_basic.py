import time
from pathlib import Path

import pytest

import extrainterpreters
from extrainterpreters import interpreters

def test_running_plain_call_works():
    from math import cos

    with extrainterpreters.Interpreter() as interp:
        assert interp.run(cos, 0.) == 1.0

def test_interpreter_is_destroyed_after_context_exits():
    with extrainterpreters.Interpreter() as interp:
        intno = interp.intno
        assert intno in interpreters.list_all()

    assert intno not in interpreters.list_all()

def test_extrainterpreters_list_all():
    with extrainterpreters.Interpreter() as interp:
        intno = interp.intno
        assert interp in extrainterpreters.list_all()

    assert interp not in extrainterpreters.list_all()

def test_interpreter_if_out_of_scope_without_explicit_close():
    intno = None
    def inner():
        nonlocal intno
        interp = extrainterpreters.Interpreter().start()
        intno = interp.intno
        assert intno in interpreters.list_all()
    inner()
    assert intno not in interpreters.list_all()


def test_interpreter_cant_be_started_twice():
    interp = extrainterpreters.Interpreter().start()
    with pytest.raises(RuntimeError):
        interp.start()


time_res = 0.02

def test_running_threaded_call_works():
    from math import cos

    interp = extrainterpreters.Interpreter().start()
    assert interp.run_in_thread(cos, 0)
    while not interp.done():
        time.sleep(time_res)
    assert interp.result() == 1.0
    interp.close()

def to_run_remotely():
    import time
    time.sleep(time_res * 2)
    return 42

def test_running_threaded_call_works_local():
    import sys
    path = sys.path[:]
    sys.path.insert(0, str(Path(__file__).absolute().parent))
    import helper_01
    try:
        with extrainterpreters.Interpreter() as interp:
            assert interp.run_in_thread(helper_01.to_run_remotely)
            while not interp.done():
                time.sleep(time_res)
            assert interp.result() == 42
    finally:
        sys.path[:] = path
