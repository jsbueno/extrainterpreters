import time
from pathlib import Path

import pytest

import extrainterpreters
from extrainterpreters import interpreters, raw_list_all


def test_running_plain_call_works():
    from math import cos

    with extrainterpreters.Interpreter() as interp:
        assert interp.run(cos, 0.0) == 1.0


def test_interpreter_is_destroyed_after_context_exits():
    with extrainterpreters.Interpreter() as interp:
        intno = interp.intno
        assert intno in raw_list_all()

    assert intno not in raw_list_all()


def test_extrainterpreters_list_all():
    with extrainterpreters.Interpreter() as interp:
        intno = interp.intno
        assert interp in extrainterpreters.list_all()

    assert interp not in extrainterpreters.list_all()


def test_interpreter_closes_out_of_scope():
    intno = None

    def inner():
        nonlocal intno
        interp = extrainterpreters.Interpreter().start()
        intno = interp.intno
        assert intno in raw_list_all()

    inner()
    assert intno not in raw_list_all()


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


@pytest.fixture
def add_current_path():
    import sys

    path = sys.path[:]
    sys.path.insert(0, str(Path(__file__).absolute().parent))
    try:
        yield
    finally:
        sys.path[:] = path


def test_running_threaded_call_works_local(add_current_path):
    import sys
    import helper_01

    with extrainterpreters.Interpreter() as interp:
        assert interp.run_in_thread(helper_01.to_run_remotely)
        while not interp.done():
            time.sleep(time_res)
        assert interp.result() == 42


@pytest.mark.skip(
    "Closing the interpreter fails if 'is_running' is called while it is running. Wait for fix in interpreters or workaround"
)
def test_interpreter_is_running(add_current_path):
    import sys
    import helper_01

    with extrainterpreters.Interpreter() as interp:
        assert interp.run_in_thread(helper_01.to_run_remotely)
        while not interp.done():
            # assert interpreters.is_runnining(interp.intno)
            assert interp.is_running()
            time.sleep(time_res)
        assert not interp.is_running()


def test_interpreter_fails_trying_to_send_data_larger_than_buffer():
    with extrainterpreters.Interpreter() as interp:
        with pytest.raises(BufferError):
            interp.run(str.upper, "a" * (extrainterpreters.BFSZ))


def test_interpreter_fails_trying_to_receive_data_larger_than_buffer(add_current_path):
    import helper_01

    with extrainterpreters.Interpreter() as interp:
        with pytest.raises(interpreters.RunFailedError):
            interp.run(helper_01.big_return_payload)


def text_extrainterpreters_can_be_imported_in_sub_interpreter():
    with extrainterpreters.Interpreter() as interp:
        interp.run_string("import extrainterpreters")


def test_interpreter_target_argument(add_current_path):
    import sys
    import helper_01

    interp = extrainterpreters.Interpreter(target=helper_01.to_run_remotely)
    assert not interp.intno
    interp.start()
    assert interp.intno
    interp.join()
    assert interp.result() == 42


def test_interpreter_run_can_instantiate_class(add_current_path):
    import sys
    import helper_01

    with extrainterpreters.Interpreter() as interp:
        assert interp.run_in_thread(helper_01.RemoteClass)
        while not interp.done():
            time.sleep(time_res)
        assert isinstance(interp.result(), helper_01.RemoteClass)


def test_interpreter_run_can_call_classmethod(add_current_path):
    import sys
    import helper_01

    with extrainterpreters.Interpreter() as interp:
        assert interp.run_in_thread(helper_01.RemoteClass.to_run_remotely_2)
        while not interp.done():
            time.sleep(time_res)
    assert interp.result() == 42


def test_interpreter_run_can_call_callable(add_current_path):
    import sys
    import helper_01

    with extrainterpreters.Interpreter() as interp:
        inst = helper_01.RemoteClass()
        interp.run_in_thread(inst)
        while not interp.done():
            time.sleep(time_res)
    assert interp.result() == 23
