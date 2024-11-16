import pickle
import time
from textwrap import dedent as D


from extrainterpreters.memoryboard import (
    LockableBoard,
    _CrossInterpreterStructLock,
    RemoteArray,
)
from extrainterpreters.remote_array import RemoteArray, RemoteDataState, RemoteState
from extrainterpreters import Interpreter
from extrainterpreters import memoryboard
from extrainterpreters import remote_array
from extrainterpreters.utils import StructBase, Field

import extrainterpreters as ei

import pytest


def test_lockableboard_single_item_back_and_forth(lowlevel):
    board = LockableBoard()
    size = board.collect()
    index, _ = board.new_item(obj := {"a": 1})
    assert index == 0
    assert board.collect() == size - 1

    board2 = pickle.loads(pickle.dumps(board))
    index, new_obj = board2.fetch_item()
    assert index == 0
    assert new_obj == obj and new_obj is not obj
    assert board.collect() == size


def test_lockableboard_to_other_interpreter(lowlevel):
    board = LockableBoard()
    size = board.collect()
    interp = ei.Interpreter().start()
    board.new_item((1, 2))
    interp.run_string(
        D(
            f"""
        import extrainterpreters; extrainterpreters.DEBUG=True
        board = pickle.loads({pickle.dumps(board)})
        index, item = board.fetch_item()
        assert item == (1,2)
        board.new_item((3,4))
    """
        )
    )
    index, item = board.fetch_item()
    assert item == (3, 4)
    assert board.collect() == size
    # TBD: this design leaks "board.locks" items
    # until a collection is also done on the subinterpreter.
    # (leaking is limited to the number of slots on the memoryboard,tough)


# FileLikeArray is no more
# def test_filelikearray_creates_a_remote_memory_buffer_on_unpickle(lowlevel):
# aa = FileLikeArray(bytearray([0,] * 256))
# bb = pickle.dumps(aa)
# cc = pickle.loads(bb)
# assert isinstance(cc.data, memoryview)
# cc[0] = 42
# assert aa[0] == 42

# LockableBoardChild is no more
# def test_lockableboard_unpickles_as_child_counterpart(lowlevel):
# import pickle
# aa = LockableBoardParent()
# aa.new_item((1,2))
# bb = pickle.loads(pickle.dumps(aa))
# assert isinstance(bb, LockableBoardChild)
# assert bb.()[1] == (1, 2)


@pytest.fixture
def lockable():
    class LockableStruct(StructBase):
        lock = Field(1)
        value = Field(8)

    return LockableStruct(lock=0, value=0)


def test_structlock_works(lockable):
    lock = _CrossInterpreterStructLock(lockable)
    assert lockable.lock == 0
    with lock:
        assert lockable.lock == 1
    assert lockable.lock == 0


def test_structlock_reentrant(lockable):
    lock = _CrossInterpreterStructLock(lockable)
    assert lockable.lock == 0
    with lock:
        with lock:
            assert lockable.lock == 1
        assert lockable.lock == 1
    assert lockable.lock == 0


def test_structlock_locks(lockable):
    lock = _CrossInterpreterStructLock(lockable)
    lock2 = _CrossInterpreterStructLock(lockable, timeout=None)
    assert lockable.lock == 0
    with lock:
        with pytest.raises(RuntimeError):
            with lock2:
                pass
        assert lockable.lock == 1
    with lock2:
        assert lockable.lock == 1


@pytest.mark.parametrize("set_timeout", [None, 0.0001])
def test_structlock_timeout_is_restored_on_acquire_fail(lockable, set_timeout):
    lock = _CrossInterpreterStructLock(lockable, timeout=1)
    lock2 = _CrossInterpreterStructLock(lockable, timeout=0.002)

    with lock:
        with pytest.raises((ei.ResourceBusyError, TimeoutError)):
            with lock2.timeout(set_timeout):
                pass
    assert lock2._timeout == lock2._original_timeout


def test_remotearray_base():
    # temp work around: other tests are leaking interpreters and boards!
    xx = remote_array._array_registry
    try:
        remote_array._array_registry = []
        buffer = RemoteArray(size=2048, ttl=0.05)
        assert buffer.header.state == RemoteState.building
        with buffer:
            assert buffer.header.state == RemoteState.ready
            serialized = pickle.dumps(buffer)
            assert buffer.header.state == RemoteState.serialized
            new_buffer = pickle.loads(serialized)
            assert buffer.header.state == RemoteState.serialized
            with new_buffer:
                assert buffer.header.state == RemoteState.received

                new_buffer[0] = 255

                assert buffer.header.enter_count == 1
                assert buffer.header.exit_count == 0
            with pytest.raises(RuntimeError):
                new_buffer[0] = 128
            assert buffer.header.exit_count == 1
            assert buffer[0] == 255
            time.sleep(buffer._ttl * 1.1)
        assert buffer._data is None
        assert len(remote_array._array_registry) == 0
    finally:
        remote_array._array_registry = xx


def test_remotearray_not_deleted_before_ttl_expires():
    import gc
    xx = remote_array._array_registry
    try:
        remote_array._array_registry = []

        buffer = RemoteArray(size=2048, ttl=0.1)
        assert buffer.header.state == RemoteState.building
        with buffer:
            serialized = pickle.dumps(buffer)
            new_buffer = pickle.loads(serialized)
            with new_buffer:
                pass
            assert buffer.header.exit_count == 1
            original_data = buffer._data
            gc.disable()
        assert buffer._data is None
        assert buffer._data_state == RemoteDataState.not_ready
        assert len(remote_array._array_registry) == 1
        dead = remote_array._array_registry[0]
        assert dead._data is original_data

        gc.enable()

        time.sleep(buffer._ttl * 1.1)
        gc.collect()
        assert len(remote_array._array_registry) == 0
    finally:
        remote_array._array_registry = xx


def test_memoryboard_fetch_from_gone_interpreter_doesnot_crash(lowlevel):
    interp = ei.Interpreter().start()
    board = LockableBoard()
    interp.run_string(
        D(
            f"""\
        import extrainterpreters; extrainterpreters.DEBUG = True
        board = pickle.loads({pickle.dumps(board)})
        board.new_item((1,2))

    """
        )
    )
    index, item = board.fetch_item()
    assert item == (1, 2)
    interp.run_string(
        D(
            f"""\
        board.new_item((3, 4))
    """
        )
    )
    interp.close()
    assert board.fetch_item() is None
