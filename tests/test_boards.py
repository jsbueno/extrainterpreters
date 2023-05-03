import pickle
import time

from extrainterpreters.memoryboard import LockableBoardParent, LockableBoardChild, _CrossInterpreterStructLock, RemoteArray
from extrainterpreters import memoryboard
from extrainterpreters.utils import StructBase, Field
from extrainterpreters import Interpreter

import pytest

def test_lockableboard_single_item_back_and_forth(lowlevel):
    board = LockableBoardParent()
    size = board.collect()
    index, _ = board.new_item(obj:={"a":1})
    assert index == 0
    assert board.collect() == size - 1

    board2 = LockableBoardChild(pickle.loads(pickle.dumps(board.map)).start())
    index, new_obj = board2.get_work_data()
    assert index == 0
    assert new_obj == obj and new_obj is not obj
    assert board.collect() == size

# FileLikeArray is no more
#def test_filelikearray_creates_a_remote_memory_buffer_on_unpickle(lowlevel):
    #aa = FileLikeArray(bytearray([0,] * 256))
    #bb = pickle.dumps(aa)
    #cc = pickle.loads(bb)
    #assert isinstance(cc.data, memoryview)
    #cc[0] = 42
    #assert aa[0] == 42


def test_lockableboard_unpickles_as_child_counterpart(lowlevel):
    import pickle
    aa = LockableBoardParent()
    aa.new_item((1,2))
    bb = pickle.loads(pickle.dumps(aa))
    assert isinstance(bb, LockableBoardChild)
    assert bb.get_work_data()[1] == (1, 2)


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


def test_remotearray_base():
    # temp work around: other tests are leaking interpreters and boards!
    xx = memoryboard._array_registry
    memoryboard._array_registry = []
    buffer = RemoteArray(size=2048, ttl=0.05)
    assert buffer.header.state == memoryboard.RemoteState.building
    with buffer:
        assert buffer.header.state == memoryboard.RemoteState.ready
        serialized = pickle.dumps(buffer)
        assert buffer.header.state == memoryboard.RemoteState.serialized
        new_buffer = pickle.loads(serialized)
        assert buffer.header.state == memoryboard.RemoteState.serialized
        with new_buffer:
            assert buffer.header.state == memoryboard.RemoteState.received

            new_buffer[0] = 255

            assert buffer.header.enter_count == 1
            assert buffer.header.exit_count == 0
        with pytest.raises(RuntimeError):
            new_buffer[0] = 128
        assert buffer.header.exit_count == 1
        assert buffer[0] == 255
        time.sleep(buffer._ttl * 1.1)
    assert buffer._data is None
    assert len(memoryboard._array_registry) == 0
    memoryboard._array_registry = xx


def test_remotearray_not_deleted_before_ttl_expires():
    xx = memoryboard._array_registry
    memoryboard._array_registry = []
    import gc
    buffer = RemoteArray(size=2048, ttl=0.1)
    assert buffer.header.state == memoryboard.RemoteState.building
    with buffer:
        serialized = pickle.dumps(buffer)
        new_buffer = pickle.loads(serialized)
        with new_buffer:
            pass
        assert buffer.header.exit_count == 1
        original_data = buffer._data
        gc.disable()
    assert buffer._data is None
    assert buffer._data_state == memoryboard.RemoteDataState.not_ready
    assert len(memoryboard._array_registry) == 1
    dead = memoryboard._array_registry[0]
    assert dead._data is original_data

    gc.enable()

    time.sleep(buffer._ttl * 1.1)
    gc.collect()
    assert len(memoryboard._array_registry) == 0
    memoryboard._array_registry = xx

