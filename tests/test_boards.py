import pickle

from extrainterpreters.memoryboard import LockableBoardParent, LockableBoardChild, FileLikeArray
from extrainterpreters import Interpreter

import pytest

def test_lockableboard_single_item_back_and_forth(lowlevel):
    board = LockableBoardParent()
    size = board.collect()
    index, _ = board.new_item(obj:={"a":1})
    assert index == 0
    assert board.collect() == size - 1

    board2 = LockableBoardChild(*board.map._data_for_remote())
    index, new_obj = board2.get_work_data()
    assert index == 0
    assert new_obj == obj and new_obj is not obj
    assert board.collect() == size


def test_filelikearray_creates_a_remote_memory_buffer_on_unpickle(lowlevel):
    aa = FileLikeArray(bytearray([0,] * 256))
    bb = pickle.dumps(aa)
    cc = pickle.loads(bb)
    assert isinstance(cc.data, memoryview)
    cc[0] = 42
    assert aa[0] == 42


def test_lockableboard_unpickles_as_child_counterpart(lowlevel):
    import pickle
    aa = LockableBoardParent()
    aa.new_item((1,2))
    bb = pickle.loads(pickle.dumps(aa))
    assert isinstance(bb, LockableBoardChild)
    assert bb.get_work_data()[1] == (1, 2)
