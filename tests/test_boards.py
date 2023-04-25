from extrainterpreters.memoryboard import LockableBoardParent, LockableBoardChild
from extrainterpreters import Interpreter

import pytest

def test_lockableboard_single_item_back_and_forth():
    zz = LockableBoardParent()
    size = zz.collect()
    zz.new_item(obj:={"a":1})
    assert zz.collect() == size - 1

    yy = LockableBoardChild(*zz._data_for_remote())
    new_obj = yy.get_work_data()[1]
    assert new_obj == obj and new_obj is not obj
    assert zz.collect() == size
