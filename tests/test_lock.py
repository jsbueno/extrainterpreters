import pickle
from textwrap import dedent as D


import pytest

import extrainterpreters as ei


from extrainterpreters import Lock, RLock
from extrainterpreters.lock import IntRLock

@pytest.mark.parametrize("LockCls", [Lock, RLock, IntRLock])
def test_locks_are_acquireable(LockCls):
    lock = LockCls()
    assert not lock.locked()
    lock.acquire()
    assert lock.locked()
    lock.release()
    assert not lock.locked()


@pytest.mark.parametrize("LockCls", [Lock, RLock, IntRLock])
def test_locks_work_as_context_manager(LockCls):
    lock = LockCls()
    assert not lock.locked()
    with lock:
        assert lock.locked()
    assert not lock.locked()



def test_lock_cant_be_reacquired():
    lock = Lock()

    lock.acquire()

    with pytest.raises(TimeoutError):
        lock.acquire(timeout=0)


@pytest.mark.parametrize("LockCls", [Lock, RLock, IntRLock])
def test_locks_cant_be_passed_to_other_interpreter(LockCls):
    lock = LockCls()
    interp = ei.Interpreter().start()
    interp.run_string(
        D(
            f"""
            import extrainterpreters; extrainterpreters.DEBUG=True
            lock = pickle.loads({pickle.dumps(lock)})
            assert lock._lock._data == 0
            """
        )
    )
    lock._lock._data[0] = 2
    interp.run_string(
        D(
            """
            assert lock._lock._data[0] == 2
            lock._lock._data[0] = 5
            """
        )
    )
    assert lock._lock._data[0] == 5
    interp.close()


#@pytest.mark.parametrize("LockCls", [Lock, RLock, IntRLock])
#def test_locks_cant_be_acquired_in_other_interpreter(LockCls):
    #lock = LockCls()
    #interp = ei.Interpreter().start()
    #board.new_item((1, 2))
    #interp.run_string(
        #D(
            #f"""
        #import extrainterpreters; extrainterpreters.DEBUG=True
        #lock = pickle.loads({pickle.dumps(lock)})

        #index, item = board.fetch_item()
        #assert item == (1,2)
        #board.new_item((3,4))
    #"""
        #)
    #)
