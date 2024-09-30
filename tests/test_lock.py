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
def test_locks_can_be_passed_to_other_interpreter(LockCls, lowlevel):
    lock = LockCls()
    lock_data = ei.utils._remote_memory(lock._lock._lock_address, 1)
    interp = ei.Interpreter().start()
    interp.run_string(
        D(
            f"""
            import extrainterpreters as ei; ei.DEBUG=True
            lock = pickle.loads({pickle.dumps(lock)})
            lock_data = ei.utils._remote_memory(lock._lock._lock_address, 1)
            assert lock_data[0] == 0
            """
        ),
        raise_=True
    )
    lock_data[0] = 2
    interp.run_string(
        D(
            """
            assert lock_data[0] == 2
            lock_data[0] = 5
            """
        ),
        raise_=True
    )
    assert lock_data[0] == 5
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
