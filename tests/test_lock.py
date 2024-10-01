import pickle
import threading
import time
from functools import partial
from textwrap import dedent as D


import pytest

import extrainterpreters as ei


from extrainterpreters import Lock, RLock
from extrainterpreters.lock import IntRLock


@pytest.fixture
def interpreter(lowlevel):
    interp = ei.Interpreter().start()
    interp.run_string(
        D(
            f"""
            import extrainterpreters as ei; ei.DEBUG=True
            """
        ),
        raise_=True
    )
    yield interp
    interp.close()

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



def test_lock_cant_be_reacquired_same_interpreter():
    lock = Lock()

    assert lock.acquire()

    assert not lock.acquire(blocking=False)


def test_lock_cant_be_reacquired_other_interpreter(interpreter):
    lock = Lock()
    # some assertion lasagna -
    # just checks basic toggling - no race conditions tested here:
    run = partial(interpreter.run_string, raise_=True)
    run(f"lock = pickle.loads({pickle.dumps(lock)})")
    run (f"assert lock.acquire(blocking=False)")
    assert not lock.acquire(blocking=False)
    run (f"assert not lock.acquire(blocking=False)")
    run (f"lock.release()")
    assert lock.acquire(blocking=False)
    run (f"assert not lock.acquire(blocking=False)")
    lock.release()
    run (f"assert lock.acquire(blocking=False)")
    run (f"lock.release()")



def test_lock_works_across_threads_in_same_interpreter():
    lock = Lock()
    results = []
    def aux1():
        # assert code does't work in nested functions
        results.append((lock.acquire(blocking=False), "aux1 - first lock, should work"))
        time.sleep(0.1)
        lock.release()
    def aux2():
        time.sleep(0.025)
        results.append((not lock.acquire(blocking=False), "aux2 - first lock, should fail"))
        time.sleep(0.085)
        results.append((lock.acquire(blocking=False), "aux2 - second lock, should work"))
        if lock.locked():
            lock.release()
    t1 = threading.Thread(target=aux1)
    t2 = threading.Thread(target=aux2)
    t1.start(); t2.start()

    t1.join(); t2.join()

    for status, message in results:
        assert status, message


@pytest.mark.parametrize("LockCls", [Lock, RLock, IntRLock])
def test_locks_can_be_passed_to_other_interpreter(LockCls, interpreter):
    lock = LockCls()
    lock_data = ei.utils._remote_memory(lock._lock._lock_address, 1)
    interpreter.run_string(
        D(
            f"""
            lock = pickle.loads({pickle.dumps(lock)})
            lock_data = ei.utils._remote_memory(lock._lock._lock_address, 1)
            assert lock_data[0] == 0
            """
        ),
        raise_=True
    )
    lock_data[0] = 2
    interpreter.run_string(
        D(
            """
            assert lock_data[0] == 2
            lock_data[0] = 5
            """
        ),
        raise_=True
    )
    assert lock_data[0] == 5


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
