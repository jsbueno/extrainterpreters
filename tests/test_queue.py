import pickle
import threading
from textwrap import dedent as D


import extrainterpreters as ei
from extrainterpreters import Pipe, SingleQueue, Queue, LockablePipe
from extrainterpreters import Interpreter


import pytest


def test_pipe_is_unpickled_as_counterpart_and_comunicates(lowlevel):
    interp = Interpreter().start()
    interp.run_string("import pickle")
    aa = Pipe()
    interp.run_string("import extrainterpreters; extrainterpreters.DEBUG = True")
    interp.run_string(f"bb = pickle.loads({pickle.dumps(aa)})")
    aa.send(b"01234")
    interp.run_string("cc = bytes(i + 1 for i in bb.read())")
    interp.run_string("bb.send(cc)")
    assert aa.read() == b"12345"
    interp.close()

def test_locacklepipe_locks(lowlevel):
    interp = Interpreter().start()
    interp.run_string("import pickle")
    aa = LockablePipe()
    interp.run_string("import extrainterpreters; extrainterpreters.DEBUG = True")
    interp.run_string(f"bb = pickle.loads({pickle.dumps(aa)})")
    interp.run_string("bb.lock.__enter__()")
    with pytest.raises(ei.ResourceBusyError):
        aa.lock.timeout(None).__enter__()
    interp.run_string("bb.lock.__exit__()")
    assert aa.lock._timeout == aa.lock._original_timeout
    aa.lock.__enter__()



def test_singlequeue_is_unpickled_as_counterpart_and_comunicates(lowlevel):
    interp = Interpreter().start()
    interp.run_string("import pickle")
    aa = SingleQueue()
    # Creates queue end in the sub-interpreter:
    interp.run_string("import extrainterpreters; extrainterpreters.DEBUG = True")
    interp.run_string(f"bb = pickle.loads({pickle.dumps(aa)})")
    obj = {"1": "1234", None: ...}
    aa.put(obj)
    interp.run_string("cc = bb.get()")
    interp.run_string(f"assert cc == {obj}")
    interp.close()

def test_queue_send_object():
    q = Queue()
    q.put((1,2))
    assert q.get((1, 2))


def test_queue_can_build_private_pipe_once_active_on_subinterpreter(lowlevel):

    interp = ei.Interpreter().start()
    interp.run_string("import extrainterpreters; extrainterpreters.DEBUG=True")

    q = ei.Queue()
    qp = pickle.dumps(q)
    assert not q.endpoints
    interp.run_string(f"q = pickle.loads({qp})")

    q._dispatch_return_opcode()
    assert q.endpoints[0] == interp.intno
    assert isinstance(q._child_pipes[interp.intno], Pipe)


def test_queue_sent_to_other_interpreter():
    queue = Queue()
    q_pickle = pickle.dumps(queue)
    #q.put((1, 2))
    with ei.Interpreter() as interp:

        interp.run_string(D(f"""\
            import extrainterpreters
            extrainterpreters.DEBUG=True
            import pickle
            queue = pickle.loads({q_pickle})
        """))
        breakpoint()
        queue.put((1, 2))
        interp.run_string(D(f"""\
            values = queue.get()
            data = sum(values)
        """))
        interp.run_string("queue.put(values)")
        assert queue.get() == 3

@pytest.mark.skip
def test_queue_each_value_is_read_in_a_single_interpreter():

    queue = q = Queue()
    q_pickle = pickle.dumps(q)
    q.put((1, 2))
    q.put((3, 4))
    with ei.Interpreter() as interp1, ei.Interpreter() as interp2:

        code = D(f"""\
            import pickle, time
            from extrainterpreters import get_current
            from queue import Empty

            import extrainterpreters
            extrainterpreters.DEBUG=True

            def func(queue):
                values = (0,)
                while True:
                    try:
                        values = queue.get(block=False)
                        time.sleep(0.03)  # block this interpreter
                    except Empty:
                        return sum(values)

            queue = pickle.loads({q_pickle})
            queue.put((func(queue), get_current()))
        """)
        def run(interp):
            interp.run_string(code)
        threads = [threading.Thread(target=run, args=(interp,))
        for interp in (interp1,interp2)]
        [t.start() for t in threads]

    v1, id1 = queue.get()
    v2, id2 = queue.get()
    assert v1 == 3 and id1 == interp1.id
    assert v2 == 7 and id2 == interp2.id
    [t.join() for t in threads]

