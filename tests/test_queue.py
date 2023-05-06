import pickle
import threading
from textwrap import dedent as D


import extrainterpreters as ei
from extrainterpreters import Pipe, SingleQueue, Queue
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


def test_queue_sent_to_other_interpreter():
    q = Queue()
    q_pickle = pickle.dumps(q)
    q.put((1, 2))
    with ei.Interpreter() as interp:

        interp.run_string(D(f"""\
            import pickle

            def func(queue):
                values = queue.get()
                return sum(values)

            queue = pickle.loads({q_pickle})
            queue.put(func(queue))
        """))
        assert queue.get() == 3

def test_queue_each_value_is_read_in_a_single_interpreter():

    q = Queue()
    q_pickle = pickle.dumps(q)
    q.put((1, 2))
    q.put((3, 4))
    with ei.Interpreter() as interp1, ei.Interpreter() as interp2:

        code = D(f"""\
            import pickle, time
            from extrainterpreters import get_current
            from queue import Empty

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
        for interp in (interp1, interp2)]
        [t.start() for t in threads]

    v1, id1 = queue.get()
    v2, id2 = queue.get()
    assert v1 == 3 and id1 == interp1.id
    assert v2 == 7 and id2 == interp2.id
    [t.join() for t in threads]

