import pickle
import threading
import time
import os
from textwrap import dedent as D


import extrainterpreters as ei
from extrainterpreters import SingleQueue, Queue
from extrainterpreters import get_current
from extrainterpreters import Interpreter
from extrainterpreters.queue import Empty, _SimplexPipe, _DuplexPipe
from extrainterpreters import resources

import pytest


def test_simplexpipe_works():
    xx = _SimplexPipe()
    yy = pickle.loads(pickle.dumps(xx))
    xx.write(b"\x01\x02")
    assert yy.read(2) == (b"\x01\x02")
    yy.write(b"\x03\x04")
    assert xx.read(2) == (b"\x03\x04")


def test_simplexpipe_unpickles_with_same_memory_buffer_main_intrepreter():
    xx = _SimplexPipe()
    yy = pickle.loads(pickle.dumps(xx))
    assert xx is yy
    del xx, yy

    xx = _SimplexPipe()
    # Force unregister:
    del resources.PIPE_REGISTRY[xx.reader_fd, xx.writer_fd]
    yy = pickle.loads(pickle.dumps(xx))
    assert xx is not yy


def test_simplexpipe_unpickles_with_same_memory_buffer_child_intrepreter():
    xx = _SimplexPipe()
    with ei.Interpreter() as interp:
        interp.run_string("import extrainterpreters as ei; ei.DEBUG=True")
        interp.run_string(f"xx = pickle.loads({pickle.dumps(xx)})")
        interp.run_string(
            D(
                f"""\
            yy = pickle.loads(pickle.dumps(xx))
            assert xx is yy
        """
            )
        )


def test_simplexpipe_file_registers_unregister_from_global_selector():
    xx = _SimplexPipe()
    fds = xx.reader_fd, xx.writer_fd
    assert fds in resources.PIPE_REGISTRY
    for fd in fds:
        assert resources.EISelector.selector.get_key(fd)
    xx.close()
    assert fds not in resources.PIPE_REGISTRY
    for fd in fds:
        with pytest.raises(KeyError):
            resources.EISelector.selector.get_key(fd)


def test_simplexpipe_closes_fds_on_exit():
    xx = _SimplexPipe()
    fds = xx.reader_fd, xx.writer_fd
    xx.__del__()  # `del xx` doesn't cut it. I hope it is due to references kept by pytest.
    for fd, method, arg in zip(fds, (os.read, os.write), (0, b"\x00")):
        with pytest.raises(OSError):
            try:
                method(fd, arg)
            except OSError as oserror:
                assert (
                    oserror.args[0] == 9
                )  # "Bad file descriptor" indicating the file  is closed
                raise


def test_simplexpipe_doesnotclose_when_open_in_other_interpreter():
    TOTAL = 5
    aa = _SimplexPipe()
    interps = []
    code = D(
        f"""\
        import extrainterpreters; extrainterpreters.DEBUG = True
        aa = pickle.loads({pickle.dumps(aa)})
    """
    )
    for i in range(TOTAL):
        interp = Interpreter().start()
        interp.run_string(code)
        interps.append(interp)
    fd = aa.writer_fd
    assert os.write(fd, b"") == 0
    assert aa.active.counter == TOTAL + 1

    for interp in interps[:-1]:
        interp.run_string("aa.close()")
    assert os.write(fd, b"") == 0
    assert aa.active.counter == 2
    aa.close()
    assert os.write(fd, b"") == 0
    assert aa.active.counter == 1
    interps[-1].run_string("aa.close()")
    assert aa.active.counter == 0
    with pytest.raises(OSError):
        os.write(fd, b"")


def test_duplexpipe_is_unpickled_as_counterpart_and_comunicates(lowlevel):
    interp = Interpreter().start()
    interp.run_string("import pickle")
    aa = _DuplexPipe()
    interp.run_string("import extrainterpreters; extrainterpreters.DEBUG = True")
    interp.run_string(f"bb = pickle.loads({pickle.dumps(aa)})")
    aa.send(b"01234")
    interp.run_string("cc = bytes(i + 1 for i in bb.read())")
    interp.run_string("bb.send(cc)")
    assert aa.read() == b"12345"
    interp.close()


def test_locackle_duplexpipe_locks(lowlevel):
    interp = Interpreter().start()
    interp.run_string("import pickle")
    aa = _DuplexPipe()
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
    q.put((1, 2))
    assert q.get((1, 2))


# gone are the private pipes inside queues.
# def test_queue_can_build_private_pipe_once_active_on_subinterpreter(lowlevel):

# interp = ei.Interpreter().start()
# interp.run_string("import extrainterpreters; extrainterpreters.DEBUG=True")

# q = ei.Queue()
# qp = pickle.dumps(q)
# assert not q.endpoints
# interp.run_string(f"q = pickle.loads({qp})")

# q._dispatch_return_opcode()
# assert q.endpoints[0] == interp.intno
# assert isinstance(q._child_pipes[interp.intno], Pipe)


def test_queue_sent_to_other_interpreter():
    queue = Queue()
    q_pickle = pickle.dumps(queue)
    # q.put((1, 2))
    with ei.Interpreter() as interp:
        interp.run_string(
            D(
                f"""\
            import extrainterpreters
            extrainterpreters.DEBUG=True
            import pickle
            queue = pickle.loads({q_pickle})
        """
            )
        )
        queue.put((1, 2))
        interp.run_string(
            D(
                f"""\
            values = queue.get()
            data = sum(values)
        """
            )
        )
        interp.run_string("queue.put(data)")
        assert queue.get() == 3


@pytest.mark.skip
def test_queue_each_value_is_read_in_a_single_interpreter():
    # FIXME: this is failing IRL : this test is not deterministic (neither are queue values read in a single interpreter by now)
    queue = q = Queue()
    q_pickle = pickle.dumps(q)
    q.put((1, 2))
    q.put((3, 4))
    with ei.Interpreter() as interp1, ei.Interpreter() as interp2:
        code = D(
            f"""\
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
                        time.sleep(0.05)  # block this interpreter
                    except Empty:
                        return sum(values)

            queue = pickle.loads({q_pickle})
            queue.put((func(queue), get_current()))
        """
        )

        def run(interp):
            interp.run_string(code)

        threads = [
            threading.Thread(target=run, args=(interp,))
            for interp in (interp1, interp2)
        ]

        # GETs taking place _before_ subinterpreter shutdown
        threads[0].start()
        time.sleep(0.025)
        threads[1].start()
        time.sleep(0.05)
        v1, id1 = queue.get()
        time.sleep(0.05)
        v2, id2 = queue.get()
        time.sleep(0.05)

    assert v1 == 3 and id1 == interp1.id
    assert v2 == 7 and id2 == interp2.id
    [t.join() for t in threads]


def test_queue_get_value_from_subinterpreter():
    queue = q = Queue()
    q_pickle = pickle.dumps(q)

    code = D(
        """\
        import extrainterpreters; extrainterpreters.DEBUG=1
        from extrainterpreters import get_current
        queue = pickle.loads({q_pickle})
        queue.put({value})
    """
    )

    with ei.Interpreter() as interp1:
        interp1.run_string(code.format(q_pickle=q_pickle, value=(1, 2)))
        assert queue.get() == (1, 2)
    pass


def test_queue_trying_to_get_value_from_closed_interpreter_doesnot_break_queue():
    queue = q = Queue()
    q_pickle = pickle.dumps(q)
    queue._signal_pipe._ttl = 3600
    value1 = (1, 2)
    value2 = (1, 2)
    code = D(
        f"""\
        import extrainterpreters; extrainterpreters.DEBUG=1
        from extrainterpreters import get_current
        queue = pickle.loads({q_pickle})
        queue.put({value1})
        queue.put({value2})
    """
    )

    with ei.Interpreter() as interp1:
        interp1.run_string(code)
        assert queue.get() == (1, 2)
    with pytest.raises(Empty):
        queue.get(block=False)

    with ei.Interpreter() as interp2:
        interp2.run_string(code)
        assert queue.get() == (1, 2)
        assert queue.get() == (1, 2)
        with pytest.raises(Empty):
            queue.get(block=False)
    with pytest.raises(Empty):
        queue.get(block=False)


def test_queue_subinterpreters_can_exchange_data():
    queue = q = Queue()
    q_pickle = pickle.dumps(q)

    code = D(
        """\
        import extrainterpreters; extrainterpreters.DEBUG=1
        from extrainterpreters import get_current
        queue = pickle.loads({q_pickle})
    """
    )

    with ei.Interpreter() as interp1, ei.Interpreter() as interp2:
        for interp in (interp1, interp2):
            interp.run_string(code.format(q_pickle=q_pickle))
        interp1.run_string("queue.put((1,2))")
        interp2.run_string("assert queue.get() == (1,2)")
        interp2.run_string("queue.put((3,4))")
        interp1.run_string("assert queue.get() == (3,4)")
        interp1.run_string("queue.put((5,6))")
        interp1.run_string("assert queue.get() == (5,6)")


def test_queue_get_is_blocking_by_default_same_interpreter():
    delay = 0.05
    q = Queue()

    def source():
        time.sleep(delay)
        q.put("element")

    def sink():
        nonlocal failed
        start_time = time.monotonic()
        try:
            assert q.get() == "element"
            assert (time.monotonic() - start_time) > delay * 0.95
        except Exception as error:
            failed = error

    failed = False
    t1 = threading.Thread(target=source)
    t2 = threading.Thread(target=sink)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    assert not failed, failed


def test_queue_get_is_blocking_by_default_other_interpreter():
    delay = 0.05
    q_send = Queue()
    q_response = Queue()
    q_send_pickle = pickle.dumps(q_send)
    q_response_pickle = pickle.dumps(q_response)
    failed = False
    interp1 = ei.Interpreter()
    interp2 = ei.Interpreter()
    with interp1, interp2:
        for interp in (interp1, interp2):
            interp.run_string(
                D(
                    f"""\
                import extrainterpreters; extrainterpreters.DEBUG=1
                import time, threading, pickle
                q_send = pickle.loads({q_send_pickle})
                q_response = pickle.loads({q_response_pickle})
                """
                )
            )
        interp1.run_string(
            D(
                f"""\
            def source():
                time.sleep({delay})
                q_send.put("element")
            t = threading.Thread(target=source)
            t.start()
            """
            )
        )
        interp2.run_string(
            D(
                f"""\
            def sink():
                failed = "" # str with False value for assertion
                start_time = time.monotonic()
                try:
                    assert q_send.get() == "element"
                    assert (time.monotonic() - start_time) > {delay} * 0.95
                except Exception as error:
                    failed = error
                q_response.put(str(failed)) # the error name is enough
            t = threading.Thread(target=sink)
            t.start()
            """
            )
        )
        failed = q_response.get()
        for interp in (interp1, interp2):
            interp.run_string(
                D(
                    f"""\
                t.join()
                """
                )
            )
    assert not failed, failed

#  FIXME: this is really falling due to malfunctioning code
@pytest.mark.skip
def test_queue_each_value_is_read_in_a_single_interpreter_several_interpreters():
    # FIXME: this is failing IRL : this test is not deterministic (neither are queue values read in a single interpreter by now)
    n_interpreters = 5
    qsource = q = Queue()
    qreturn = Queue()
    q_pickle = pickle.dumps(q)
    q_return_pickle = pickle.dumps(qreturn)
    interps = []
    threads = []
    code = D(
        f"""\
        import pickle, time
        from extrainterpreters import get_current
        from queue import Empty

        import extrainterpreters
        extrainterpreters.DEBUG=True

        def func(queue, ret_queue):
            while True:
                try:
                    value = queue.get(timeout=1)
                except TimeoutError:
                    break
                time.sleep(0.5)
                ret_queue.put((value, get_current()))


        queue = pickle.loads({q_pickle})
        ret_queue = pickle.loads({q_return_pickle})
        func(queue, ret_queue)
    """
    )
    for i in range(n_interpreters):
        interp = ei.Interpreter().start()
        interps.append(interp)

        def run(interp):
            interp.run_string(code)

        threads.append(
            thread:=threading.Thread(target=run, args=(interp,))
        )
        thread.start()

    for i in range(n_interpreters):
        q.put(i)

    time.sleep(1)
    returned_values = list()
    returned_interps = list()
    while True:
        try:
            v, interp = qreturn.get(timeout=0.02)
            returned_values.append(v)
            returned_interps.append(interp)
        except TimeoutError:
            break

    print ("values: ", sorted(returned_values))
    print("interps: ", sorted(returned_interps))
    assert len(set(returned_interps)) == n_interpreters
    assert len(set(returned_values)) == n_interpreters

    [t.join() for t in threads]
    [interp.close() for interp in interps]
