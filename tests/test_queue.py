import pickle

from extrainterpreters import Pipe, SingleQueue
from extrainterpreters import Interpreter

import pytest


def test_pipe_is_unpickled_as_counterpart_and_comunicates():
    interp = Interpreter().start()
    interp.run_string("import pickle")
    aa = Pipe()
    interp.run_string(f"bb = pickle.loads({pickle.dumps(aa)})")
    aa.send(b"01234")
    interp.run_string("cc = bytes(i + 1 for i in bb.read())")
    interp.run_string("bb.send(cc)")
    assert aa.read() == b"12345"
    interp.close()


def test_singlequeue_is_unpickled_as_counterpart_and_comunicates():
    interp = Interpreter().start()
    interp.run_string("import pickle")
    aa = SingleQueue()
    # Creates queue end in the sub-interpreter:
    interp.run_string(f"bb = pickle.loads({pickle.dumps(aa)})")
    obj = {"1": "1234", None: ...}
    aa.put(obj)
    interp.run_string("cc = bb.get()")
    interp.run_string(f"assert cc == {obj}")
    interp.close()
