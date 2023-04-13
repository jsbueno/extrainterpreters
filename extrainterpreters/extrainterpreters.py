import threading
import os, sys
import pickle
import mmap
import tempfile

from textwrap import dedent as D

try:
    import interpreters
except ImportError:
    try:
        import _xxsubinterpreters as interpreters
    except ImportError:
        raise ImportError(D("""
            interpreters module not available in this Python install.
            If you are early to it (before 3.12 beta), you need to build the
            per-interpreter-gil-new branch from
            https://github.com/ericsnowcurrently/cpython.git
            """))


BFSZ = 10_000_000
RET_OFFSET = 8_000_000

class Interpreter:
    """High level Interpreter object

    Simply instantiate this, and use as a context manager
    (or call `.start()` to make calls that will execute in the
    subinterpreter.

    If the `run_in_thread` call is used, a new thread is created
    and the sub-interpreter will execute code in that thread.

    Pickle is used to translate functions to the subinterpreter - so
    only pick-able callables can be used.
    """

    # TBD: add async interface

    def __init__(self):
        self.intno = None
        self.lock = threading.Lock()

    def start(self):
        with self.lock:
            self.intno = interpreters.create()
            self.fname = tempfile.mktemp()
            self.file = open(self.fname, "w+b")
            self.file.write(b"\x00" * BFSZ)
            self.fileno = self.file.fileno()
            self.map = mmap.mmap(self.fileno, BFSZ)
            self.thread = None
            self._init_interpreter()
            return self

    __enter__ = start

    def close(self, *args):
        with self.lock:
            interpreters.destroy(self.intno)
            try:
                self.map.close()
                self.file.close()
            finally:
                os.unlink(self.fname)
            self.map = None
            self.intno = False
        self.thread = None
        # TBD: check if there was a task running in a thread,
        # if so, and it is done, retrieve the result for further
        # use.

    __exit__ = close

    def _init_interpreter(self):
        code = D(f"""\
            import mmap
            import pickle
            import sys
            sys.path[:] = {sys.path}

            BFSZ = {BFSZ}
            RET_OFFSET = {RET_OFFSET}
            _m = mmap.mmap({self.fileno}, BFSZ)

            def _thaw(ind_data):
                _m.seek(ind_data)
                func = pickle.load(_m)
                args = pickle.load(_m)
                kw = pickle.load(_m)
                return func, args, kw

            def _call(ind_data):
                func, args, kw = _thaw(ind_data)
                result = func(*args, **kw)
                _m.seek(RET_OFFSET)
                pickle.dump(result, _m)

        """)
        interpreters.run_string(self.intno, code)

    def run(self, func, *args, **kwargs):
        """Call to execute a function in the sub-interpreter synchronously"""
        self.execute(func, args=args, kwargs=kwargs)
        # Answer should be synchronous
        return self.result()

    def run_in_thread(self, func, *args, **kwargs):
        """Call to execute a function in a different thread.

        Call `.done()` to check the execution is complete
        and `.result()` to retrieve the return value.
        """
        self.thread = threading.Thread(target=self.execute, args=(func,),  kwargs={"args": args, "kwargs":kwargs})
        self.thread.start()
        return True

    def execute(self, func, args=(), kwargs=None):
        """Lower level function to actual dispatch the call
        to the subinterpreter in the current running thread.

        Prefer to use `.run` to run in the same thread
        or `.run_in_thread` for now.
        """

        # TBD: allow multi-threaded parallel calls (in parent intertpreter)
        # to sub-interpreter.

        if self.intno is None:
            raise RuntimeError(D("""\
                Sub-interpreter not initialized. Call ".start()" or enter context to make calls.
            """))
        self.map[RET_OFFSET] == 0
        kwargs = kwargs or {}
        self.map.seek(0)
        pickle.dump(func, self.map)
        pickle.dump(args, self.map)
        pickle.dump(kwargs, self.map)
        code = "_call(0)"
        try:
            interpreters.run_string(self.intno, code)
        except interpreters.RunFailedError as error:
            self.map[RET_OFFSET] = True
            self.exception = error
            raise

    def done(self):
        # TBD: check interpreters.is_running?
        return self.map[RET_OFFSET] == 0

    def result(self):
        if not self.done:
            raise InvalidState("Task not completed in subinterpreter")
        self.map.seek(RET_OFFSET)
        result = pickle.load(self.map)
        if self.thread:
            self.thread.join()
            self.thread = None
        return result

    def __repr__(self):
        return f"Sub-Interpreter <#. {self.intno}>"
