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
    def __init__(self):
        self.intno = interpreters.create()
        self.fname = tempfile.mktemp()
        self.file = open(self.fname, "w+b")
        self.file.write(b"\x00" * BFSZ)
        self.fileno = self.file.fileno()
        self.map = mmap.mmap(self.fileno, BFSZ)
        self.thread = None
        self._init_interpreter()

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

    def run(self, func, args=(), kwargs=None):
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

    def run_in_thread(self, func, args=(), kwargs=None):
        self.thread = threading.Thread(target=self.run, args=(func,),  kwargs={"args": args, "kwargs":kwargs})
        self.thread.start()

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

    def __del__(self):
        self.thread = None
        interpreters.destroy(self.intno)
        try:
            self.map.close()
            self.file.close()
        finally:
            os.unlink(self.fname)

    def __repr__(self):
        return f"Sub-Interpreter <n. {self.intno}> "
