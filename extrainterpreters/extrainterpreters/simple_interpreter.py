import sys
import pickle
from pathlib import Path
from textwrap import dedent as D

from . import BFSZ, interpreters
from .memoryboard import ProcessBuffer
from .base_interpreter import BaseInterpreter

class SimpleInterpreter(BaseInterpreter):
    """High level Interpreter object

    Simply instantiate this, and use as a context manager
    (or call `.start()` to make calls that will execute in the
    subinterpreter.


    If the `run_in_thread` call is used, a new thread is created
    and the sub-interpreter will execute code in that thread.

    Pickle is used to translate functions to the subinterpreter - so
    only pickle-able callables can be used.

    This implementation uses a memory area  (by default of 10MB), to send pickled objects back and fort at fixed offsets.
    """


    def _create_channel(self):
        self.buffer = ProcessBuffer(BFSZ)
        self.map = self.buffer.map
        super()._create_channel()

    def _close_channel(self):
        with self.lock:
            self.buffer.close()
            self.map = None
        super()._close_channel()

    def _interp_init_code(self):
        code = super()._interp_init_code()
        code += D(f"""\
            import pickle
            import sys
            sys.path[:] = {sys.path}
            from extrainterpreters import _memoryboard
            from extrainterpreters import memoryboard

            BFSZ = {self.buffer.size}
            RET_OFFSET = {self.buffer.nranges["return_data"]}
            _m = memoryboard.FileLikeArray(_memoryboard.remote_memory(*{self.buffer._data_for_remote()}))

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

            def _exit():
                global _m
                _m.close()
                del _m

        """)
        return code


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

        revert_main_name = False
        if getattr(func, "__module__", None) == "__main__":
            if (mod_name:=getattr(mod_name:=sys.modules["__main__"], "__file__", None)):
                revert_main_name = True
                mod = __import__(Path(mod_name).stem)
                func = getattr(mod, func.__name__)
            else:
                self._prepare_interactive(func)

        self.map[self.buffer.nranges["return_data"]] = 0
        self.exception = None
        kwargs = kwargs or {}
        self.map.seek(self.buffer.nranges["send_data"])
        _failed = False
        for obj in (func, args, kwargs):
            try:
                pickle.dump(obj, self.map)
            except ValueError:
                _failed = True
            if _failed or self.map.tell() >= self.buffer.range_sizes["send_data"]:
                raise RuntimeError(D(f"""\
                    Payload to subinterpreter larger than payload buffer.
                    Call cancelled. If needed, just make buffer larger by tweaking
                    extrainterpreters' {BFSZ=} value.
                    """))

        if revert_main_name:
            mod.__name__ = "__main__"

        code = f"""_call({self.buffer.nranges["send_data"]})"""
        try:
            interpreters.run_string(self.intno, code)
        except interpreters.RunFailedError as error:
            # self.map[RET_OFFSET] = True
            self.exception = error
            print(f"Failing code\n {func}(*{args}, **{kwargs})\n, passed as {code}", file=sys.stderr)
            raise

    def done(self):
        if self.exception:
            return True
        return self.map[self.buffer.nranges["return_data"]] != 0


