import sys
import pickle
import time
from pathlib import Path
from textwrap import dedent as D

from . import BFSZ, interpreters
from .memoryboard import ProcessBuffer
from .base_interpreter import BaseInterpreter


class _BufferedInterpreter(BaseInterpreter):
    """Internal class holding methods to be used
    as building blocks for the final classes
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
        code += D(
            f"""\
            import pickle
            import sys
            sys.path[:] = {sys.path}
            from extrainterpreters import _memoryboard
            from extrainterpreters import memoryboard

            BFSZ = {self.buffer.size}
            RET_OFFSET = {self.buffer.nranges["return_data"]}
            _m = pickle.loads({pickle.dumps(self.buffer.map)})
            _m.__enter__()

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

        """
        )
        return code

    def done(self):
        if self.exception or hasattr(self, "_cached_result"):
            return True
        if not self.map:
            return False
        return self.map[self.buffer.nranges["return_data"]] != 0

    def result(self):
        if not self.done():
            raise ValueError("Task not completed in subinterpreter")
        if self.exception:
            raise ValueError(
                "An exception ocurred in the subinterpreter. Check the `.exception` attribute"
            )
        if hasattr(self, "_cached_result"):
            return self._cached_result
        self.map.seek(self.buffer.nranges["return_data"])
        result = pickle.load(self.map)
        if self.thread:
            self.thread.join()
            self.thread = None
        self._cached_result = result
        return result


class SimpleInterpreter(_BufferedInterpreter):
    """High level Interpreter object

    Simply instantiate this, and use as a context manager
    (or call `.start()`) to make calls that will execute in the
    subinterpreter.

    If the `run_in_thread` call is used, a new thread is created
    and the sub-interpreter will execute code in that thread.

    Also, the semantics of "threading.Thread" is given for convenience:
    if the class is instantiated with a "target" function (along with
    optional args and kwargs), upon calling "start()", the new
    interpreter will imediatelly run the target function.
    Checking when its done, and the return value can be checked
    by calling `join()` or  `.done()` and `.result()` methods.

    Subclassing and overriding `.run`, as in threading.Thread
    is __not__ supported, though: unlike an instance of a (subclass of)
    Thread, the instance of Interpreter does not exist in the sub-interpreter, and it can't be simply pickled and unpickled there.
    Although executing the `run()` method as a simple function,
    which could be "transplanted into a dummy class" is feasible,
    projects that prefer overriding "run" over using a "target="
    argument may try to enrich the subclass itself
    with other methods and capabilities - that would be
    too complicated to send to the sub-interpreter.

    Pickle is used to translate functions to the subinterpreter - so
    only pickle-able callables can be used.

    This implementation uses a memory area  (by default of 10MB), to send pickled objects back and forth at fixed offsets.
    """

    def __init__(self, target=None, args=(), kwargs=None):
        kwargs = kwargs or {}
        super().__init__()
        self.target = target
        self._args = args
        self._kwargs = kwargs
        self.exception = None

    def start(self):
        super().start()
        if self.target:
            self.run_in_thread(self.target, *self._args, **self._kwargs)
        return self

    def join(self):
        """Analog to Thread.join()

        Waits until an off-thread task is completed
        in the sub-interpreter and closes the interpreter.

        Task may have been started either with a "target" argument
        when instantiating this interpreter, or with a
        call to "run_in_thread".

        After joining a call to ".result()"can retrieve
        the return value.
        """
        while not self.done():
            time.sleep(0.005)
        self.close()

    def execute(self, func, args=(), kwargs=None):
        """Lower level function to actual dispatch the call
        to the subinterpreter in the current running thread.

        Prefer to use `.run` to run in the same thread
        or `.run_in_thread` for now.
        """
        kwargs = kwargs or {}

        super().execute(
            func, args, kwargs
        )  # NOP on the interpreter, but sets internal states.
        self.map[self.buffer.nranges["return_data"]] = 0
        self.exception = None
        try:
            del self._cached_result
        except AttributeError:
            pass

        revert_main_name = False
        if getattr(func, "__module__", None) == "__main__":
            if mod_name := getattr(
                mod_name := sys.modules["__main__"], "__file__", None
            ):
                # changing the module name from "__main__" to its
                # normal importable name makes functions and
                # classes on it able to be unpickled on
                # target interpreters.
                revert_main_name = True
                mod = __import__(Path(mod_name).stem)
                func = getattr(mod, func.__name__)
            else:
                self._prepare_interactive(func)

        self.map.seek(self.buffer.nranges["send_data"])
        _failed = False
        for obj in (func, args, kwargs):
            try:
                pickle.dump(obj, self.map)
            except ValueError:
                _failed = True
            if _failed or self.map.tell() >= self.buffer.range_sizes["send_data"]:
                raise RuntimeError(
                    D(
                        f"""\
                    Payload to subinterpreter larger than payload buffer.
                    Call cancelled. If needed, just make buffer larger by tweaking
                    extrainterpreters' {BFSZ=} value.
                    """
                    )
                )

        if revert_main_name:
            mod.__name__ = "__main__"

        code = f"""_call({self.buffer.nranges["send_data"]})"""
        try:
            error_result = interpreters.run_string(self.intno, code)
        except interpreters.RunFailedError as error:
            # self.map[RET_OFFSET] = True
            self.exception = error
            print(
                f"Failing code\n {func}(*{args}, **{kwargs})\n, passed as {code}",
                file=sys.stderr,
            )
            # implementation still can't reraise the exception on main interpreter
            # error.resolve()
            raise
        if error_result:
            # Python 3.13
            self.exception = error_result
            # TODO: improve this
            raise RuntimeError(error_result)

    def close(self, *args):
        if self.done():
            try:
                # Caches an eventual result so it is available after closing
                self.result()
            except Exception as err:
                pass
        return super().close(*args)
