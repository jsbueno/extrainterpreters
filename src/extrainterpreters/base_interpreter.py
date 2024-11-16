import inspect
import time
import threading
import sys
import pickle
from types import ModuleType, FunctionType
from textwrap import dedent as D


from . import interpreters, BFSZ, running_interpreters


_TTL = sys.getswitchinterval() * 10

class BaseInterpreter:
    def __init__(self):
        # .intno and .id are both set to the interpreter id,
        # but .intno is set to None when the interpreter is closed.
        self.intno = self.id = None
        self.lock = threading.RLock()

    def start(self):
        with self.lock:
            if self.intno is not None:
                raise RuntimeError("Interpreter already started")
            self.intno = self.id = interpreters.create()
            running_interpreters[self.intno] = self
            self.thread = None
            self._create_channel()
            self._init_interpreter()
        self._started_at = time.monotonic()
        return self

    def __enter__(self):
        return self.start()

    def _create_channel(self):
        pass

    def close(self, *args):
        with self.lock:
            self._close_channel()
            try:
                while time.monotonic() - self._started_at < 10 * _TTL:
                    # subinterpreters need sometime to stabilize.
                    # shutting then imediatelly may lead to a segfault.
                    time.sleep(0.002)
                if interpreters.is_running(self.intno):
                    # TBD: close on "at exit"
                    # # but really, just enduser code running with "run_string΅ on other thread should
                    # leave the sub-interpreter on this state.
                    return
                interpreters.destroy(self.intno)
            except RuntimeError:
                raise  ## raised if interpreter is running. TBD: add a timeout mechanism
            self.intno = None
        self.thread = None
        running_interpreters.pop(self.id, None)

    def __exit__(self, *args):
        return self.close(*args)

    def _close_channel(self):
        pass

    def _init_interpreter(self):
        code = self._interp_init_code()
        interpreters.run_string(self.intno, code)

    def _interp_init_code(self):
        return ""

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
        self.thread = threading.Thread(
            target=self.execute, args=(func,), kwargs={"args": args, "kwargs": kwargs}
        )
        self.thread.start()
        return True

    def _source_handle(self, func):
        if (
            func.__name__ in self._source_handled
            and hash(func) == self._source_handled[func.__name__]
        ):
            return
        source = inspect.getsource(func)
        self.run_string(source)  # "types" function in remote interpreter __main__
        self._source_handled[func.__name__] = hash(func)

    def _handle_module(self, mod_name):
        if mod_name and not mod_name in self._source_handled:
            try:
                self.run_string(f"import {mod_name}")
                self._source_handled[mod_name] = "module"
            except interpreters.RunFailedError:
                # certain modules won't load in subinterpreters.
                # if the called function would need them, it would
                # not work all the same.
                pass

    def _prepare_interactive(self, func):
        # rebuilds part of the environment of the interactive interpreter
        # in the sub-interpreter - including a interactively typed function
        # there are limits for what can reasonably be done here:
        # we will inspect the globals in the interactive shell
        # and import all root-level modules in the sub-interpreter
        # but not sub-modules. e.g.: if the user imported "math"
        # we will import that into the sub interpreter.
        # if the user imported "xml.etree", just
        # "xml" will be imported, and if the user tries
        # to use "xml.etree" inside the interactive
        # function, that will fail.

        assert func.__module__ == "__main__"

        if not hasattr(self, "_source_handled"):
            self._source_handled = {}

        self._source_handle(func)

        main_module = sys.modules["__main__"]
        main_globals = main_module.__dict__

        for name, obj in list(main_globals.items()):
            if isinstance(obj, FunctionType):
                if (mod_name := getattr(obj, "__module__", None)) == "__main__":
                    self._source_handle(obj)
                else:
                    self._handle_module(mod_name)
                    # Poor man's "from x import y"
                    if func.__name__ not in self._source_handled:
                        self.run_string(
                            f"{name} = getattr({mod_name}, '{obj.__name__}')"
                        )
            elif isinstance(obj, ModuleType):
                mod_name = obj.__name__
                self._handle_module(mod_name)
                if mod_name != name and name not in self._source_handled:
                    self.run_string(f"{name} = {mod_name}")
            # Test: is it worth setting literal objects that might be
            # used as global variables?

    def execute(self, func, args=(), kwargs=None):
        # to sub-interpreter.
        if self.intno is None:
            raise RuntimeError(
                D(
                    """\
                Sub-interpreter not initialized. Call ".start()" or enter context to make calls.
                """
                )
            )

    def run_string(self, code, raise_=False):
        """Execs a string of code in associated interpreter

        Mostly to mirror interpreters.run_string as a convenient method.
        """
        result = interpreters.run_string(self.intno, code)
        if result and raise_:
            # In Python 3.13+ indicates an exception occured.
            # (in Python 3.12, an exception is raised immediatelly)
            raise RuntimeError(result)
        return result

    #  currently not working. will raise when the interpreter is destroyed:
    # def is_running(self):
    # """Proxies interpreters.is_running

    # Can be used instead of "done" to check if work
    # in a threaded call has ended.
    # """
    # with self.lock:
    # return interpreters.is_running(self.intno)

    def result(self):
        raise NotImplementedError()

    def __repr__(self):
        return f"Sub-Interpreter <#{self.intno}>"

    def __del__(self):
        # thou shall not leak
        # (At a subinterpreter + 10MB tempfile, that is expensive!)
        if getattr(self, "intno", None):
            self.close()

    def done(self):
        raise NotImplementedError()
