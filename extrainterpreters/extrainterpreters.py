import atexit
import inspect
import threading
import os, sys
import pickle
import mmap
import selectors
import tempfile
import weakref
from pathlib import Path
from types import ModuleType, FunctionType
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
PAYLOAD_BUFFER = RET_OFFSET
RETURN_BUFFER = BFSZ - RET_OFFSET

running_interpreters = weakref.WeakSet()


class Pipe:
    """Full Duplex Pipe class.


    """
    def __init__(self):
        self.originator_fds = os.pipe()
        self.counterpart_fds = os.pipe()
        self._all_fds = self.originator_fds + self.counterpart_fds

    def _post_init(self):
        self._selector = selectors.DefaultSelector()
        self._selector.register(self.counterpart_fds[0], selectors.EVENT_READ, self.read_blocking)

    @property
    def counterpart(self):
        new_inst = type(self).__new__(type(self))
        new_inst.originator_fds = self.counterpart_fds
        new_inst.counterpart_fds = self.originator_fds
        new_inst._all_fds = ()
        new_inst._post_init()
        return new_inst

    def send(self, data):
        if isinstance(data, str):
            data = data.encode("utf-8")
        os.write(self.originator_fds[1], data)

    def read_blocking(self, amount=4096):
        return os.read(self.counterpart_fds[0], amount)

    def read(self, timeout=0):
        result = b""
        for key, mask in self._selector.select(timeout=timeout):
            v = key.data()
            if key.fd == self.counterpart_fds[0]:
                result = v
        return result

    def __del__(self):
        for fd in self._all_fds:
            try:
                os.close(fd)
            except OSError:
                pass

class BaseInterpreter:
    """High level Interpreter object

    Simply instantiate this, and use as a context manager
    (or call `.start()` to make calls that will execute in the
    subinterpreter.

    If the `run_in_thread` call is used, a new thread is created
    and the sub-interpreter will execute code in that thread.

    Pickle is used to translate functions to the subinterpreter - so
    only pickle-able callables can be used.
    """

    # TBD: add async interface

    def __init__(self):
        self.intno = None
        self.lock = threading.RLock()

    def start(self):
        if self.intno is not None:
            raise RuntimeError("Interpreter already started")
        with self.lock:
            self.intno = interpreters.create()
            running_interpreters.add(self)
            self.thread = None
            self._create_channel()
            self._init_interpreter()
        return self

    __enter__ = start

    def _create_channel(self):
        pass

    def close(self, *args):
        with self.lock:
            try:
                interpreters.destroy(self.intno)
            except RuntimeError:
                raise  ## raised if interpreter is running. TBD: add a timeout mechanism
            self.intno = False
            self._close_channel()
        self.thread = None
        try:
            running_interpreters.remove(self)
        except keyError:
            pass

    __exit__ = close

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
        self.thread = threading.Thread(target=self.execute, args=(func,),  kwargs={"args": args, "kwargs":kwargs})
        self.thread.start()
        return True

    def _source_handle(self, func):

        if func.__name__ in self._source_handled and hash(func) == self._source_handled[func.__name__]:
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
        # thre are limits for what can reasonably be done here:
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
                if (mod_name:=getattr(obj, "__module__", None)) == "__main__":
                    self._source_handle(obj)
                else:
                    self._handle_module(mod_name)
                    # Poor man's "from x import y"
                    if func.__name__ not in self._source_handled:
                        self.run_string(f"{name} = getattr({mod_name}, '{obj.__name__}')")
            elif isinstance(obj, ModuleType):
                mod_name = obj.__name__
                self._handle_module(mod_name)
                if mod_name != name and name not in self._source_handled:
                    self.run_string(f"{name} = {mod_name}")
            # Test: is it worth setting literal objects that might be
            # used as global variables?

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

        self.map[RET_OFFSET] == 0
        kwargs = kwargs or {}
        self.map.seek(0)
        _failed = False
        for obj in (func, args, kwargs):
            try:
                pickle.dump(obj, self.map)
            except ValueError:
                _failed = True
            if _failed or self.map.tell() >= BFSZ - RET_OFFSET:
                raise RuntimeError(D(f"""\
                    Payload to subinterpreter larger than payload buffer.
                    Call cancelled. If needed, just make buffer larger by tweaking
                    extrainterpreters' {BFSZ=} and {RET_OFFSET=} values.
                    """))

        if revert_main_name:
            mod.__name__ = "__main__"

        code = "_call(0)"
        try:
            interpreters.run_string(self.intno, code)
        except interpreters.RunFailedError as error:
            self.map[RET_OFFSET] = True
            self.exception = error
            print(f"Failing code\n {func}(*{args}, **{kwargs})\n, passed as {code}", file=sys.stderr)
            raise

    def run_string(self, code):
        """Execs a string of code in associated interpreter

        Mostly to mirror interpreters.run_string as a convenient method.
        """
        return interpreters.run_string(self.intno, code)

    def is_running(self):
        """Proxies interpreters.is_running

        Can be used instead of "done" to check if work
        in a threaded call has ended.
        """
        with self.lock:
            return interpreters.is_running(self.intno)
    del is_running # : currently not working. will raise when the interpreter is destroyed.

    def done(self):
        # TBD: check interpreters.is_running?
        return self.map[RET_OFFSET] != 0

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
        return f"Sub-Interpreter <#{self.intno}>"

    def __del__(self):
        # thou shall not leak
        # (At a subinterpreter + 10MB tempfile, that is expensive!)
        # Maybe even add an "at_exit" handler to really kill those files.
        if getattr(self, "intno", None):
            self.close()

class MMapInterpreter(BaseInterpreter):

    def _create_channel(self):
        self.buffer = ProcessBuffer()
        self.map = self.buffer.map
        super()._create_channel()

    def _close_channel(self):
        with self.lock:
            self.buffer.__del__()
            self.map = None
            self.intno = False
        super()._close_channel()

    def _interp_init_code(self):
        code = super()._interp_init_code()
        code += D(f"""\
            import mmap
            import pickle
            import sys
            sys.path[:] = {sys.path}

            BFSZ = {BFSZ}
            RET_OFFSET = {RET_OFFSET}
            _m = mmap.mmap({self.buffer.fileno}, BFSZ)

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


class WorkerOpcodes: # WorkerOpecodes
    close = 0
    import_module = 1
    run_func_no_args = 2
    run_func_args = 3
    run_func_args_kwargs = 4
    run_string = 5


class clsproperty:
    def __init__(self, method):
        self.method = method

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, intance, owner):
        return self.method(owner)

    def __repr__(self):
        return f"clsproperty <{self.name}>"


class Field:
    def __init__(self, bytesize):
        self.size = bytesize

    def _calc_offset(self, owner):
        offset = 0
        for name, obj in owner.__dict__.items():
            if not isinstance(obj, Field):
                continue
            if obj is self:
                return offset
            offset += obj.size

    def __set_name__(self, owner, name):
        self.offset = self._calc_offset(owner)
        self.name = name

    def __get__(self, instance, owner):
        if instance is None:
            return self
        off = instance._offset + self.offset
        return int.from_bytes(instance._data[off: off + self.size], "little")

    def __set__(self, instance, value):
        off = instance._offset + self.offset
        instance._data[off: off + self.size] = value.to_bytes(self.size, "little")


class StructBase:
    """A Struct type class which can attach to offsets in an existing memory buffer

    Currently, only different sized integer fields are implemented.
    """

    slots = ("_data", "_offset")
    def __init__(self, _data, _offset):
        self._data = _data
        self._offset = _offset

    @clsproperty
    def _size(cls):
        size = 0
        for name, obj in cls.__dict__.items():
            if isinstance(obj, Field):
                size += obj.size
        return size

    def _detach(self):
        self._data = self._data[self._offset: self._offset + self._size]
        self._offset = 0


class ProcessBuffer:
    def __init__(self):
        self.fname = tempfile.mktemp()
        self.file = open(self.fname, "w+b")
        self.file.write(b"\x00" * BFSZ)
        self.file.flush()
        self.fileno = self.file.fileno()
        self.map = mmap.mmap(self.fileno, BFSZ)

    def __del__(self):
        # Should be called explicitly by users of the class
        try:
            if self.map:
                self.map.close()
            if self.file:
                self.file.close()
        finally:
            try:
                os.unlink(self.fname)
            except FileNotFoundError:
                pass
        self.map = self.file = None

    def __repr__(self):
        return f"<Mmaped interprocess buffer with {BFSZ:_} bytes>"


""" #WIP
class PipedInterpreter(BaseInterpreter):

    def _create_channel(self):
        self.pipe = Pipe(os.pipe)
        super()._create_channel()

    def _close_channel(self):
        with self.lock:
            os.close(self.pipe.reader)
            os.close(self.pipe.writer)
        super().close()
"""

Interpreter = MMapInterpreter



def list_all():
    """Returns a weakset with all active Interpreter instances

    The idea is to have the same API available in "interpreters" working
    for the higher level objects. But converting the weakset to a full list
    could result in dangling references we don't want.
    """
    return running_interpreters


def destroy_dangling_interpreters():
    for interp in list(running_interpreters):
        try:
            interp.close()
        except Exception:
            import warnings
            warnings.warn(D(f"""\
                Failed to close dangling {interp}. Data exchange file
                {interp.buffer.fname} may not have been erased.
                """))
        else:
            print(f"{interp} closed at main interpreter exit", file=sys.stderr)


# the new "if __name__ == "__main__":
if interpreters.get_current() == interpreters.get_main():
    atexit.register(destroy_dangling_interpreters)


