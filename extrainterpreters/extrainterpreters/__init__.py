import atexit
import inspect
import threading
import os, sys
import pickle
import selectors
import weakref
import warnings
from pathlib import Path
from types import ModuleType, FunctionType
from textwrap import dedent as D

from .memoryboard import ProcessBuffer

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

running_interpreters = weakref.WeakSet()

from .base_interpreter import BaseInterpreter

class Pipe:
    """Full Duplex Pipe class.
    """
    def __init__(self):
        self.originator_fds = os.pipe()
        self.counterpart_fds = os.pipe()
        self._all_fds = self.originator_fds + self.counterpart_fds
        self._post_init()

    def _post_init(self):
        self.closed = False
        self._selector = selectors.DefaultSelector()
        self._selector.register(self.counterpart_fds[0], selectors.EVENT_READ, self.read_blocking)

    @classmethod
    def counterpart_from_fds(cls, originator_fds, counterpart_fds):
        self = cls.__new__(cls)
        self.originator_fds = originator_fds
        self.counterpart_fds = counterpart_fds
        self._all_fds = ()
        self._post_init()
        return self.counterpart

    @property
    def counterpart(self):
        new_inst = type(self).__new__(type(self))
        new_inst.originator_fds = self.counterpart_fds
        new_inst.counterpart_fds = self.originator_fds
        new_inst._all_fds = ()
        new_inst._post_init()
        return new_inst

    def send(self, data):
        if self.closed:
            warnings.warn("Pipe already closed. No data sent")
            return
        if isinstance(data, str):
            data = data.encode("utf-8")
        os.write(self.originator_fds[1], data)

    def read_blocking(self, amount=4096):
        return os.read(self.counterpart_fds[0], amount)

    def read(self, timeout=0):
        if self.closed:
            warnings.warn("Pipe already closed. Not trying to read anything")
            return b""
        result = b""
        for key, mask in self._selector.select(timeout=timeout):
            v = key.data()
            if key.fd == self.counterpart_fds[0]:
                result = v
        return result

    def close(self):
        for fd in getattr(self, "_all_fds", ()):
            try:
                os.close(fd)
            except OSError:
                pass
        self.closed = True
        self._all_fds = ()

    def __del__(self):
        self.close()



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

    Methods and propertys start with "_" just to avoid nae clashes with fields.
    Feel free to use anything here. (use is by subclassing and defining fields)
    """

    slots = ("_data", "_offset")
    def __init__(self, _data, _offset=0):
        self._data = _data
        self._offset = _offset

    @clsproperty
    def _fields(cls):
        for k, v in cls.__dict__.items():
            if isinstance(v, Field):
                yield k

    @classmethod
    def _from_values(cls, *args):
        data = bytearray(b"\x00" * cls._size)
        self = cls(data, 0)
        for arg, field_name in zip(args, self._fields):
            setattr(self, field_name, arg)
        return self

    @property
    def _bytes(self):
        return bytes(self._data[self._offset: self._offset + self._size])

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

class WorkerOpcodes: # WorkerOpecodes
    close = 0
    import_module = 1
    run_func_no_args = 2
    run_func_args = 3
    run_func_args_kwargs = 4
    run_string = 5


WO = WorkerOpcodes

class ExecModes:
    immediate = 0
    new_thread = 1
    thread_pool = 2


class Command(StructBase):
    opcode = Field(1)
    exec_mode = Field(1)
    data_record = Field(2)
    data_extra = Field(4)

class FuncData(StructBase):
    data_offset = Field(4)


def _dispatcher(pipe, buffer):
    """the core running function in a PipedInterpreter

    This is responsible for watching comunications with the parent
    interpreter, dispathing execution, and place the return values
    """

    while True:
        try:
            data = pipe.read(timeout=0.1)
            if not data:
                continue
            command = Command(data)
            match command.opcode:
                case WO.close:
                    break
                case WO.run_func_args_kwargs:
                    funcdata = FuncData(buffer, FuncData._size * command.data_record)
                    buffer.seek(funcdata.data_offset)
                    func = pickle.load(buffer)
                    args = pickle.load(buffer)
                    kwargs = pickle.load(buffer)
                    if command.exec_mode == ExecModes.immediate:
                        result = func(*args, **kwargs)
                        pipe.send(pickle.dumps(result))

                case _:
                    pass  # opcode not implemented
        except Exception as err:
            # TBD: define exceptions policy
            print(err, f"in interpreter {interpreters.get_current()}\n\n", file=sys.stderr)
    pipe.send(b"ok")



class PipedInterpreter(SimpleInterpreter):

    def _create_channel(self):
        super()._create_channel()
        self.pipe = Pipe()

    def _interp_init_code(self):
        code = super()._interp_init_code()
        code += D(f"""\
            import extrainterpreters
            import threading

            pipe = extrainterpreters.Pipe.counterpart_from_fds({self.pipe.originator_fds}, {self.pipe.counterpart_fds})
            disp_thread = threading.Thread(target=extrainterpreters._dispatcher, args=(pipe, _m))
            disp_thread.start()
        """)
        return code

    def execute(self, func, args=(), kwargs=None):
        # WIP: find out free range in buffer
        slot = 0
        cmd = Command._from_values(WO.run_func_args_kwargs, ExecModes.immediate, slot, 0)
        data_offset = self.buffer.nranges["send_data"]
        self.map.seek(data_offset)
        pickle.dump(func, self.map)
        pickle.dump(args, self.map)
        pickle.dump(kwargs, self.map)
        exec_data = FuncData(self.map, slot * FuncData._size)
        exec_data.data_offset = data_offset
        self.pipe.send(cmd._bytes)

    def result(self):
        # if last command exec_mode was "immediate":
        return pickle.loads(self.pipe.read(timeout=0.01))

    def _create_channel(self):
        self.pipe = Pipe()
        super()._create_channel()

    def _close_channel(self):
        with self.lock:
            self.pipe.send(Command._from_values(WO.close, 0, 0, 0)._bytes)
            self.pipe.read(timeout=None)
            self.pipe.close()
        super()._close_channel()


Interpreter = SimpleInterpreter



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


