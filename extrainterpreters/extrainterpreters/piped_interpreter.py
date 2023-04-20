import sys
import pickle
import weakref
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

running_interpreters = weakref.WeakSet()

from .memoryboard import ProcessBuffer
from .base_interpreter import BaseInterpreter
from .pipe import Pipe
from .simple_interpreter import SimpleInterpreter


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



