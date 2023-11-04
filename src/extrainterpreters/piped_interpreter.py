import sys
import pickle
import weakref
from textwrap import dedent as D

from . import BFSZ, interpreters, running_interpreters
from .memoryboard import ProcessBuffer
from .base_interpreter import BaseInterpreter
from .queue import Pipe
from .simple_interpreter import _BufferedInterpreter
from .utils import Field, StructBase


class WorkerOpcodes:  # WorkerOpecodes
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
            command = Command._from_data(data)
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
            print(
                err, f"in interpreter {interpreters.get_current()}\n\n", file=sys.stderr
            )
    pipe.send(b"ok")


class PipedInterpreter(_BufferedInterpreter):
    """
    This code is unused - and will probably remain so.


    A single interpreter type is to be used - aditional
    subinterpreter capabilities, like being a member
    of a worker-pool will be added by calling aditional
    methods on it
    """

    def _interp_init_code(self):
        code = super()._interp_init_code()
        code += D(
            f"""\
            import extrainterpreters
            import threading

            # pipe = extrainterpreters.Pipe.counterpart_from_fds({self.pipe.originator_fds}, {self.pipe.counterpart_fds})
            disp_thread = threading.Thread(target=extrainterpreters.piped_interpreter._dispatcher, args=(pipe, _m))
            disp_thread.start()
        """
        )
        return code

    def execute(self, func, args=(), kwargs=None):
        # WIP: find out free range in buffer
        slot = 0
        cmd = Command(
            opcode=WO.run_func_args_kwargs,
            exec_mode=ExecModes.immediate,
            data_record=slot,
            data_extra=0,
        )
        data_offset = self.buffer.nranges["send_data"]
        self.map.seek(data_offset)
        pickle.dump(func, self.map)
        pickle.dump(args, self.map)
        pickle.dump(kwargs, self.map)
        exec_data = FuncData._from_data(self.map, slot * FuncData._size)
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
