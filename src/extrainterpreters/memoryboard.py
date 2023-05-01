import os
import pickle
import threading
import time
import sys
from functools import wraps

from collections.abc import MutableSequence

from . import interpreters
from . import _memoryboard
from .utils import guard_internal_use, Field, StructBase, _InstMode

from ._memoryboard import _remote_memory, _address_and_size, _atomic_byte_lock

_remote_memory = guard_internal_use(_remote_memory)
_address_and_size = guard_internal_use(_address_and_size)
_atomic_byte_lock = guard_internal_use(_atomic_byte_lock)


class RemoteState:
    building = 0
    ready_to_transmit = 1
    received = 2
    garbage = 3

class RemoteHeader(StructBase):
    lock = Field(1)
    state = Field(1)
    enter_count = Field(3)
    exit_count = Field(3)

TIME_RESOLUTION = 0.002
REMOTE_HEADER_SIZE = RemoteHeader._size

class RemoteArrayState:
    parent = 0
    child_not_ready = 1
    child_ready = 2

class DeleteSemantics:
    # buffer can't die while target interpreter is running:
    follow_interpreter = "follow"
    # buffer can be per-used, and its usage is comunicated back
    # by a side-channel:
    communicated = "communicated"
    # buffer has a fixed time-to-live, after which
    # it can be deleted if it was not entered in another interpreter
    ttl = "ttl"


class _CrossInterpreterStructLock:
    def __init__(self, struct, timeout=None):
        buffer_ptr, size = _address_and_size(struct._data)#, struct._offset)
        #struct_ptr = buffer_ptr + struct._offset
        lock_offset = struct._offset + struct._get_offset_for_field("lock")
        if lock_offset >= size:
            raise ValueError("Lock address out of bounds for struct buffer")
        self._lock_address = buffer_ptr + lock_offset
        self._original_timeout = self._timeout = None
        self._entered = False

    def timeout(self, timeout: None | float):
        """One use only timeout, for the same lock

        with lock.timeout(0.5):
           ...
        """
        self._timeout = timeout
        return self

    def __enter__(self):
        # no check to "self._entered": acquiring the
        # lock byte field will fail if that is the case.
        if self._timeout is None:
            if not _atomic_byte_lock(self._lock_address):
                raise RuntimeError("Couldn't acquire lock")
        else:
            threshold = time.time() + self._timeout
            while time.time() <= threshold:
                if _atomic_byte_lock(self._lock_address):
                    break
            else:
                raise RuntimeError("Timeout on trying to acquire lock")
        self._entered = True
        return self

    def __exit__(self, *args):
        if not self._entered:
            return
        buffer = _remote_memory(self._lock_address, 1)
        buffer[0] = 0
        del buffer
        self._entered = False
        self._timeout = self._original_timeout

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_entered"] = False
        return state


@MutableSequence.register
class RemoteArray:
    """[EARLY WIP]
    Single class which can hold shared buffers across interpreters.

    It is used in the internal mechanisms of extrainterpreters, but offers
    enough safeguards to be used in user code - upon being sending to a
    remote interpreter, data can be shared through this structure in a safe way.

    (It can be sent to the sub-interpreter through a Queue, or by unpckling it
    in a "run_string" call)

    It offers both byte-access with item-setting (Use sliced notation to
    write a lot of data at once) and a file-like interface, mainly for providing
    pickle compatibility.

    """

    """
    Life cycle semantics:
        - creation: set header state to "building"
        - on first serialize (__getstate__), set a "serialized" state:
            - can no longer be deleted, unless further criteia are met
            - mark timestamp on buffer - this is checked against TTL
            - on de-serialize: do nothing
            - on client-side "__del__" without enter:
                - increase cancel in buffer canceled counter (?)
            - on client-side "__enter__":
                - check TTL against serialization timestamp - on fail, Raise
                - increment "entered" counter on buffer
            - on client-side "__exit__":
                - increment "exited" counter on buffer
        - on parent side "exit":
            - check serialization:
                if no serialization ocurred, just destroy buffer.
            - check enter and exit on buffer counters:
                if failed,  (more enter than exit) save to "pending deletion"
            - check TTL against timestamp of serialization:
                if TTL not reached, save to "pending deletion"
        - on parent side "__del__":
            - call __exit__

        - suggested default TTL: 3 seconds

        - check for the possibility of a GC hook (gc.callbacks list)
            - if possible, iterate all "pending deletion" and check conditions in "__exit__"
            - if no GC hook possible,think of another reasonable mechanism to periodically try to delete pending deletion buffers. (dedicate thread with one check per second? Less frequent?

        - extra semantic - buffer type:
            - "main buffer" shared with a single interpreter
                - on unpickle, mark client interpreter on buffer
                - only destroy once interpreter is destroyed
            - shared with multiple interpreters:
                -use semantics above - TL;DR: ultimately TTL and _exit_ counters
                    determine if a buffer can be disposed.
    """
    """
        Other route for deleting semantics:
            - Always depend on external notification of "ok" for deletion
                (maybe a call to `__exit__`).
            - Controled user classes:
                - 1. a driver to an Interpreter which will only allow deletion
                when the interpretr is shut down
                - 2. a Queue which will allow deletion when notified of the memory buffer closure by its built-in return Pipe
            - Pair that with "reference counting" for open clients ("__enter__" on child increment a value on header)
            - keep the idea of a "to_delete" registry, scanned regularly, driven by GC callbacks.
            - keep the "sent to child" above, but flag is set by controled class, not by "__setstate__"

        - these simpler semantics ditch the TTL thing.

        - this is simpler, more maintainable, but has less control and users could shoot themselves in the foot easier.
    """
    __slots__ = ("_cursor", "_lock", "_data", "_state", "_size", "_anchor", "_mode", "_delete_semantics", "_ttl")


    def __init__(self, *, size=None):
        self._size = size
        self._data = bytearray(b"\x00" * (size + REMOTE_HEADER_SIZE))
        # Keeping reference to a "normal" memoryview, so that ._data
        # can't be resized (and worse: repositioned) by the interpreter.
        # trying to do so will raise a BufferError
        self._anchor = memoryview(self._data)
        self._cursor = 0
        self._lock = _CrossInterpreterStructLock(self.header)
        self._state = RemoteArrayState.parent
        self._mode = _InstMode.parent

    @property
    def header(self):
        return RemoteHeader._from_data(self._data, 0)

    def _convert_index(self, index):
        if isinstance(index, slice):
            start, stop, step = index.indices(self._size)
            index = slice(start + REMOTE_HEADER_SIZE, stop + REMOTE_HEADER_SIZE, step)
        else:
            index += REMOTE_HEADER_SIZE
        return index

    def __getitem__(self, index):
        return self._data.__getitem__(self._convert_index(index))

    def __setitem__(self, index, value):
        # TBD: Maybe require lock?
        # An option is to fail if unable to get the lock, and
        # provide a timeouted method that will wait for it.
        # (going for that):
        with self._lock:
            return self._data.__setitem__(self._convert_index(index), value)
        raise RuntimeError("Remote Array busy in other thread")

    def __delitem__(self, index):
        raise NotImplementedError()

    def __len__(self):
        return self._size

    def iter(self):
        return iter(data)

    def read(self, n=None):
        with self._lock:
            if n is None:
                n = len(self) - self._cursor
            prev = self._cursor
            self._cursor += n
            return self[prev: self._cursor]

    def write(self, content):
        with self._lock:
            if isinstance(content, str):
                content = content.encode("utf-8")
            self[self._cursor: self._cursor + len(content)] = content
            self._cursor += len(content)

    def tell(self):
        return self._cursor

    def readline(self):
        # needed by pickle.load
        result = []
        read = 0
        with self._lock:
            cursor = self._cursor
            while read != 0x0a:
                if cursor >= len(self):
                    break
                result.append(read:=self[cursor])
                cursor += 1
            self._cursor = self.cursor
        return bytes(result)

    def seek(self, pos):
        self._cursor = pos

    def _data_for_remote(self):
        return _address_and_size(self._data)

    def __getstate__(self):
        ns = {"buffer_data": self._data_for_remote()}
        return ns

    @guard_internal_use
    def __setstate__(self, state):
        data = _remote_memory(*state["buffer_data"])
        self._lock = state["_lock"]
        self._data = data
        self._cursor = 0

    def __repr__(self):
        return f"<{self.__class__.__name__} with {len(self)} bytes>"

    def close(self):
        if self._mode == _InstMode.child:
            ...
            with self._lock:
                self.header.exit_count += 1

            del self._data
    def __del__(self):
        if self._mode == _InstMode.parent:
            ...
        else:
            self.close()

@MutableSequence.register
class FileLikeArray:
    """DEPRECATED: to be replaced with 'RemoteArray' everywhere
    as soon as it is ready
    """
    __slots__ = ("_cursor", "_lock", "data")

    def __init__(self, data):
        self.data = data
        self._cursor = 0
        self._lock = threading.RLock()

    def __getitem__(self, index):
        return self.data.__getitem__(index)

    def __setitem__(self, index, value):
        return self.data.__setitem__(index, value)

    def __delitem__(self, index):
        raise NotImplementedError()

    def __len__(self):
        return len(self.data)

    def iter(self):
        return iter(data)

    def read(self, n=None):
        with self._lock:
            if n is None:
                n = len(self) - self._cursor
            prev = self._cursor
            self._cursor += n
            return self.data[prev: self._cursor]

    def write(self, content):
        with self._lock:
            if isinstance(content, str):
                content = content.encode("utf-8")
            self.data[self._cursor: self._cursor + len(content)] = content
            self._cursor += len(content)

    def tell(self):
        return self._cursor

    def readline(self):
        # needed by pickle.load
        result = []
        read = 0
        with self._lock:
            cursor = self._cursor
            while read != 0x0a:
                if cursor >= len(self):
                    break
                result.append(read:=self.data[cursor])
                cursor += 1
            self._cursor = self.cursor
        return bytes(result)

    def seek(self, pos):
        self._cursor = pos

    def _data_for_remote(self):
        return _address_and_size(self.data)

    def __getstate__(self):
        ns = {"buffer_data": self._data_for_remote()}
        return ns

    @guard_internal_use
    def __setstate__(self, state):
        data = _remote_memory(*state["buffer_data"])
        self._lock = threading.RLock()
        self.data = data
        self._cursor = 0

    def __repr__(self):
        return f"<{self.__class__.__name__} with {len(self)} bytes>"

class BufferBase:
    map: FileLikeArray

    def close(self):
        self.map = None


class ProcessBuffer(BufferBase):
    def __init__(self, size, ranges: dict[int,str]|None=None):
        if ranges is None:
            ranges = {
                0: "command_area",
                4096: "send_data",
                (size // 5 * 4): "return_data"
            }

        self.size = size
        self.ranges = ranges
        self.nranges = {v:k for k, v in ranges.items()}
        self._init_range_sizes()
        self.map = FileLikeArray(bytearray(b"\x00" * size))

    def _init_range_sizes(self):
        prev_range = ""
        last_range_start = 0
        self.range_sizes = {}
        for i, (range_name, offset) in enumerate(self.nranges.items()):
            if i:
                self.range_sizes[prev_range] = offset - self.nranges[prev_range]
            prev_range = range_name
            if offset < last_range_start:
                raise ValueError("Buffer Range window starts must be in ascending order")
            last_range_start = offset

    def __repr__(self):
        return f"<interprocess buffer with {self.size:_} bytes>"


class LockState:
    free = 0
    locked = 1

class State:
    not_initialized = 0
    building = 1
    ready = 2
    locked = 3
    # lock_complete = 4
    garbage = 5


class BlockLock(StructBase):
    state = Field(1) # State
    lock = Field(1)  # LockState
    owner = Field(4) # InterpreterID(threadID?)
    content_type = Field(1)  # 0 for pickled data
    content_address = Field(8)
    content_length = Field(8)


class LockableBoardParent(BufferBase):
    maxblocks = 2048
    def __init__(self):
        self._size = self.maxblocks
        data = bytearray(b"\x00" * self.maxblocks * BlockLock._size)
        self.map = FileLikeArray(data)
        #self.root = LockableBoardRoot.from_data(self.map, 0)
        #self.root.size = 0
        self.blocks = {}

    def __getstate__(self):
        ns = self.__dict__.copy()
        del ns["blocks"]
        return ns

    @guard_internal_use
    def __setstate__(self, state):
        self.__class__ = LockableBoardChild
        self.__dict__.update(state)

    def new_item(self, data):
        data = OwnableBuffer(pickle.dumps(data))
        offset, control = self.get_free_block()
        control.content_address, control.content_length = data.map._data_for_remote()
        self.blocks[offset] = data
        control.owner = 0
        control.state = State.ready
        control.lock = 0
        return offset // BlockLock._size, control

    def __getitem__(self, index):
        offset = BlockLock._size * index
        return BlockLock._from_data(self.map, offset)

    def __delitem__(self, index):
        offset = BlockLock._size * index
        control = BlockLock._from_data(self.map, offset)
        if control.lock != 0:
            raise ValueError("Item is locked")
        if control.state not in (State.not_initialized, State.ready, State.garbage):
            raise ValueError("Invalid State")
        self.blocks.pop(offset, None)
        control.state = State.not_initialized
        control.content_address = 0

    def collect(self):
        data = self.map.data
        free_blocks = 0
        for index in range(0, self._size):
            # block = BlockLock(self.map, offset)
            # if block.lock == LockState.garbage:
            # TBD: benchmark things with
            # the above two lines instead of the "low level":
            offset = index * BlockLock._size
            if data[offset] == State.garbage:
                del self.blocks[offset]
                data[offset] = data[offset + 1] = 0
            if data[offset] == 0:
                free_blocks += 1
        return free_blocks

    def get_free_block(self):
        # maybe call self.collect automatically?
        id_ = threading.current_thread().native_id
        data = self.map.data
        for offset in range(0, len(data), BlockLock._size):
            if data[offset] == State.garbage:
                del self.blocks[offset]
                data[offset] = data[offset + 1] = 0
            if data[offset] == 0 and data[offset+1] == 0:
                lock_ptr = self.map._data_for_remote()[0] + offset + 1
                if not _atomic_byte_lock(lock_ptr):
                    continue
                # we are the now sole owners of the block.
                block = BlockLock._from_data(self.map, offset)
                block.onwer = id_
                block.state = State.building
                break
        else:
            raise ValueError("Board full. Can't allocate data block to send to remote interpreter")
        return offset, block

    # not implementing __len__ because occupied
    # blocks are not always in sequence. Trying
    # to iter with len + getitem will yield incorrect results.

    def __repr__(self):
        free_blocks = self.collect()
        return f"LockableBoard with {free_blocks} free slots."


class LockableBoardChild(BufferBase):
    def __init__(self, buffer_ptr, buffer_length):
        self.map = FileLikeArray(_remote_memory(buffer_ptr, buffer_length))
        self._size = len(self.map) // BlockLock._size
        #self.start_offset = random.randint(0, self._size)

    def get_work_data(self):
        control = BlockLock._from_data(self.map, 0)
        for index in range(0, self._size):
            offset = index * BlockLock._size
            control._offset = offset
            if control.state != State.ready:
                continue
            lock_ptr = self.map._data_for_remote()[0] + offset + 1
            if _atomic_byte_lock(lock_ptr):
                break
        else:
            return None
        control.owner = threading.current_thread().native_id
        control.state = State.locked
        control.lock = 0
        buffer = _remote_memory(control.content_address, control.content_length)
        data = pickle.loads(buffer)
        del buffer
        control.state = State.garbage
        # TBD: caller could have a channel to comunicate the parent thread its done
        # with the buffer.
        return index, data

    def __delitem__(self, index):
        raise TypeError("Removal of blocks can only happen on parent-side of Buffer")

    def __getitem__(self, index):
        offset = BlockLock._size * index
        return BlockLock._from_data(self.map, offset)

    def __del__(self):
        self.close()

    def __repr__(self):
        return f"<Child LockableBoard on interpreter {interpreters.get_current()}>"


class OwnableBuffer(BufferBase):
    def __init__(self, payload):
        """'use-once' read-only buffer meant to be read by a single peer

        The addresses and lock-blocks should be stored in a
        LockableBoard object.
        """

        self.map = FileLikeArray(payload)

