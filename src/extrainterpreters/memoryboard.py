import os
import pickle
import threading

from collections.abc import MutableSequence

from . import _memoryboard
from .queue import Field, StructBase

@MutableSequence.register
class FileLikeArray:
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

    def __repr__(self):
        return f"<self.__class__.__name__ with {len(self)} bytes>"

class BufferBase:
    map: FileLikeArray

    def _data_for_remote(self):
        return _memoryboard.address_and_size(self.map.data)

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
    lock_started = 3
    lock_complete = 4
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


    def new_item(self, data):
        data = OwnableBuffer(pickle.dumps(data))
        offset, control = self.get_free_block()
        control.content_address, control.content_length = data._data_for_remote()
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

    def collect(self):
        data = self.map.data
        free_blocks = 0
        for offset in range(0, len(data), BlockLock._size):
            # block = BlockLock(self.map, offset)
            # if block.lock == LockState.garbage:
            # TBD: benchemark things with
            # the above two lines instead of the "low level":
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
            if data[offset] == 5:
                del self.blocks[offset]
                data[offset] = data[offset + 1] = 0
            if data[offset] == 0 and data[offset+1] == 0:
                lock_ptr = self._data_for_remote()[0] + offset + 1
                if not _memoryboard.atomic_byte_lock(lock_ptr):
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



class OwnableBuffer(BufferBase):
    def __init__(self, payload):
        """'use-once' read-only buffer meant to be read by a single peer

        The addresses and lock-blocks should be stored in a
        LockableBoard object.
        """

        self.map = FileLikeArray(payload)

