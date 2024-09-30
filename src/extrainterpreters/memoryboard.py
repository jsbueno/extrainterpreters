import os
import pickle
import threading
import time
import sys
from functools import wraps

from collections.abc import MutableSequence

from . import interpreters, running_interpreters, get_current, raw_list_all
from . import _memoryboard
from .remote_array import RemoteArray
from .utils import (
    _InstMode,
    _remote_memory,
    _address_and_size,
    _atomic_byte_lock,
    DoubleField,
    Field,
    StructBase,
    ResourceBusyError,
    guard_internal_use
)

from .lock import _CrossInterpreterStructLock


DEFAULT_TTL = 3600


class BufferBase:
    map: RemoteArray

    def close(self):
        if getattr(self, "map", None):
            self.map.close()
            self.map = None

    def __del__(self):
        self.close()


class ProcessBuffer(BufferBase):
    def __init__(self, size, ranges: dict[int, str] | None = None):
        if ranges is None:
            ranges = {
                0: "command_area",
                4096: "send_data",
                (size // 5 * 4): "return_data",
            }

        self.size = size
        self.ranges = ranges
        self.nranges = {v: k for k, v in ranges.items()}
        self._init_range_sizes()
        self.map = RemoteArray(size=size)
        self.map.__enter__()

    def _init_range_sizes(self):
        prev_range = ""
        last_range_start = 0
        self.range_sizes = {}
        for i, (range_name, offset) in enumerate(self.nranges.items()):
            if i:
                self.range_sizes[prev_range] = offset - self.nranges[prev_range]
            prev_range = range_name
            if offset < last_range_start:
                raise ValueError(
                    "Buffer Range window starts must be in ascending order"
                )
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
    # parent_review = 4
    garbage = 5


class BlockLock(StructBase):
    state = Field(1)  # State
    lock = Field(1)  # LockState
    owner = Field(4)  # InterpreterID(threadID?)
    content_type = Field(1)  # 0 for pickled data
    content_address = Field(8)
    content_length = Field(8)


class LockableBoard:
    maxblocks = 2048

    def __init__(self, size=None):
        self._size = size or self.maxblocks
        self.map = RemoteArray(size=self._size * BlockLock._size)
        self.map.start()
        self.blocks = {}
        self._parent_interp = get_current()
        # This is incremented when a item that "looks good"
        # was originally exported by a interpreter that is closed now.
        # Also, queue.Queue uses and can decrement this to keep
        # queues internal state.
        self._items_closed_interpreters = 0

    @property
    def mode(self):
        return (
            _InstMode.parent
            if get_current() == self._parent_interp
            else _InstMode.child
        )

    def __getstate__(self):
        ns = self.__dict__.copy()
        del ns["blocks"]
        return ns

    @guard_internal_use
    def __setstate__(self, state):
        self.__dict__.update(state)
        self.map.start()
        self.blocks = {}

    def new_item(self, data):
        """Atomically post a pickled Python object in a
        shareable buffer.

        Objects posted can be retrieved out-of-order
        by calling "fetch_item".

        Whatever caller posts a new_item is responsible to call `.collect()`  or `del`
        at some point in the future to ensure origin-side data of objects
        that were read in other interpreters is collected, otherwise
        there will be leaks.

        (All data posted live as an anchor on "self.blocks")
        """
        data = OwnableBuffer(pickle.dumps(data))
        offset, control = self.get_free_block()
        control.content_address, control.content_length = data.map._data_for_remote()
        self.blocks[offset] = data
        control.owner = get_current()
        control.state = State.ready
        control.lock = 0
        return offset // BlockLock._size, control

    def __getitem__(self, index):
        offset = BlockLock._size * index
        return BlockLock._from_data(self.map, offset)

    def __delitem__(self, index):
        offset = BlockLock._size * index
        control = BlockLock._from_data(self.map, offset)
        lock_ptr = self.map._data_for_remote()[0] + offset + 1
        if not _atomic_byte_lock(lock_ptr):
            raise ValueError("Could not get block lock for deleting")
        if control.state not in (State.not_initialized, State.ready, State.garbage):
            raise ValueError("Invalid State")

        self.blocks.pop(offset, None)
        control.state = State.not_initialized
        control.content_address = 0
        control.lock = 0

    def collect(self):
        data = self.map
        free_blocks = 0
        for index in range(0, self._size):
            # block = BlockLock(self.map, offset)
            # if block.lock == LockState.garbage:
            # TBD: benchmark things with
            # the above two lines instead of the "low level":
            offset = index * BlockLock._size
            if data[offset] == State.garbage:
                try:
                    del self[index]
                except ValueError:
                    pass
            elif index in self.blocks and data[offset] == State.not_initialized:
                del self.blocks[index]
            if data[offset] == State.not_initialized:
                free_blocks += 1
        return free_blocks

    def get_free_block(self):
        # maybe call self.collect automatically?
        id_ = threading.current_thread().native_id
        data = self.map
        for offset in range(0, len(data), BlockLock._size):
            if data[offset] == State.garbage:
                self.blocks.pop(offset, None)
                # data[offset] = data[offset + 1] = 0
            if (
                data[offset] in (State.not_initialized, State.garbage)
                and data[offset + 1] == 0
            ):
                lock_ptr = self.map._data_for_remote()[0] + offset + 1
                if not _atomic_byte_lock(lock_ptr):
                    continue
                # we are the now sole owners of the block.
                block = BlockLock._from_data(self.map, offset)
                block.owner = id_
                block.state = State.building
                break
        else:
            raise ValueError(
                "Board full. Can't allocate data block to send to remote interpreter"
            )
        return offset, block

    def fetch_item(self):
        """Atomically retrieves an item posted with "new_item" and frees its block"""
        control = BlockLock._from_data(self.map, 0)
        interp_list = raw_list_all()
        for index in range(0, self._size):
            offset = index * BlockLock._size
            control._offset = offset
            if control.state != State.ready:
                continue
            lock_ptr = self.map._data_for_remote()[0] + offset + 1
            if not _atomic_byte_lock(lock_ptr):
                continue
            if control.owner not in interp_list:
                # Counter consumed by queues: they have to fetch
                # a byte on the notification pipe if an item
                # vanished due to this.
                self._items_closed_interpreters += 1
                control.state = State.garbage
                control.lock = 0
                continue
            break
        else:
            return None
        # control.owner = threading.current_thread().native_id
        control.state = State.locked
        control.lock = 0
        buffer = _remote_memory(control.content_address, control.content_length)
        try:
            item = pickle.loads(buffer)
        finally:
            del buffer
        # Maybe add an option to "peek" an item only?
        # all that would be needed would be to restore state to "ready"
        control.state = State.garbage
        # TBD: caller could have a channel to comunicate the parent thread its done
        # with the buffer.
        return index, item

    # not implementing __len__ because occupied
    # blocks are not always in sequence. Trying
    # to iter with len + getitem will yield incorrect results.

    def close(self):
        if hasattr(self, "map") and self.map:
            self.map.close()
            self.map = None

    def __del__(self):
        self.close()

    def __repr__(self):
        if self.mode == _InstMode.parent:
            free_blocks = self.collect()
        return f"LockableBoard with {free_blocks} free slots."


class OwnableBuffer(BufferBase):
    def __init__(self, payload):
        """'use-once' read-only buffer meant to be read by a single peer

        The addresses and lock-blocks should be stored in a
        LockableBoard object.
        """

        self.map = RemoteArray(payload=payload)
        self.map.start()
