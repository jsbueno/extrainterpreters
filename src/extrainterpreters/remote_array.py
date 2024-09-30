import gc
import os
import pickle
import threading
import time
import sys
from functools import wraps

from collections.abc import MutableSequence

from . import interpreters, running_interpreters, get_current, raw_list_all
from . import _memoryboard
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


class RemoteState:
    building = 0
    ready = 1
    serialized = 2
    received = 2
    garbage = 3


class RemoteHeader(StructBase):
    lock = Field(1)
    state = Field(1)
    enter_count = Field(3)
    exit_count = Field(3)


class RemoteDataState:
    not_ready = 0
    read_only = 1  # not used for now.
    read_write = 2


DEFAULT_TTL = 3600
REMOTE_HEADER_SIZE = RemoteHeader._size


# when a RemoteArray can't be destroyed in parent,
# it comes to "sleep" here, where a callback in the
# GC will periodically try to remove it:
_array_registry = []

_collecting_generation = 1


def _collector(action, data):
    """Garbage Collector "plug-in":

    when a RemoteBuffer is closed in parent interpreter
    earlier than it is exited in sub-interpreters,
    it is decomissioned and put in "_array_registry".

    This function will be called when the garbage collector
    is run, and check if any pending buffers can be fully
    dealocated.
    """
    if action != "start" or data.get("generation", 0) < _collecting_generation:
        return
    if not _array_registry:
        return
    new_registry = []
    for buffer in _array_registry:
        buffer.close()
        if buffer._data is not None:
            new_registry.append(buffer)
    _array_registry[:] = new_registry



gc.callbacks.append(_collector)


@MutableSequence.register
class RemoteArray:
    """[WIP]
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
            - can no longer be deleted, unless further criteria are met
            - mark timestamp on buffer - this is checked against TTL
            - on de-serialize: do nothing
            - on client-side "__del__" without enter:
                - increase cancel in buffer canceled counter (?)
            - on client-side "__enter__":
                - check TTL against serialization timestamp - on fail, raise
                - increment "entered" counter on buffer
            - on client-side "__exit__":
                - increment "exited" counter on buffer
        - on parent side "exit":
            - check serialization:
                if no serialization ocurred, just destroy buffer.
            - check enter and exit on buffer counters:
                if failed,  (more enters than exits) save to "pending deletion"
            - check TTL against timestamp of serialization:
                if TTL not reached, save to "pending deletion"
        - on parent side "__del__":
            - call __exit__

        - suggested default TTL: 1 seconds
    """
    __slots__ = (
        "_cursor",
        "_lock",
        "_data",
        "_data_state",
        "_size",
        "_anchor",
        "_mode",
        "_timestamp",
        "_ttl",
        "_internal",
    )

    def __init__(self, *, size=None, payload=None, ttl=DEFAULT_TTL):
        from .lock import _CrossInterpreterStructLock  # avoid circular imports, the easy way.
        if size is None and payload is not None:
            size = len(payload)
        self._size = size
        self._data = bytearray(size + REMOTE_HEADER_SIZE)
        if payload:
            # TBD: Temporary thing - we are allowing zero-copy buffers soon
            self._data[REMOTE_HEADER_SIZE:] = payload
        # Keeping reference to a "normal" memoryview, so that ._data
        # can't be resized (and worse: repositioned) by the interpreter.
        # trying to do so will raise a BufferError
        self._anchor = memoryview(self._data)
        self._cursor = 0
        self._data_state = RemoteDataState.read_write
        self._lock = _CrossInterpreterStructLock(self.header)
        self._mode = _InstMode.parent
        self._ttl = ttl
        self.header.state = RemoteState.building

    @property
    def header(self):
        if self._data_state == RemoteDataState.not_ready:
            raise RuntimeError("Trying to use buffer metadata not ready for use.")
        return RemoteHeader._from_data(self._data, 0)

    def _convert_index(self, index):
        if isinstance(index, slice):
            start, stop, step = index.indices(self._size)
            index = slice(start + REMOTE_HEADER_SIZE, stop + REMOTE_HEADER_SIZE, step)
        else:
            index += REMOTE_HEADER_SIZE
        return index

    def __getitem__(self, index):
        if not self._data_state in (
            RemoteDataState.read_only,
            RemoteDataState.read_write,
        ):
            raise RuntimeError(
                "Trying to read data from buffer that is not ready for use"
            )
        return self._data.__getitem__(self._convert_index(index))

    def __setitem__(self, index, value):
        # TBD: Maybe require lock?
        # An option is to fail if unable to get the lock, and
        # provide a timeouted method that will wait for it.
        # (going for that):
        if self._data_state != RemoteDataState.read_write:
            raise RuntimeError(
                "Trying to write data to buffer that is not ready for use"
            )
        with self._lock:
            return self._data.__setitem__(self._convert_index(index), value)
        raise RuntimeError("Remote Array busy in other thread")

    def _enter_child(self):
        ttl = self._check_ttl()
        if not ttl:
            raise RuntimeError(
                f"TTL Exceeded trying to use buffer in sub-interpreter {get_current()}"
            )
        self._data = _remote_memory(*self._internal[:2])
        self._lock = self._internal[2]
        self._cursor = 0
        with self._lock:
            # Avoid race conditions: better re-test the TTL
            ttl = self._check_ttl()
            if not ttl:
                self._data = None
                raise RuntimeError(
                    f"TTL Exceeded trying to use buffer in sub-interpreter {get_current()}, (stage 2)"
                )
            self._data_state = RemoteDataState.read_write
            if (state := self.header.state) not in (
                RemoteState.serialized,
                RemoteState.received,
            ):
                self._data = None
                raise RuntimeError(f"Invalid state in buffer: {state}")
            self.header.enter_count += 1
        return self

    def _enter_parent(self):
        if self.header.state != RemoteState.building:
            raise RuntimeError("Cannot enter buffer: invalid state")
        self.header.state = RemoteState.ready
        self._data_state = RemoteDataState.read_write
        return self

    def start(self):
        if self._mode == _InstMode.zombie:
            raise RuntimeError(
                "This buffer is decomissioned and no longer can be used for data exchange"
            )
        return (
            self._enter_child()
            if self._mode == _InstMode.child
            else self._enter_parent()
        )

    def __delitem__(self, index):
        raise NotImplementedError()

    def __len__(self):
        return self._size

    def iter(self):
        return iter(self.data)

    def read(self, n=None):
        with self._lock:
            if n is None:
                n = len(self) - self._cursor
            prev = self._cursor
            self._cursor += n
            return self[prev : self._cursor]

    def write(self, content):
        with self._lock:
            if isinstance(content, str):
                content = content.encode("utf-8")
            self[self._cursor : self._cursor + len(content)] = content
            self._cursor += len(content)

    def tell(self):
        return self._cursor

    def readline(self):
        # needed by pickle.load
        result = []
        read = 0
        with self._lock:
            cursor = self._cursor
            while read != 0x0A:
                if cursor >= len(self):
                    break
                result.append(read := self[cursor])
                cursor += 1
            self._cursor = self.cursor
        return bytes(result)

    def seek(self, pos):
        self._cursor = pos

    def _data_for_remote(self):
        # TBD: adjust when spliting payload buffer from header buffer
        # return _address_and_size(self.data)
        address, length = _address_and_size(self._data)
        address += RemoteHeader._size
        length -= RemoteHeader._size
        return address, length

    def __getstate__(self):
        with self._lock:
            if self.header.state not in (
                RemoteState.ready,
                RemoteState.serialized,
                RemoteState.received,
            ):
                raise RuntimeError(
                    f"Can not pickle remote buffer in current state {self.header.state=}"
                )
        with self._lock:
            if self.header.state == RemoteState.ready:
                self.header.state = RemoteState.serialized
        state = {"buffer_data": _address_and_size(self._data)}
        state["ttl"] = self._ttl
        # if not hasattr(self, "_timestamp"):
        # self._timestamp = time.monotonic()
        self._timestamp = time.monotonic()
        state["timestamp"] = self._timestamp
        state["_lock"] = self._lock
        return state

    def __setstate__(self, state):
        self._internal = state["buffer_data"] + (state["_lock"],)
        self._ttl = state["ttl"]
        self._timestamp = state["timestamp"]
        self._size = state["buffer_data"][1] - RemoteHeader._size
        # atention: the Lock will use a byte in the buffer, with an independent allocation mechanism.
        # It is unpickled and ready to use at this point - but we will
        # just add it to the instance in __enter__ , after other checks
        # take place.
        self._lock = None  # state["_lock"]
        self._data = None
        self._cursor = 0
        self._mode = _InstMode.child
        self._data_state = RemoteDataState.not_ready

    def __repr__(self):
        return f"<{self.__class__.__name__} with {len(self)} bytes>"

    def _copy_to_limbo(self):
        inst = type(self).__new__(type(self))
        inst._anchor = self._anchor
        inst._data = self._data
        inst._mode = _InstMode.zombie
        inst._size = self._size
        inst._lock = self._lock
        inst._data_state = self._data_state
        inst._timestamp = self._timestamp
        inst._ttl = self._ttl
        _array_registry.append(inst)

    def _check_ttl(self):
        """Returns True if time-to-live has not expired"""
        if not (timestamp := getattr(self, "_timestamp", None)):
            return True
        return time.monotonic() - timestamp <= self._ttl

    def close(self):
        # when called at interpreter shutdown, "_InstMode" may have been deleted
        target_mode = _InstMode.child if globals()["_InstMode"] else "child"
        if self._mode == target_mode:
            if self._data is None:
                return
            with self._lock:
                self.header.exit_count += 1
            self._data_state = RemoteDataState.not_ready
            self._data = None
            return
        with self._lock:
            early_stages = self.header.state in (
                RemoteState.building,
                RemoteState.ready,
            )
            if early_stages:
                self.header.state = RemoteState.garbage

            ttl_cleared = not self._check_ttl()

            if ttl_cleared and self.header.exit_count >= self.header.enter_count:
                self.header.state = RemoteState.garbage

        if self.header.state == RemoteState.garbage:
            self._data_state = RemoteDataState.not_ready
            del self._anchor
            self._data = None
            return
        if self._mode == _InstMode.zombie:
            # do nothing on fail
            return
        self._copy_to_limbo()
        del self._anchor
        self._data = None
        del self._cursor
        self._data_state = RemoteDataState.not_ready
        # This instance is now a floating "casc" which can no longer access
        # data. GC "plugin" will keep trying to delete it.

    def __exit__(self, *args):
        return self.close()

    def __enter__(self):
        return self.start()

    def __del__(self):
        if getattr(self, "_data", None) is not None:
            try:
                self.close()
            except (TypeError, AttributeError):
                # at interpreter shutdown, some of the names needed in "close"
                # may have been deleted
                pass

