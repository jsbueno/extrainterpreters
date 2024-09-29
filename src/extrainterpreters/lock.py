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
    guard_internal_use,
    Field,
    DoubleField,
    StructBase,
    _InstMode,
    ResourceBusyError,
)

from ._memoryboard import _remote_memory, _address_and_size, _atomic_byte_lock

_remote_memory = guard_internal_use(_remote_memory)
_address_and_size = guard_internal_use(_address_and_size)
_atomic_byte_lock = guard_internal_use(_atomic_byte_lock)


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


TIME_RESOLUTION = sys.getswitchinterval()
DEFAULT_TIMEOUT = 50 * TIME_RESOLUTION
DEFAULT_TTL = 3600
REMOTE_HEADER_SIZE = RemoteHeader._size


class _CrossInterpreterStructLock:
    def __init__(self, struct, timeout=DEFAULT_TIMEOUT):
        buffer_ptr, size = _address_and_size(struct._data)  # , struct._offset)
        # struct_ptr = buffer_ptr + struct._offset
        lock_offset = struct._offset + struct._get_offset_for_field("lock")
        if lock_offset >= size:
            raise ValueError("Lock address out of bounds for struct buffer")
        self._lock_address = buffer_ptr + lock_offset
        self._original_timeout = self._timeout = timeout
        self._entered = 0

    def timeout(self, timeout: None | float):
        """One use only timeout, for the same lock

        with lock.timeout(0.5):
           ...
        """
        self._timeout = timeout
        return self

    def __enter__(self):
        if self._entered:
            self._entered += 1
            return self
        if self._timeout is None:
            if not _atomic_byte_lock(self._lock_address):
                self._timeout = self._original_timeout
                raise ResourceBusyError("Couldn't acquire lock")
        else:
            threshold = time.time() + self._timeout
            while time.time() <= threshold:
                if _atomic_byte_lock(self._lock_address):
                    break
            else:
                self._timeout = self._original_timeout
                raise TimeoutError("Timeout trying to acquire lock")
        self._entered += 1
        return self

    def __exit__(self, *args):
        if not self._entered:
            return
        self._entered -= 1
        if self._entered:
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

