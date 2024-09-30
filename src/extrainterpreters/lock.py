import time
import sys


from . import running_interpreters

from .remote_array import RemoteArray
from .utils import (
    _atomic_byte_lock,
    _remote_memory,
    _address_and_size,
    guard_internal_use,
    Field,
    StructBase,
    ResourceBusyError,
)

class _LockBuffer(StructBase):
    lock = Field(1)

TIME_RESOLUTION = sys.getswitchinterval()
DEFAULT_TIMEOUT = 50 * TIME_RESOLUTION
DEFAULT_TTL = 3600
LOCK_BUFFER_SIZE = _LockBuffer._size


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
        # Remember: all attributes are "interpreter-local"
        # just the bytes in the passed in struct are shared.
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
                time.sleep(TIME_RESOLUTION * 4)
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


class IntRLock:
    """Cross Interpreter re-entrant lock

    This will allow re-entrant acquires in the same
    interpreter, _even_ if it is being acquired
    in another thread in the same interpreter.

    It should not be very useful - but
    the implementation path code leads here. Prefer the public
    "RLock" and "Lock" classes to avoid surprises.
    """

    def __init__(self):
        self._buffer = bytearray(1)
        # prevents buffer from being moved around by Python allocators
        self._anchor = memoryview(self._buffer)

        lock_str = _LockBuffer._from_data(self._buffer)
        self._lock = _CrossInterpreterStructLock(lock_str)

    def acquire(self, blocking=True, timeout=-1):
        timeout = None if timeout == -1 or not blocking else timeout
        self._lock.timeout(timeout)
        self._lock.__enter__()
        return

    def release(self):
        self._lock.__exit__()

    def __enter__(self):
        self.acquire()
        #self._lock.__enter__()
        return self

    def __exit__(self, *args):
        self.release()
        #self._lock.__exit__()

    def locked(self):
        return bool(self._lock._entered)

    def __getstate__(self):
        return {"_lock": self._lock}



class RLock(IntRLock):
    """Cross interpreter re-entrant lock, analogous to
    threading.RLock
    https://docs.python.org/3/library/threading.html#rlock-objects

    More specifically: it will allow re-entrancy in
    _the same thread_ and _same interpreter_ -
    a different thread in  the same interpreter will still
    be blocked out.
    """


class Lock(IntRLock):
    pass
