from threading import TIMEOUT_MAX
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
    """Foundations for cross-interpreter lock.

    Used internally, coupled wiht larger memory structs
    from which it will consume a single byte - it will
    ideally lock those structs.

    A "struct"  should be a StructBase class containing a single-byte
    "lock" field, with a proper buffer.

    (Keep in mind that when the struct is passed to other interpreters,
    if dealocated in the interpreter of origin, the "byte" buffer used
    will point to unalocated memory, with certain disaster ahead)

    It is also used as base for the public Lock classes bellow:
    those can be used in user-code.

    """

    def __init__(self, struct, timeout=DEFAULT_TIMEOUT):
        if isinstance(struct._data, RemoteArray):
            buffer_ptr, size = struct._data._data_for_remote()
        else:  # bytes, bytearray
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
        try:
            return self.acquire(self._timeout)
        finally:
            self._timeout = self._original_timeout

    def acquire(self, timeout):
        # Remember: all attributes are "interpreter-local"
        # just the bytes in the passed in struct are shared.
        if self._entered:
            self._entered += 1
            return self
        if timeout is None or timeout == 0:
            if not _atomic_byte_lock(self._lock_address):
                raise ResourceBusyError("Couldn't acquire lock")
        else:
            threshold = time.monotonic() + timeout
            while time.monotonic() <= threshold:
                if _atomic_byte_lock(self._lock_address):
                    break
                time.sleep(TIME_RESOLUTION * 4)
            else:
                raise TimeoutError("Timeout trying to acquire lock")
        self._entered += 1
        return self

    def __exit__(self, *args):
        self.release()

    def release(self):
        if not self._entered:
            return
        self._entered -= 1
        if self._entered:
            return
        buffer = _remote_memory(self._lock_address, 1)
        buffer[0] = 0
        del buffer
        self._entered = 0
        self._timeout = self._original_timeout

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_entered"] = 0
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

        # RemoteArray is a somewhat high-level data structure,
        # which includes another byte for a lock - just
        # to take account of the buffer life-cycle
        # across interpreters.

        # unfortunatelly, I got no simpler mechanism than that
        # to resolve the problem of the Lock object, along
        # with the buffer being deleted in its owner interpreter
        # while alive in a scondary one.
        # (Remotearrays will go to a parking area, waiting until they
        # are dereferenced remotely before freeing the memory)

        self._buffer = RemoteArray(size=1)
        self._buffer._enter_parent()

        lock_str = _LockBuffer._from_data(self._buffer)
        self._lock = _CrossInterpreterStructLock(lock_str)

    def acquire(self, blocking=True, timeout=-1):
        if blocking:
            timeout = TIMEOUT_MAX if timeout == -1 or not blocking else timeout
            self._lock.acquire(timeout)
            return True
        else:
            try:
                self._lock.acquire(None)
            except ResourceBusyError:
                return False
            return True

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
        if self._lock._entered:
            return True
        try:
            self._lock.acquire(0)
        except ResourceBusyError:
            return True
        self._lock.release()
        return False

    def __getstate__(self):
        return {"_lock": self._lock, "_buffer": self._buffer}



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
    _delay = 4 * TIME_RESOLUTION

    def acquire(self, blocking=True, timeout=-1):
        locked = self.locked()
        if self.locked() and not blocking:
            return False
        timeout = TIMEOUT_MAX if timeout == -1 else timeout
        start = time.monotonic()
        retry = False
        while time.monotonic() - start < timeout:
            if self.locked():
                retry = True
            else:
                try:
                    self._lock.acquire(0)
                except ResourceBusyError:
                    retry = True

                if self._lock._entered > 1:
                    retry = True
                    self._lock.release()

            if not retry:
                return True

            time.sleep(self._delay)
            retry = False

        raise ResourceBusyError("Could not acquire lock")
