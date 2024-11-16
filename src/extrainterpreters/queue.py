import os
import pickle
import selectors
import time
import warnings
from collections import deque
from functools import wraps
from textwrap import dedent as D

import queue as threading_queue
from queue import Empty, Full  #  make these available to users

from .utils import (
    guard_internal_use,
    StructBase,
    Field,
    _InstMode,
    non_reentrant,
    ResourceBusyError,
)
from .remote_array import RemoteArray, RemoteState
from .memoryboard import LockableBoard
from . import interpreters, get_current
from .resources import EISelector, register_pipe, PIPE_REGISTRY


# TBD: Fix blocking/timeout semantics
# (default for timeout=None should be "wait forever" everywhere)
_DEFAULT_BLOCKING_TIMEOUT = 5  #


class ActiveInstance(StructBase):
    counter = Field(2)


class _PipeBase:
    # class LockableMixin:
    def __init__(self):
        # TBD: a "multiinterpreter lock" will likely be a wrapper over this patterh
        # use that as soon as it is done:
        self._lock_array = RemoteArray(size=ActiveInstance._size)
        self._lock_array.header.state = RemoteState.ready
        super().__init__()

    @property
    def active(self):
        return ActiveInstance._from_data(self._lock_array, 0)

    def _post_init(self):
        if self._lock_array._data is None:
            self._lock_array.start()
        self.lock = self._lock_array._lock
        with self.lock:
            self.active.counter += 1

    def readline(self):
        # needed by pickle.load
        result = []
        read_byte = ...
        while read_byte and read_byte != "\n":
            result.append(read_byte := self.read(1))
        return b"".join(result)

    def _read_ready_callback(self, key, *args):
        self._read_ready_flag = True

    def select(self, timeout=0):
        self._read_ready_flag = False
        EISelector.select(timeout=timeout, target_fd=self.reader_fd)
        return self._read_ready_flag

    def _write_ready_callback(self, *args):
        self._write_ready_flag = True

    def select_for_write(self, timeout=None):
        self._write_ready_flag = False
        EISelector.select(timeout=timeout, target_fd=self.writer_fd)
        return self._write_ready_flag

    def send(self, data, timeout=None):
        if self.closed:
            warnings.warn("Pipe already closed. No data sent")
            return
        if isinstance(data, str):
            data = data.encode("utf-8")
        if self.select_for_write(timeout):
            return os.write(self.writer_fd, data)

        raise TimeoutError("FD not ready for writting - Pipe full")

    write = send

    def read_blocking(self, amount=4096):
        result = os.read(self.reader_fd, amount)
        return result

    def read(self, amount=4096, timeout=0):
        if self.closed:
            warnings.warn("Pipe already closed. Not trying to read anything")
            return b""
        result = b""
        if self.select(timeout):
            return self.read_blocking(amount)
        return b""

    def close(self):
        # FIXME: embedd a shared buffer struct with a count of instances
        # of the same pipe per interpreter. When the last interpreter
        # calls  :close" (or __del__), close the fds
        if getattr(self, "lock", None):
            with self.lock:
                all_fds = getattr(self, "_all_fds", ())
                for fd in all_fds:
                    try:
                        EISelector.unregister(fd)
                    except KeyError:
                        pass
                if all_fds in PIPE_REGISTRY:
                    del PIPE_REGISTRY[self._all_fds]
                # Close file descriptors if this was the last
                # interpreter where they are open:
                if self.active.counter:
                    self.active.counter -= 1
                if self.active.counter == 0:
                    for fd in getattr(self, "_all_fds", ()):
                        try:
                            os.close(fd)
                        except OSError:
                            pass
        self.closed = True
        self._all_fds = ()
        self.reader_fd = None
        self.writer_fd = None

    def __del__(self):
        if not getattr(self, "closed", False):
            self.close()


class _SimplexPipe(_PipeBase):
    """Wrapper around os.pipe

    makes use of the per-interpreter single Selector, and registry -
    so that even if unpickled several times in the same interpreter
    it still uses the same underlying resources.
    """

    # TBD: Refactor features common to "_DuplexPipe" (Pipe cls bellow)
    def __init__(self):
        self.reader_fd, self.writer_fd = self._all_fds = register_pipe(os.pipe(), self)
        super().__init__()
        self._post_init()
        self._bound_interp = get_current()

    @classmethod
    def _unpickler(cls, reader_fd, writer_fd):
        if (reader_fd, writer_fd) in PIPE_REGISTRY:
            return PIPE_REGISTRY[reader_fd, writer_fd]
        instance = cls.__new__(cls)
        instance.reader_fd = reader_fd
        instance.writer_fd = writer_fd
        instance._new_in_interpreter = True
        return instance

    def __reduce__(self):
        return (
            type(self)._unpickler,
            (self.reader_fd, self.writer_fd),
            self.__dict__.copy(),
        )

    # @guard_internal_use
    def __setstate__(self, state):
        if getattr(self, "_new_in_interpreter", False):
            # initialize only on first unpickle in an interpreter
            self.__dict__.update(state)
            self._post_init()
            if hasattr(self, "_post_setstate"):
                self._post_setstate(state)
            register_pipe((self.reader_fd, self.writer_fd), self)
        self._new_in_interpreter = False

    def _post_init(self):
        self.closed = False
        EISelector.register(
            self.reader_fd, selectors.EVENT_READ, self._read_ready_callback
        )
        EISelector.register(
            self.writer_fd, selectors.EVENT_WRITE, self._write_ready_callback
        )
        self._read_ready_flag = False
        self._write_ready_flag = False
        super()._post_init()


class _DuplexPipe(_PipeBase):
    """Full Duplex Pipe class.

    # warning - not sure if this will be kept in the API at all.
    # do not rely on it
    """

    def __init__(self):
        self.originator_fds = os.pipe()
        self.counterpart_fds = os.pipe()
        super().__init__()

        self._all_fds = self.originator_fds + self.counterpart_fds
        self._post_init()
        self._bound_interp = get_current()

    # These guys are cute!
    # A Pipe unpickled in another interpreter
    # is automatically promoted to be "the other end"
    # of the Pipe that was pickled in parent interpreter
    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop("_counterpart", None)
        return state

    @guard_internal_use
    def __setstate__(self, state):
        current = get_current()
        self.__dict__.update(state)
        if current != state["_bound_interp"]:
            # reverse the direction in child intrepreter
            self.originator_fds, self.counterpart_fds = (
                self.counterpart_fds,
                self.originator_fds,
            )
        self._post_init()
        if hasattr(self, "_post_setstate"):
            self._post_setstate(state)

    def _post_init(self):
        self.writer_fd = self.originator_fds[1]
        self.reader_fd = self.counterpart_fds[0]
        self.closed = False
        EISelector.register(
            self.counterpart_fds[0], selectors.EVENT_READ, self._read_ready_callback
        )
        EISelector.register(
            self.originator_fds[1], selectors.EVENT_WRITE, self._write_ready_callback
        )
        self._read_ready_flag = False
        self._write_ready_flag = False
        super()._post_init()

    @property
    def counterpart(self):
        if hasattr(self, "_counterpart"):
            return self._counterpart
        cls = type(self)
        new_inst = cls.__new__(type(self))
        new_inst.originator_fds = self.originator_fds
        new_inst.counterpart_fds = self.counterpart_fds
        new_inst._all_fds = ()
        new_inst._lock_array = self._lock_array
        new_inst.lock = self.lock
        new_inst._post_init()
        self._counterpart = new_inst
        return new_inst


def _parent_only(func):
    @wraps(func)
    def wrapper(self, *args, **kw):
        if self.mode != _InstMode.parent:
            raise RuntimeError(
                f"Invalid queue state: {func.__name__} can only be called on parent interpreter"
            )
        return func(self, *args, **kw)

    return wrapper


def _child_only(func):
    @wraps(func)
    def wrapper(self, *args, **kw):
        if self.mode != _InstMode.child:
            raise RuntimeError(
                f"Invalid queue state: {func.__name__} can only be called on child interpreter"
            )
        return func(self, *args, **kw)

    return wrapper


class _ABSQueue:
    def put_nowait(self, item):
        """Equivalent to put(item, block=False)."""
        return self.put(item, block=False)

    def get_nowait(self):
        """Equivalent to get(False)"""
        return self.get(False)


class SingleQueue(_ABSQueue):
    """ "simplex" queue implementation, able to connect a parent interpreter
    to a single sub-interpreter.

    Data passing uses Pipes alone as a channel to pickling/unpickling objects.

    Instances can be in two states: parent side or child side. When unpickled on
    a sub-interpreter, the instance will assume the "child side" state  -
    appropriate methods should only be used in each side. (basically: put on parent, get on child)
    """

    mode = _InstMode.parent

    @_parent_only
    def __init__(self, maxsize=0):
        self.maxsize = maxsize
        self.pipe = _DuplexPipe()
        self.mode = _InstMode.parent
        self.bound_to_interp = get_current()
        self._size = 0
        self._post_init_parent()

    def _post_init_parent(self):
        self._writing_fd = self.pipe.originator_fds[1]
        EISelector.register(
            self._writing_fd, selectors.EVENT_WRITE, self._ready_to_write
        )
        self._ready_to_write_flag = False

    def __getstate__(self):
        state = self.__dict__.copy()
        return state

    @guard_internal_use
    def __setstate__(self, state):
        from . import interpreters

        self.__dict__.update(state)
        if self.pipe._bound_interp == get_current():
            self._post_init_parent()
        else:
            self.mode = _InstMode.child

    @property
    def size(self):
        # implicit binary protocol:
        # other end of pipe should post, as an unsigned byte,
        # the amount of tasks confirmed with ".task_done()"
        if amount := self.pipe.read():
            for done_count in amount:
                self._size -= done_count
        return self._size

    def qsize(self):
        """Return the approximate size of the queue. Note, qsize() > 0 doesn-’t guarantee that a subsequent get() will not block, nor will qsize() < maxsize guarantee that put() will not block."""
        return self.size

    def empty(self):
        """
        Queue.empty()
        Return True if the queue is empty, False otherwise. If empty() returns True it doesn’t guarantee that a subsequent call to put() will not block. Similarly, if empty() returns False it doesn’t guarantee that a subsequent call to get() will not block.
        """
        pass

    def full(self):
        """Return True if the queue is full, False otherwise. If full() returns True it doesn’t guarantee that a subsequent call to get() will not block. Similarly, if full() returns False it doesn’t guarantee that a subsequent call to put() will not block."""
        if not self._ready_to_send():
            return True
        return self.size >= self.maxsize

    def _ready_to_write(self, *args):
        self._ready_to_write_flag = True

    def _ready_to_send(self, timeout=0):
        EISelector.select(timeout=timeout, target_fd=self._writing_fd)
        result = self._ready_to_write_flag
        self._ready_to_write_flag = False
        return result

    @_parent_only
    def put(self, item, block=True, timeout=None):
        """Put item into the queue. If optional args block is true and timeout is None (the default), block if necessary until a free slot is available. If timeout is a positive number, it blocks at most timeout seconds and raises the Full exception if no free slot was available within that time. Otherwise (block is false), put an item on the queue if a free slot is immediately available, else raise the Full exception (timeout is ignored in that case)."""
        ready = False
        if self._ready_to_send():
            ready = True

        if self.maxsize > 0 and self.size >= self.maxsize:
            ready = False

        if not block and not ready:
            raise threading_queue.Full()

        now = time.time()
        ready = self._ready_to_send(timeout)
        while self.maxsize > 0 and self.size >= self.maxsize:
            if time.time() - now > timeout:
                raise threading_queue.Full()
            time.sleep(0.05)
            ready = self._ready_to_send()

        if not ready:
            raise threading_queue.Full()

        # TBD: check for safety on pickle size:
        # large objects should be sent via memory buffer
        # (which will likely be implemented in a fullQueue
        # - which can be produced and consumed by
        # arbitrary interpreters, before finished up here)
        self.pipe.send(pickle.dumps(item))
        self._size += 1

    @_child_only
    def get(self, block=True, timeout=None):
        """Remove and return an item from the queue. If optional args block is true and timeout is None (the default), block if necessary until an item is available. If timeout is a positive number, it blocks at most timeout seconds and raises the Empty exception if no item was available within that time. Otherwise (block is false), return an item if one is immediately available, else raise the Empty exception (timeout is ignored in that case)."""
        # TBD: many fails. fix
        return pickle.load(self.pipe)

    """Two methods are offered to support tracking whether enqueued tasks have been fully processed by daemon consumer interpreters
    """

    @_child_only
    def task_done(self):
        """Indicate that a formerly enqueued task is complete. Used by queue consumer threads. For each get() used to fetch a task, a subsequent call to task_done() tells the queue that the processing on the task is complete.

        If a join() is currently blocking, it will resume when all items have been processed (meaning that a task_done() call was received for every item that had been put() into the queue).

        Raises a ValueError if called more times than there were items placed in the queue.
        """
        pass

    @_parent_only
    def join(self):
        """Blocks until all items in the queue have been gotten and processed.

        The count of unfinished tasks goes up whenever an item is added to the queue. The count goes down whenever a consumer thread calls task_done() to indicate that the item was retrieved and all work on it is complete. When the count of unfinished tasks drops to zero, join() unblocks.
        """
        pass

    def __del__(self):
        self.pipe.close()


class _QueueReturnOpcodes:
    NOP = 0
    BUILD_PIPE = 1
    ACK_ELEMENT = 2
    SENT_ITEM = 3


class Queue(_ABSQueue):
    def __init__(self, size=None):
        self.size = size
        self._buffer = LockableBoard()
        self._signal_pipe = _SimplexPipe()
        self._parent_interp = get_current()
        self._post_init()

    @property
    def endpoints(self):
        return []

    @_parent_only
    @non_reentrant
    def _dispatch_return_opcode(self, *args):
        pass

    @property
    def mode(self):
        return (
            _InstMode.parent
            if get_current() == self._parent_interp
            else _InstMode.child
        )

    def __getstate__(self):
        ns = self.__dict__.copy()
        del ns["_data"]
        return ns

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._post_init()

    def _post_init(self):
        self._data = deque(maxlen=self.size)

    @_child_only
    def _child_post_init(self):
        ...

    def _fetch(self):
        """
        _inner function - must be called only from .get()
        """

        # FIXME: This code likely needs a lock -
        origin_pipe = self._signal_pipe
        tmp = self._buffer.fetch_item()
        if not tmp:
            if self._buffer._items_closed_interpreters > 0:
                self._buffer._items_closed_interpreters -= 1
                origin_pipe.read(1)
            return
        # Only the winner of the race to fetch object, consumes a ready byte on
        # on signaler pipe. (Byte is there - indicating an object
        # had been posted on the board - it is detected in the select
        # inside the get method.
        origin_pipe.read(1)
        _, obj = tmp
        self._data.append(obj)

    def get(self, block=True, timeout=None):
        if not block:
            # Python queue.Queue does not error here, so we don't as well:
            #if timeout not in (None, 0):
            #    raise TypeError("Can't specify a timeout with a non-blocking call")
            timeout = 0

        #if not block:
            #timeout = 0
        #elif timeout is None:
            #timeout = _DEFAULT_BLOCKING_TIMEOUT
        try:
            if self._signal_pipe.select(timeout=timeout):
                self._fetch()
        except TimeoutError:
            if block:
                raise

        if self._data:
            return self._data.popleft()
        if not block:
            raise threading_queue.Empty()
        raise TimeoutError()

    def put(self, item, block=True, timeout=None):
        if block and not timeout:
            timeout = _DEFAULT_BLOCKING_TIMEOUT
        index, item = self._buffer.new_item(item)
        try:
            self._signal_pipe.select_for_write(timeout=timeout)
            self._signal_pipe.write(b"\x01")

        except (ResourceBusyError, TimeoutError):
            del self._buffer[index]
            raise

    def __repr__(self):
        return f"{self.__class__.__name__}(size={self.size})"
