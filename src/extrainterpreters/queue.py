import os
import pickle
import selectors
import time
import warnings
from functools import wraps
from textwrap import dedent as D

import queue as threading_queue

from .utils import guard_internal_use, StructBase, Field, _InstMode


class Pipe:
    """Full Duplex Pipe class.
    """
    def __init__(self):
        from . import interpreters
        self.originator_fds = os.pipe()
        self.counterpart_fds = os.pipe()
        self._all_fds = self.originator_fds + self.counterpart_fds
        self._post_init()
        self._bound_interp = int(interpreters.get_current())

    # These guys are cute!
    # A Pipe unpickled in another interpreter
    # is automatically promoted to be "the other end"
    # of the Pipe that was pickled in parent interpreter
    def __getstate__(self):
        state = self.__dict__.copy()
        del state["_selector"]
        return state

    @guard_internal_use
    def __setstate__(self, state):
        from . import interpreters
        current = interpreters.get_current()
        self.__dict__.update(state)
        if interpreters.get_current() == state["_bound_interp"]:
            self._post_init()
        else:
            self.__dict__.update(self.counterpart.__dict__)

    def _post_init(self):
        self.closed = False
        self._selector = selectors.DefaultSelector()
        self._selector.register(self.counterpart_fds[0], selectors.EVENT_READ, self.read_blocking)

    @classmethod
    def counterpart_from_fds(cls, originator_fds, counterpart_fds):
        self = cls.__new__(cls)
        self.originator_fds = originator_fds
        self.counterpart_fds = counterpart_fds
        self._all_fds = ()
        self._post_init()
        return self.counterpart

    @property
    def counterpart(self):
        new_inst = type(self).__new__(type(self))
        new_inst.originator_fds = self.counterpart_fds
        new_inst.counterpart_fds = self.originator_fds
        new_inst._all_fds = ()
        new_inst._post_init()
        return new_inst

    def send(self, data):
        if self.closed:
            warnings.warn("Pipe already closed. No data sent")
            return
        if isinstance(data, str):
            data = data.encode("utf-8")
        os.write(self.originator_fds[1], data)

    def read_blocking(self, amount=4096):
        return os.read(self.counterpart_fds[0], amount)

    def _read_ready(self, timeout=0):
        events = self._selector.select(timeout=timeout)
        if len(events) == 1 and events[0][0].fd == self.counterpart_fds[0]:
            return True
        return False

    def readline(self):
        # needed by pickle.load
        result = []
        read_byte = ...
        while read_byte and read_byte != '\n':
            result.append(read_byte:=self.read(1))
        return b"".join(result)

    def read(self, amount=4096, timeout=0):
        if self.closed:
            warnings.warn("Pipe already closed. Not trying to read anything")
            return b""
        result = b""
        if self._read_ready(timeout):
            return self.read_blocking(amount)
        return b""

    def close(self):
        for fd in getattr(self, "_all_fds", ()):
            try:
                os.close(fd)
            except OSError:
                pass
        self.closed = True
        self._all_fds = ()

    def __del__(self):
        self.close()


def _parent_only(func):
    @wraps(func)
    def wrapper(self, *args, **kw):
        if self.mode != _InstMode.parent:
            raise RuntimeError(f"Invalid queue state: {func.__name__} can only be called on parent interpreter")
        return func(self, *args, **kw)
    return wrapper

def _child_only(func):
    @wraps(func)
    def wrapper(self, *args, **kw):
        if self.mode != _InstMode.child:
            raise RuntimeError(f"Invalid queue state: {func.__name__} can only be called on child interpreter")
        return func(self, *args, **kw)
    return wrapper


class SingleQueue:
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
        from . import interpreters
        self.maxsize = maxsize
        self.pipe = Pipe()
        self.mode = _InstMode.parent
        self.bound_to_interp = int(interpreters.get_current())
        self._size = 0
        self._post_init_parent()

    def _post_init_parent(self):
        self._writing_fd = self.pipe.originator_fds[1]
        self._write_selector = selectors.DefaultSelector()
        self._write_selector.register(self._writing_fd, selectors.EVENT_WRITE, None)

    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop("_write_selector")
        return state

    @guard_internal_use
    def __setstate__(self, state):
        from . import interpreters
        self.__dict__.update(state)
        if self.pipe._bound_interp == interpreters.get_current():
            self._post_init_parent()
        else:
            self.mode = _InstMode.child

    @property
    def size(self):
        # implicit binary protocol:
        # other end of pipe should post, as an unsigned byte,
        # the amount of tasks confirmed with ".task_done()"
        if (amount:=self.pipe.read()):
            for done_count in amount:
                self._size -= done_count
        return self._size


    def qsize(self):
        """Return the approximate size of the queue. Note, qsize() > 0 doesn-’t guarantee that a subsequent get() will not block, nor will qsize() < maxsize guarantee that put() will not block.
        """
        return self.size

    def empty(self):
        """
        Queue.empty()
        Return True if the queue is empty, False otherwise. If empty() returns True it doesn’t guarantee that a subsequent call to put() will not block. Similarly, if empty() returns False it doesn’t guarantee that a subsequent call to get() will not block.
        """
        pass

    def full(self):
        """Return True if the queue is full, False otherwise. If full() returns True it doesn’t guarantee that a subsequent call to get() will not block. Similarly, if full() returns False it doesn’t guarantee that a subsequent call to put() will not block.
        """
        if not self._ready_to_send():
            return True
        return self.size >= self.maxsize

    def _ready_to_send(self, timeout=0):
        ready = self._write_selector.select(timeout=timeout)
        return len(ready) == 1 and ready[0][0].fd == self._writing_fd

    @_parent_only
    def put(self, item, block=True, timeout=None):
        """Put item into the queue. If optional args block is true and timeout is None (the default), block if necessary until a free slot is available. If timeout is a positive number, it blocks at most timeout seconds and raises the Full exception if no free slot was available within that time. Otherwise (block is false), put an item on the queue if a free slot is immediately available, else raise the Full exception (timeout is ignored in that case).
        """

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


    def put_nowait(self, item):
        """Equivalent to put(item, block=False)."""
        return self.put(item, block=False)

    @_child_only
    def get(self, block=True, timeout=None):
        """Remove and return an item from the queue. If optional args block is true and timeout is None (the default), block if necessary until an item is available. If timeout is a positive number, it blocks at most timeout seconds and raises the Empty exception if no item was available within that time. Otherwise (block is false), return an item if one is immediately available, else raise the Empty exception (timeout is ignored in that case).
        """
        # TBD: many fails. fix
        return pickle.load(self.pipe)

    def get_nowait(self):
        """Equivalent to get(False)"""
        return self.get(False)

    """Two methods are offered to support tracking whether enqueued tasks have been fully processed by daemon consumer interpreters
    """

    @_child_only
    def task_done(self):
        """Indicate that a formerly enqueued task is complete. Used by queue consumer threads. For each get() used to fetch a task, a subsequent call to task_done() tells the queue that the processing on the task is complete.

        If a join() is currently blocking, it will resume when all items have been processed (meaning that a task_done() call was received for every item that had been put() into the queue).

        Raises a ValueError if called more times than there were items placed in the queue.
        """
        pass

    _parent_only
    def join(self):
        """Blocks until all items in the queue have been gotten and processed.

        The count of unfinished tasks goes up whenever an item is added to the queue. The count goes down whenever a consumer thread calls task_done() to indicate that the item was retrieved and all work on it is complete. When the count of unfinished tasks drops to zero, join() unblocks.
        """
        pass

    def __del__(self):
        self.pipe.close()




class MultiplexEnd: #(_QueueChannelBase):

    def __init__(self, pipe):
        pass

    @_child_only
    def get(self, block=True, timeout=None):
        """retrieves a controled lockable block"""

