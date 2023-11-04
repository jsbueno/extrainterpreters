from datetime import datetime
from selectors import DefaultSelector
from types import MethodType
from weakref import WeakValueDictionary
import time
import warnings


import extrainterpreters


# Unified selector for all Pipes, queues in a given interpreter:

FILE_WATCHER = DefaultSelector()


class EISelector:
    # A Per interpreter Selector instance which will
    # orchestrate the pipe-data availability and callbacks
    # of all pipe and queue instances.
    def __init__(self):
        self.selector = DefaultSelector()
        self.select_depth = 0

    def register(self, file, event, callback):
        try:
            self.selector.register(file, event, (callback,))
        except KeyError:
            pass
        else:
            return
        # we are adding another callback to the same pair
        # of file/event.
        # no need to worry about changing masks, as we
        # are using pipes, and each fd is either ro or wo
        previous_key = self.selector.get_key(file)
        # check if callback is the same:
        is_method = isinstance(callback, MethodType)
        for reg_callback in previous_key.data:
            if (
                reg_callback is callback
                or isinstance(reg_callback, MethodType)
                and is_method
                and reg_callback.__self__ is callback.__self__
                and reg_callback.__func__ is callback.__func__
            ):
                break
        else:
            new_data = previous_key.data + (callback,)
            self.selector.modify(file, event, new_data)

    def select(self, timeout=None, target_fd=None):
        """calls the O.S. selector method and calls back any
        callables in `data` for all given keys

        The obserbality will end when the first of
        the registered callbacks for this instance - unless
        "target_fd" is given, in that case, timeout is
        respected for that file descritor alone

        """

        # mechanism to prevent called callbacks to be re-entered
        # (a callback in a queue can call methods in the associated
        # pipe which will try to select again: so _this_ method
        # is re-enterable.
        self.select_depth += 1
        if self.select_depth == 1:
            self.entered_callbacks = set()

        start_time = time.monotonic()
        ellapsed = ellapsed_in_step = 0
        adjusted_timeout = timeout
        target_fd_reached = False
        while (target_fd is None or not target_fd_reached) and (
            timeout is None or ellapsed <= timeout
        ):
            x = self.selector.select(adjusted_timeout)

            for key, events in x:
                for callback in key.data:
                    if callback in self.entered_callbacks:
                        continue
                    self.entered_callbacks.add(callback)
                    if key.fd == target_fd:
                        target_fd_reached = True
                    try:
                        callback(key)
                    except Exception as err:
                        warnings.warn(
                            f"Error in select callback {getattr(callback, '__qualname__', callback)}: {err}"
                        )
                    finally:
                        self.entered_callbacks.remove(callback)
            ellapsed_in_step = time.monotonic() - (start_time + ellapsed)
            ellapsed += ellapsed_in_step
            if timeout is not None:
                adjusted_timeout -= ellapsed_in_step

        self.select_depth -= 1

    def unregister(self, file):
        self.selector.unregister(file)

    # TBD: think of a mechanism to ensure resources of gone objects are unregistered.


# singleton:
EISelector = EISelector()

PIPE_REGISTRY = WeakValueDictionary()


def register_pipe(keys, instance):
    """Not for public use.

    A registry so that a given Python object using process-wide resources
    (like file descriptors) is effectivelly a "singleton" in the same interpreter
    (each instance sharing the same file descriptors will also share the same
    memory buffers)
    """
    PIPE_REGISTRY[keys] = instance
    return keys
