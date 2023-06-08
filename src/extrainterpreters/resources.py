from selectors import DefaultSelector
from types import MethodType
import warnings

from weakref import WeakValueDictionary
from datetime import datetime


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
                reg_callback is callback or
                isinstance(reg_callback, MethodType) and is_method and
                reg_callback.__self__ is callback.__self__ and
                reg_callback.__func__ is callback.__func__
            ):
                break
        else:
            new_data = previous_key.data + (callback, )
            self.selector.modify(file, event, new_data)

    def select(self, timeout=None):
        # call the selector method and calls back any callables in `data` for all given keys

        # mechanism to prevent called callbacks to be re-entered
        # (a callback in a queue can call methods in the associated
        # pipe which will try to select again: so _this_ method
        # is re-enterable.
        self.select_depth += 1
        if self.select_depth == 1:
            self.entered_callbacks = set()
        x = self.selector.select(timeout)
        from . import interpreters
        if interpreters.get_current() != 0:
            with open("bla", "at") as f:
                f.write(f"{datetime.now().isoformat()} - {x}\n\n")
        for key, events in x:
            for callback in key.data:
                if callback not in self.entered_callbacks:
                    self.entered_callbacks.add(callback)
                    try:
                        callback(key)
                    except Exception as err:
                        warnings.warn(f"Error in select callback {err}")
                    finally:
                        self.entered_callbacks.remove(callback)
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


