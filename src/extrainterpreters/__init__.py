"""
Author: João S. O. Bueno
Special Thanks: Eric Snow for his work on subinterpreters and a per-interpreter GIL

"""

import atexit
import sys
import weakref
from textwrap import dedent as D

try:
    # Python 3.14+
    from concurrent.interpreters import _interpreters as interpreters
except ImportError:

    try:
        # PEP 734 Python 3.13+
        import _interpreters as interpreters
    except ImportError:
        try:
            # Draft for PEP 554 - Python 3.12.x
            import _xxsubinterpreters as interpreters
        except ImportError:
            raise ImportError(
                D(
                    """
                interpreters module not available in this Python install.
                If you are early to it (before 3.12 beta), you need to build an up-to-date
                cPython 3.12 or later.
                """
                )
            )


# Early declarations to avoid circular imports:

__version__ = "0.3.0"

# Crude 10MB buffer per interpreter to serialize objects back and forth:
BFSZ = 10_000_000
# (some of the classes developerd since won't even need this anymore - but
# I have to dive in and make this configurable/optional)

running_interpreters = weakref.WeakValueDictionary()


class RootInterpProxy:
    intno = id = 0


RootInterpProxy = RootInterpProxy()

running_interpreters[0] = RootInterpProxy

def get_current():
    id_ = interpreters.get_current()
    return int(id_) if not isinstance(id_, tuple) else id_[0]


def raw_list_all():
    # .list_all changed to return tuples between Python 3.12 and 3.13
    ids = interpreters.list_all()
    if not isinstance(ids[0], tuple):
        return ids
    return [tuple_id[0] for tuple_id in interpreters.list_all()]

if not hasattr(interpreters, "RunFailedError"):
    # exception was removed in Python 3.13, but we need to
    # have it present in some except clauses while  3.12 is supported:
    interpreters.RunFailedError = RuntimeError

from .utils import ResourceBusyError
from .memoryboard import ProcessBuffer, RemoteArray
from .base_interpreter import BaseInterpreter
from .queue import SingleQueue, Queue
from .simple_interpreter import SimpleInterpreter as Interpreter
from .lock import Lock, RLock


def list_all():
    """Returns a list with all active subinterpreter instances"""
    return [interp for id_, interp in running_interpreters.items() if id_ != 0]


def destroy_dangling_interpreters():
    print(list(running_interpreters.values()))
    for interp in list(running_interpreters.values()):
        if interp is RootInterpProxy:
            continue
        try:
            interp.close()
        except Exception as error:
            import warnings

            warnings.warn(
                D(
                    f"""\
                Failed to close dangling {interp}. Data exchange file
                {interp.buffer} may not have been erased: {error}
                """
                )
            )
        else:
            print(f"{interp} closed at main interpreter exit", file=sys.stderr)


# the new "if __name__ == "__main__":
if interpreters.get_current() == interpreters.get_main():
    atexit.register(destroy_dangling_interpreters)
