"""
Author: João S. O. Bueno
Special Thanks: Eric Snow for his work on subinterpreters and a per-interpreter GIL

"""

import atexit
import sys
import weakref
from textwrap import dedent as D


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


__version__ = "0.2-beta3"


BFSZ = 10_000_000

running_interpreters = weakref.WeakValueDictionary()


class RootInterpProxy:
    intno = id = 0


RootInterpProxy = RootInterpProxy()

running_interpreters[0] = RootInterpProxy

from .utils import ResourceBusyError
from .memoryboard import ProcessBuffer, RemoteArray
from .base_interpreter import BaseInterpreter
from .queue import SingleQueue, Queue
from .simple_interpreter import SimpleInterpreter as Interpreter


get_current = interpreters.get_current


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
