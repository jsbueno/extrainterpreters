import atexit
import sys
import weakref
from textwrap import dedent as D

try:
    import interpreters
except ImportError:
    try:
        import _xxsubinterpreters as interpreters
    except ImportError:
        raise ImportError(D("""
            interpreters module not available in this Python install.
            If you are early to it (before 3.12 beta), you need to build the
            per-interpreter-gil-new branch from
            https://github.com/ericsnowcurrently/cpython.git
            """))


BFSZ = 10_000_000

running_interpreters = weakref.WeakSet()

from .memoryboard import ProcessBuffer, RemoteArray
from .base_interpreter import BaseInterpreter
from .queue import Pipe, SingleQueue, Queue
from .simple_interpreter import SimpleInterpreter as Interpreter


get_current = interpreters.get_current


def list_all():
    """Returns a weakset with all active Interpreter instances

    The idea is to have the same API available in "interpreters" working
    for the higher level objects. But converting the weakset to a full list
    could result in dangling references we don't want.
    """
    return running_interpreters


def destroy_dangling_interpreters():
    for interp in list(running_interpreters):
        try:
            interp.close()
        except Exception:
            import warnings
            warnings.warn(D(f"""\
                Failed to close dangling {interp}. Data exchange file
                {interp.buffer.fname} may not have been erased.
                """))
        else:
            print(f"{interp} closed at main interpreter exit", file=sys.stderr)


# the new "if __name__ == "__main__":
if interpreters.get_current() == interpreters.get_main():
    atexit.register(destroy_dangling_interpreters)


