import time

# uncomment the next line to witness general mayhen:
# import pytest

# separate file with no other imports.
# we have a problem because sub-interpreters do not allow arbitrary imports

# (pytest itself is one offender: things crash big way if
# one tries to import pytest in the subinterpreter)

time_res = 0.02


def to_run_remotely():
    time.sleep(time_res * 2)
    return 42


def big_return_payload():
    return b"\x00" * 2_000_001


class RemoteClass:
    def __init__(self):
        pass

    @classmethod
    def to_run_remotely_2(cls):
        return 42

    def __call__(self):
        return 23


def main():
    # try to execute function in __main__ file.
    import extrainterpreters

    with extrainterpreters.Interpreter() as interp:
        interp.run_in_thread(to_run_remotely)
        while not interp.done():
            time.sleep(time_res)
        assert interp.result() == 42


if __name__ == "__main__":
    # Test if running a function in the same module in sub-interpreter works.
    # have to work-around pickling functions in the same module, since "__main__" is
    # not importable in the sub-interpreter
    main()

if __name__ == "__main__":
    # extrainterpreters temporarily monkeypathes __name__
    # this second if clause tests if it is reverted.
    print("Remote execution of function in this module worked")
