import time
# uncomment the next line to witness general mayhen:
# import pytest

# separate file with no other imports.
# we have a problem because sub-interpreters do not allow arbitrary imports

# (pytest itself is one offender: things crash big way if
# one tries to import pytest in the subinterpreter)

time_res = 0.02

def to_run_remotely():
    import time
    time.sleep(time_res * 2)
    return 42

def big_return_payload():
    return b"\x00" * 2_000_001
