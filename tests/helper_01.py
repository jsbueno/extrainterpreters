import time

# separate file with no other imports.
# we have a problem because sub-interpreters do not allow arbitrary imports


time_res = 0.02

def to_run_remotely():
    import time
    print("oi noís")
    time.sleep(time_res * 2)
    print("oi noís de novo")
    return 42
