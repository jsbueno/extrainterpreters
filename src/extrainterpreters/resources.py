from selectors import DefaultSelector


# Unified selector for all Pipes, queues in a given interpreter:

FILE_WATCHER = DefaultSelector()

def select(timeout=None):
    """Unified per-interpreter resource to be used internally
    by Pipes and queues.

    Registering of os.pipe and othe resources is to be done
    directly in `FILE_WATCHER` - this select call is a thin wrapper
    over FILE_WATCHER.select so that callbacks are executed as needed.
    """
    # call the selector method and calls back any callables in `data` for all given keys
    for key, events in FILE_WATCHER.select(timeout):
        print(key)
        if callable(key.data):
            key.data(key)

# TBD: think of a mechanism to ensure resources of gone objects are unregistered.
