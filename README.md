# extrainterpreters

Utilities to make use of new 
"interpreters"  module in Python 3.12

Status: beta

Usage: Works with cPython >= 3.12


This was made available in 2023 as Python 3.12 came out
to allow some experiments and sending objects back and 
forth subinterpreters.

As of the release of Python 3.14, `concurrent.interpreters` is
made a public API, and is capable of several of the things this
project could do - check https://docs.python.org/3.14/library/concurrent.interpreters.html 

Right now I am not updating this project - it maybe that I return to it at some point,
but really, since free-threading has become am option, I am having
a hardtime finding out where a pure Python project could effectively
make use of multiple interpreters.

Still, at the eve of Python 3.14, I am issuing an small update
so that the API as avaliable up to Python 3.13 is imported
from the right place, and the structures in this project work.


## Usage

Just import "extrainterpreters" and use the `Interpreter` class
as a wrapper to the subinterpreter represented by a single integer
handle created by the new `interpreters` module:

```python
import extrainterpreters as ET

import time

def my_function():
    time.sleep(1)
    print("running in other interpreter")
    return 42

interp = ET.Interpreter(target=my_function)
interp.start()
print("running in main interpreter")
time.sleep(1.5)
print(f"computation result: {interp.result()}")

```



## history
PEP 554 Describes a Python interface to make use of Sub-interpreters-
a long living feature of cPython, but only available through C code
embedding Python. While approved, the PEP is not in its final form,
and as of Python 3.13 the `interpreters` module suggested there
is made available as `_interpreters`. (And before that, in Python
3.13, it is available as `_xxsubinterpreters`.

With the implementation of PEP 684 before Python 3.12, using
subinterpreters became a lot more interesting, because
each subinterpreter now has an independent GIL. This means
that different interpreters (running in different threads)
can actually execute Python code in parallel in multiple
CPU cores.

"extrainterpreters" offer a nice API to make use
of this feature before PEP 554 becomes final, and
hopefully, will keep offering value as a nice API wrapper
when it is final. Enjoy!

## forms of use

with an `extrainterpreter.Interpreter` instance, one can call:
`inter.run(func_name, *args, **kwargs})` to run code there in the same
thread: the return value is just returned as long as it is pickleable.
The `inter.run_in_thread` with the same signature, will start a fresh
thead and call the target function there - the Interpreter instance  then should
be pooled in its "done()" method, and once done, the return value
will be available by calling  "result".

Also working is a threading like interface:

```
from extrainterpreters import Interpreter

...

    interp = Interpreter(target=myfunc, args=(...,))
    interp.start()
    ...
    interp.join()

    # bonus: we have return values!

    print(interp.result()
```

It will even work for methods defined on the REPL, so, just
give it a try.

There is a new class in the works which will be able to
run code in the sub-interpreter without depending of the
PEP554 "run_string" (once the setup is done). All processing in the sub-
interpreter will automatically take place in another thread,
and the roadmap is towards having a Future object and
an  `InterpreterPoolExecutor` compatible with the
existing executors in `concurrent.Futures`

A lot of things here are subject to change.
For now, all data exchange takes place in a custom memory
buffer with 10MB (it is a bytearray on  the parent interpreter,
and a memoryview for that on the target)

Data is passed to and back the interpreter using pickle - so,
no black magic trying to "kill an object here" and "make it be born there"
(not even if I intended to, as ctypes is not currently importable on 
sub-interpreters).

The good news is that `pickle` will take care of importing whatever is
needed on the other side, so that a function can run. 

# API and Usage:

In beta stage the suggestion is to use this as one would `threading.Thread` -
see the example above - and do not rely on the provided `Queue` class (seriously
it is broken right now), until some notice about it is given.

# Roadmap

## first the basics:
    we should get Lock, Rlock, Queue and some of the concurrency primitives
    as existing for threads, async and subprocessing working - at that point
    this should be production quality

## second, the bells and whistles

I plan to get this compatible with concurrent.futures.Executor, and have
an easy way to schedule a subinterpreter function execution as an async task.

Also, I should come up with a Queue object to pass data back and forth.
As for the data passing mechanism: we will use pickle, no doubt.

# Architecture
The initial implementation used a pre-allocated mmaped file
to send/get data from other interpreters, until I found out the
mmap call in the sub-interpreter would build a separate mmap
object in a different memory region, and a lot of data copying
was taking place.

Currently, two C methods retrieve the address of an object implementing
the buffer interface, and create a "stand alone" memoryview instance
from the address alone. This address is passed to the subinterpreter
as a string during the setup stage,

