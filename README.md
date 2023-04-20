# extrainterpreters

Utilities to make use of new 
"interpreters"  module in Python 3.12

Status: alpha

Usage: for now one must build cpython 
from Eric Snow at https://github.com/ericsnowcurrently/cpython.git
, branch per-interpreter-gil-new

then, just import "extrainterpreters" and use the `Interpreter` class
as a wrapper to the subinterpreter represented by a single integer
handle created by the new `interpreters` module (that should be 
in Python stdlib as soon as Eric Snow polishes it)

with an `extrainterpreter.Interpreter` instance, one can call:
`inter.run(func_name, *args, **kwargs})` to run code there in the same
thread: the return value is just returned as long as it is pickleable.
The `inter.run_in_thread` with the same signature, will start a fresh
thead and call the target function there - the Interpreter instance  then should
be pooled in its "done()" method, and once done, the return value
will be available by calling  "result".

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

# Roadmap

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

