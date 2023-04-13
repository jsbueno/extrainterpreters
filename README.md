# extrainterpreters

Utilities to make use of new 
"interpreters"  module in Python 3.12

Status: Super alpha

Usage: for now one must build cpython 
from Eric Snow at https://github.com/ericsnowcurrently/cpython.git
, branch per-interpreter-gil-new

then, just import "extrainterpreters" and use the `Interpreter` class
as a wrapper to the subinterpreter represented by a single integer
handle created by the new `interpreters` module (that should be 
in Python stdlib as soon as Eric Snow polishes it)

with an `extrainterpreter.Interpreter` instance, one can call:
`inter.run(func_name, args=(), kwargs={})` to run code there in the same
thread, or `inter.run_in_thread` with the same signature.

Both calls allow one to verify the return value with `inter.result()`

A lot of things here are subject to change.
For now, all data exchange takes place in memory mapped
10MB temporary file. (The code is so early there are not even
size checks)

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
As for the data passing mechanism: we will use pickle, no doubt. If 
"mmap" is the best idea, (in contrast with the os.pipe demonstrated
in PEP 554 examples) remain to be seem.

Probably comming up with more than one way to send/get data from other
interpreters as well. The initial implementation is using a pre-allocated
mmaped file.

