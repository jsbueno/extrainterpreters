import sys
from pathlib import Path
from functools import partial

import pytest

from extrainterpreters import memoryboard


@pytest.mark.parametrize(
    "func",
    [
        memoryboard._address_and_size,
        memoryboard._remote_memory,
        memoryboard._atomic_byte_lock,
    ],
)
def test_lowlevel_func_guarded_from_out_of_package_call(func):
    with pytest.raises(RuntimeError):
        func()


def test_get_address_works(lowlevel):
    obj = bytes(b"\xaa" * 1000)
    address, size = memoryboard._address_and_size(obj)
    assert size == 1000
    assert address == id(obj) + sys.getsizeof(obj) - size - 1


def test_remote_memory_works(lowlevel):
    obj = bytearray(b"\xaa" * 1000)
    board = memoryboard._remote_memory(*memoryboard._address_and_size(obj))
    assert len(board) == 1000
    assert board[0] == 0xAA
    board[0] = 42
    assert obj[0] == 42


def test_remote_memory_works_across_interpreters(lowlevel):
    from extrainterpreters import interpreters

    memory_board_path = str(Path(memoryboard.__file__).parent)

    interp = interpreters.create()  # low level PEP 554 handle
    try:
        run = partial(interpreters.run_string, interp)

        run(f"""import extrainterpreters; extrainterpreters.DEBUG=True""")
        run(f"""from extrainterpreters import memoryboard""")

        obj = bytearray(b"\xaa" * 1000)
        mem_data = memoryboard._address_and_size(obj)
        run(f"""board = memoryboard._remote_memory(*{mem_data})""")
        run("""board[0] = 42""")
        assert obj[0] == 42
    finally:
        interpreters.destroy(interp)


def test_atomiclock_works_only_when_indicator_at_0(lowlevel):
    buffer = bytearray(
        [
            0,
        ]
    )
    address, _ = memoryboard._address_and_size(buffer)
    assert memoryboard._atomic_byte_lock(address)
    assert buffer[0] == 1
    assert not memoryboard._atomic_byte_lock(address)
    buffer[0] = 2
    assert not memoryboard._atomic_byte_lock(address)


def test_atomiclock_locks(lowlevel):
    import time, random, threading

    buffer = bytearray(
        [
            0,
        ]
    )
    address, _ = memoryboard._address_and_size(buffer)
    counter = 0

    def increment():
        nonlocal counter
        while not memoryboard._atomic_byte_lock(address):
            time.sleep(0.00005)
        old_counter = counter
        time.sleep(random.random() % 0.1)
        counter = old_counter + 1
        buffer[0] = 0

    threads = [threading.Thread(target=increment) for i in range(20)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert counter == 20
