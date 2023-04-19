import sys
from pathlib import Path
from functools import partial

from extrainterpreters import _memoryboard

def test_get_address_works():
    obj = bytes(b"\xaa" * 1000)
    address, size = _memoryboard.address_and_size(obj)
    assert size == 1000
    assert address == id(obj) + sys.getsizeof(obj) - size - 1


def test_remote_memory_works():
    obj = bytearray(b"\xaa" * 1000)
    board = _memoryboard.remote_memory(*_memoryboard.address_and_size(obj))
    assert len(board) == 1000
    assert board[0] == 0xaa
    board[0] = 42
    assert obj[0] == 42


def test_remote_memory_works_across_interpreters():
    from extrainterpreters import interpreters
    memory_board_path = str(Path(_memoryboard.__file__).parent)

    interp = interpreters.create() # low level PEP 554 handle
    try:
        run = partial(interpreters.run_string, interp)

        run(f"""import sys;sys.path.insert(0, "{memory_board_path}")""")
        run(f"""import _memoryboard""")

        obj = bytearray(b"\xaa" * 1000)
        mem_data = _memoryboard.address_and_size(obj)
        run(f"""board = _memoryboard.remote_memory(*{mem_data})""")
        run("""board[0] = 42""")
        assert obj[0] == 42
    finally:
        interpreters.destroy(interp)




