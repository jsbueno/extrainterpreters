from extrainterpreters.queue import StructBase, Field

import pytest


def test_struct_works():
    class Struct(StructBase):
        data_offset = Field(2)
        length = Field(4)

    assert Struct._size == 2 + 4
    s = Struct(bytearray(b"\x00" * 6 ), 0)
    s.data_offset = 1000
    s.length = 2_000_000

    assert s.data_offset == 1000
    assert s.length == 2_000_000


def test_struct_with_offset():
    class Struct(StructBase):
        data_offset = Field(2)
        length = Field(4)

    assert Struct._size == 2 + 4
    s = Struct(bytearray(b"\x00" * 16 ), 10)
    s.data_offset = 1000
    s.length = 2_000_000

    assert s.data_offset == 1000
    assert s.length == 2_000_000


def test_struct_detach():
    class Struct(StructBase):
        data_offset = Field(2)
        length = Field(4)

    s = Struct(data:=bytearray(b"\x00" * 16 ), 10)
    s.data_offset = 1000
    s.length = 2_000_000

    assert data is s._data
    s._detach()
    assert data is not s._data
    assert s._offset == 0
    assert s.data_offset == 1000
    assert s.length == 2_000_000


def test_struct_from_values():
    class Struct(StructBase):
        data_offset = Field(2)
        length = Field(4)

    s = Struct._from_values(1000, 2_000_000)
    assert s.data_offset == 1000
    assert s.length == 2_000_000



def test_struct_bytes():
    class Struct(StructBase):
        data_offset = Field(2)

    s = Struct(bytearray(b"ZZ"))

    assert s._bytes == b"ZZ"
