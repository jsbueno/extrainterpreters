from extrainterpreters.queue import StructBase, Field

import pytest


def test_struct_size():
    class Struct(StructBase):
        data_offset = Field(2)
        length = Field(4)
    assert Struct._size == 6

def test_struct_works():
    class Struct(StructBase):
        data_offset = Field(2)
        length = Field(4)

    assert Struct._size == 2 + 4
    s = Struct._from_data(bytearray(b"\x00" * 6 ), 0)
    s.data_offset = 1000
    s.length = 2_000_000

    assert s.data_offset == 1000
    assert s.length == 2_000_000


def test_struct_with_offset():
    class Struct(StructBase):
        data_offset = Field(2)
        length = Field(4)

    assert Struct._size == 2 + 4
    s = Struct._from_data(bytearray(b"\x00" * 16 ), 10)
    s.data_offset = 1000
    s.length = 2_000_000

    assert s.data_offset == 1000
    assert s.length == 2_000_000


def test_struct_detach():
    class Struct(StructBase):
        data_offset = Field(2)
        length = Field(4)

    s = Struct._from_data(data:=bytearray(b"\x00" * 16 ), 10)
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

    s = Struct(data_offset=1000, length=2_000_000)
    assert s.data_offset == 1000
    assert s.length == 2_000_000



def test_struct_bytes():
    class Struct(StructBase):
        data_offset = Field(2)

    s = Struct._from_data(bytearray(b"ZZ"))

    assert s._bytes == b"ZZ"
