from extrainterpreters.utils import StructBase, Field, DoubleField

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
    s = Struct._from_data(bytearray(b"\x00" * 6), 0)
    s.data_offset = 1000
    s.length = 2_000_000

    assert s.data_offset == 1000
    assert s.length == 2_000_000


def test_struct_with_offset():
    class Struct(StructBase):
        data_offset = Field(2)
        length = Field(4)

    assert Struct._size == 2 + 4
    s = Struct._from_data(bytearray(b"\x00" * 16), 10)
    s.data_offset = 1000
    s.length = 2_000_000

    assert s.data_offset == 1000
    assert s.length == 2_000_000


def test_struct_detach():
    class Struct(StructBase):
        data_offset = Field(2)
        length = Field(4)

    s = Struct._from_data(data := bytearray(b"\x00" * 16), 10)
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

def test_struct_push_to():
    class Struct(StructBase):
        data_offset = Field(2)
        length = Field(4)

    s = Struct(data_offset=1000, length=2_000_000)

    data = bytearray(Struct._size)
    n = s._push_to(data, 0)
    assert n.data_offset == 1000
    assert n.length == 2_000_000


def test_struct_bytes():
    class Struct(StructBase):
        data_offset = Field(2)

    s = Struct._from_data(bytearray(b"ZZ"))

    assert s._bytes == b"ZZ"


def test_struct_doublefield():
    class Struct(StructBase):
        value = DoubleField()

    s = Struct(value=1.25)
    assert s._data == bytearray(b"\x00\x00\x00\x00\x00\x00\xf4?")
    assert s.value == 1.25
