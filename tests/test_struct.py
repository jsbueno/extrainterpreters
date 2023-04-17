from extrainterpreters import StructBase, Field

import pytest


def test_struct_works():
    class Struct(StructBase):
        data_offset = Field(2)
        length = Field(4)

    assert Struct.size == 2 + 4
    s = Struct(bytearray(b"\x00" * 6 ), 0)
    s.data_offset = 1000
    s.length = 2_000_000

    assert s.data_offset == 1000
    assert s.length == 2_000_000


def test_struct_with_offset():
    class Struct(StructBase):
        data_offset = Field(2)
        length = Field(4)

    assert Struct.size == 2 + 4
    s = Struct(bytearray(b"\x00" * 16 ), 10)
    s.data_offset = 1000
    s.length = 2_000_000

    assert s.data_offset == 1000
    assert s.length == 2_000_000

