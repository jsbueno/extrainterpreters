import sys
from functools import wraps

class _InstMode:
    parent = "parent"
    child = "child"

def guard_internal_use(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        f = sys._getframe().f_back
        if sys.modules["extrainterpreters"].__dict__.get("DEBUG", False):
            pass
        elif not f.f_globals.get("__name__").startswith("extrainterpreters."):
            raise RuntimeError(f"{func.__name__} can only be called from extrainterpreters code, under risk of causing a segmentation fault")
        return func(*args, **kwargs)
    return wrapper



class clsproperty:
    def __init__(self, method):
        self.method = method

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, intance, owner):
        return self.method(owner)

    def __repr__(self):
        return f"clsproperty <{self.name}>"


class Field:
    def __init__(self, bytesize):
        self.size = bytesize

    def _calc_offset(self, owner):
        offset = 0
        for name, obj in owner.__dict__.items():
            if not isinstance(obj, Field):
                continue
            if obj is self:
                return offset
            offset += obj.size

    def __set_name__(self, owner, name):
        self.offset = self._calc_offset(owner)
        self.name = name

    def __get__(self, instance, owner):
        if instance is None:
            return self
        off = instance._offset + self.offset
        return int.from_bytes(instance._data[off: off + self.size], "little")

    def __set__(self, instance, value):
        off = instance._offset + self.offset
        instance._data[off: off + self.size] = value.to_bytes(self.size, "little")


class StructBase:
    """A Struct type class which can attach to offsets in an existing memory buffer

    Currently, only different sized integer fields are implemented.

    Methods and propertys start with "_" just to avoid nae clashes with fields.
    Feel free to use anything here. (use is by subclassing and defining fields)
    """

    slots = ("_data", "_offset")

    def __init__(self, **kwargs):
        self._offset = 0
        self._data = bytearray(b"\x00" * self._size)
        for field_name in self._fields:
            setattr(self, field_name, kwargs.pop(field_name))
        if kwargs:
            raise ValueError(f"Unknown fields {kwargs} passed to {self.__class__.__name__}")

    @classmethod
    def _from_data(cls, _data, _offset=0):
        self = cls.__new__(cls)
        self._data = _data
        self._offset = _offset
        return self

    @clsproperty
    def _fields(cls):
        for k, v in cls.__dict__.items():
            if isinstance(v, Field):
                yield k

    @classmethod
    def _from_values(cls, *args):
        data = bytearray(b"\x00" * cls._size)
        self = cls(data, 0)
        for arg, field_name in zip(args, self._fields):
            setattr(self, field_name, arg)
        return self

    @property
    def _bytes(self):
        return bytes(self._data[self._offset: self._offset + self._size])

    @clsproperty
    def _size(cls):
        size = 0
        for name, obj in cls.__dict__.items():
            if isinstance(obj, Field):
                size += obj.size
        return size

    def _detach(self):
        self._data = self._data[self._offset: self._offset + self._size]
        self._offset = 0

    def _get_offset_for_field(self, field_name):
        offset = 0
        cls = self.__class__
        for name in self._fields:
            if name == field_name:
                return offset
            offset += getattr(cls, name).size
        raise AttributeError("No field named {field_name!r} in provided struct")

    def __repr__(self):
        field_data = []
        for field_name in self._fields:
            field_data.append(f"    {field_name} = {getattr(self, field_name)}")
        return(f"{self.__class__.__name__}:\n{"\n".join(field_data)}\n")

