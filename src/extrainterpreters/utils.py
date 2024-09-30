import struct
import sys
from functools import wraps


class ResourceBusyError(RuntimeError):
    pass


class _InstMode:
    parent = "parent"
    child = "child"
    zombie = "zombie"


def guard_internal_use(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        f = sys._getframe().f_back
        # This can be called at interpreter shutdown, and extrainterpreters
        # may no longer be in sys.modules:
        extrainterpreters = sys.modules.get("extrainterpreters", None)
        if extrainterpreters and extrainterpreters.__dict__.get("DEBUG", False):
            pass
        elif not f.f_globals.get("__name__").startswith("extrainterpreters."):
            raise RuntimeError(
                f"{func.__name__} can only be called from extrainterpreters code, under risk of causing a segmentation fault"
            )
        return func(*args, **kwargs)

    return wrapper


from ._memoryboard import _remote_memory, _address_and_size, _atomic_byte_lock

_remote_memory = guard_internal_use(_remote_memory)
_address_and_size = guard_internal_use(_address_and_size)
_atomic_byte_lock = guard_internal_use(_atomic_byte_lock)


class clsproperty:
    def __init__(self, method):
        self.method = method

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, intance, owner):
        return self.method(owner)

    def __repr__(self):
        return f"clsproperty <{self.name}>"


class RawField:
    def __init__(self, bytesize):
        self.size = bytesize

    def _calc_offset(self, owner):
        offset = 0
        for name, obj in owner.__dict__.items():
            if not isinstance(obj, RawField):
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
        return instance._data[off : off + self.size]

    def __set__(self, instance, value):
        off = instance._offset + self.offset
        instance._data[off : off + self.size] = value


class Field(RawField):  # int field
    def __get__(self, instance, owner):
        value = super().__get__(instance, owner)
        if not isinstance(value, (bytes, bytearray, memoryview)):
            return value
        return int.from_bytes(value, "little")

    def __set__(self, instance, value):
        value = value.to_bytes(self.size, "little")
        super().__set__(instance, value)


class DoubleField(RawField):
    def __init__(self):
        super().__init__(8)

    def __get__(self, instance, owner):
        value = super().__get__(instance, owner)
        if not isinstance(value, (bytes, bytearray)):
            return value
        return struct.unpack(
            "d",
            value,
        )[0]

    def __set__(self, instance, value):
        value = struct.pack("d", value)
        super().__set__(instance, value)


class StructBase:
    """A Struct type class which can attach to offsets in an existing memory buffer

    Currently, only different sized integer fields are implemented.

    Methods and propertys start with "_" just to avoid nae clashes with fields.
    Feel free to use anything here. (use is by subclassing and defining fields)
    """

    slots = ("_data", "_offset")

    def __init__(self, **kwargs):
        self._offset = 0
        self._data = bytearray(self._size)
        for field_name in self._fields:
            setattr(self, field_name, kwargs.pop(field_name))
        if kwargs:
            raise ValueError(
                f"Unknown fields {kwargs} passed to {self.__class__.__name__}"
            )

    @classmethod
    def _from_data(cls, _data, _offset=0):
        self = cls.__new__(cls)
        self._data = _data
        self._offset = _offset
        return self

    @clsproperty
    def _fields(cls):
        for k, v in cls.__dict__.items():
            if isinstance(v, RawField):
                yield k

    @property
    def _bytes(self):
        return bytes(self._data[self._offset : self._offset + self._size])

    @clsproperty
    def _size(cls):
        size = 0
        for name, obj in cls.__dict__.items():
            if isinstance(obj, RawField):
                size += obj.size
        return size

    def _detach(self):
        self._data = self._data[self._offset : self._offset + self._size]
        self._offset = 0

    @classmethod
    def _get_offset_for_field(cls, field_name):
        field = getattr(cls, field_name)
        return field._calc_offset(cls)

    def _push_to(self, data, offset=0):
        """Paste struct data into a new buffer given by data, offset

        Returns a new instance pointing to the data in the new copy.
        """
        data[offset: offset + self._size] = self._bytes
        return self._from_data(data, offset)

    def __repr__(self):
        field_data = []
        for field_name in self._fields:
            field_data.append(f"    {field_name} = {getattr(self, field_name)}")
        field_str = "\n".join(field_data)
        return f"{self.__class__.__name__}:\n{field_str}\n"


def non_reentrant(func):
    depth = 0

    @wraps(func)
    def wrapper(*args, **kwargs):
        nonlocal depth
        if depth == 1:
            return
        depth += 1
        try:
            result = func(*args, **kwargs)
        finally:
            depth -= 1
        return result

    return wrapper
