import os
import tempfile
import mmap
import threading

from collections.abc import MutableSequence

from . import _memoryboard

class _ProcessBuffer:
    def __init__(self, size=10_000_000, ranges: dict[int,str]|None=None):
        if ranges is None:
            ranges = {
                0: "command_area",
                4096: "send_data",
                (size // 5 * 4): "return_data"
            }

        self.size = size
        self.ranges = ranges
        self.nranges = {v:k for k, v in ranges.items()}
        self._init_range_sizes()
        self.fname = tempfile.mktemp()
        self.file = open(self.fname, "w+b")
        self.file.write(b"\x00" * self.size)
        self.file.flush()
        self.fileno = self.file.fileno()
        self.map = mmap.mmap(self.fileno, self.size)

    def _init_range_sizes(self):
        prev_range = ""
        last_range_start = 0
        self.range_sizes = {}
        for i, (range_name, offset) in enumerate(self.nranges.items()):
            if i:
                self.range_sizes[prev_range] = offset - self.nranges[prev_range]
            prev_range = range_name
            if offset < last_range_start:
                raise ValueError("Buffer Range window starts must be in ascending order")
            last_range_start = offset

    def __del__(self):
        # Should be called explicitly by users of the class
        try:
            if self.map:
                self.map.close()
            if self.file:
                self.file.close()
        finally:
            try:
                os.unlink(self.fname)
            except FileNotFoundError:
                pass
        self.map = self.file = None

    def __repr__(self):
        return f"<Mmaped interprocess buffer with {BFSZ:_} bytes>"

@MutableSequence.register
class FileLikeArray:
    __slots__ = ("_cursor", "_lock", "data")


    def __init__(self, data):
        self.data = data
        self._cursor = 0
        self._lock = threading.RLock()

    def __getitem__(self, index):
        return self.data.__getitem__(index)

    def __setitem__(self, index, value):
        return self.data.__setitem__(index, value)

    def __delitem__(self, index):
        raise NotImplementedError()

    def __len__(self):
        return len(self.data)

    def iter(self):
        return iter(data)

    def read(self, n=None):
        with self._lock:
            if n is None:
                n = len(self) - self._cursor
            prev = self._cursor
            self._cursor += n
            return self.data[prev: self._cursor]

    def write(self, content):
        with self._lock:
            if isinstance(content, str):
                content = content.encode("utf-8")
            self.data[self._cursor: self._cursor + len(content)] = content
            self._cursor += len(content)

    def tell(self):
        return self._cursor

    def readline(self):
        # needed by pickle.load
        result = []
        read = 0
        with self._lock:
            cursor = self._cursor
            while read != 0x0a:
                if cursor >= len(self):
                    break
                result.append(read:=self.data[cursor])
                cursor += 1
            self._cursor = self.cursor
        return bytes(result)

    def seek(self, pos):
        self._cursor = pos

    def __repr__(self):
        return f"<self.__class__.__name__ with {len(self)} bytes>"


class ProcessBuffer:
    def __init__(self, size=10_000_000, ranges: dict[int,str]|None=None):
        if ranges is None:
            ranges = {
                0: "command_area",
                4096: "send_data",
                (size // 5 * 4): "return_data"
            }

        self.size = size
        self.ranges = ranges
        self.nranges = {v:k for k, v in ranges.items()}
        self._init_range_sizes()
        self.map = FileLikeArray(bytearray(b"\x00" * size))

    def _init_range_sizes(self):
        prev_range = ""
        last_range_start = 0
        self.range_sizes = {}
        for i, (range_name, offset) in enumerate(self.nranges.items()):
            if i:
                self.range_sizes[prev_range] = offset - self.nranges[prev_range]
            prev_range = range_name
            if offset < last_range_start:
                raise ValueError("Buffer Range window starts must be in ascending order")
            last_range_start = offset

    def _data_for_remote(self):
        return _memoryboard.address_and_size(self.map.data)

    def close(self):
        self.map = None

    def __repr__(self):
        return f"<interprocess buffer with {self.size:_} bytes>"
