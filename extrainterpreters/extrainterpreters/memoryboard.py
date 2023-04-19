import os
import tempfile
import mmap

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
