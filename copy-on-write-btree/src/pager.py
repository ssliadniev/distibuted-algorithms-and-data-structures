import mmap
import os
import struct
import threading
from typing import Dict, Protocol, Union

from constants import DEFAULT_PAGE_SIZE


class Pager(Protocol):
    """
    Abstract interface for page-based storage backends.
    """
    page_size: int

    def read_page(self, page_id: int) -> bytearray:
        """
        Reads a fixed-size page from storage.
        """
        pass

    def write_page(self, page_id: int, data: Union[bytes, bytearray]) -> None:
        """
        Writes data into a specific page boundary, strictly enforcing size limits.
        """
        pass

    def allocate_page(self) -> int:
        """
        Expands storage by one page and returns its ID.
        """
        pass

    def publish_root(self, root_page_id: int) -> None:
        """
        Atomically updates exactly the 8-byte root page ID in the header.
        """
        pass

    def publish_freelist_head(self, freelist_head_id: int) -> None:
        """
        Atomically updates exactly the 8-byte free-list head ID in the header.
        """
        pass

    def sync(self) -> None:
        """
        Durably flushes cached operations to disk.
        """
        pass

    def close(self) -> None:
        """
        Cleans up resources and closes the backend.
        """
        pass


class InMemoryPager(Pager):
    """
    In-memory storage backend, primarily for unit testing.
    """

    def __init__(self, page_size: int = DEFAULT_PAGE_SIZE) -> None:
        self.page_size = page_size
        self.pages: Dict[int, bytearray] = {0: bytearray(self.page_size)}
        self.lock = threading.Lock()
        self.next_page_id = 1

    def read_page(self, page_id: int) -> bytearray:
        return bytearray(self.pages.get(page_id, bytearray(self.page_size)))

    def write_page(self, page_id: int, data: Union[bytes, bytearray]) -> None:
        if len(data) > self.page_size:
            raise ValueError(f"Serialized page exceeds page size: {len(data)} > {self.page_size}")

        exact_data = bytearray(data[:self.page_size].ljust(self.page_size, b'\x00'))
        self.pages[page_id] = exact_data

    def allocate_page(self) -> int:
        with self.lock:
            page_id = self.next_page_id

            self.next_page_id += 1
            self.pages[page_id] = bytearray(self.page_size)

            return page_id

    def publish_root(self, root_page_id: int) -> None:
        self.pages[0][1:9] = struct.pack("<Q", root_page_id)

    def publish_freelist_head(self, freelist_head_id: int) -> None:
        self.pages[0][9:17] = struct.pack("<Q", freelist_head_id)

    def sync(self) -> None:
        pass

    def close(self) -> None:
        pass


class MmapPager(Pager):
    """
    Real memory-mapped file backend for durable data persistence.
    """

    def __init__(self, filepath: str, page_size: int = DEFAULT_PAGE_SIZE) -> None:
        self.page_size = page_size
        self.filepath = filepath
        self.file = open(filepath, "a+b")

        if os.path.getsize(filepath) == 0:
            self.file.write(b'\x00' * self.page_size)
            self.file.flush()

        self.mmap = mmap.mmap(self.file.fileno(), 0)
        self.lock = threading.Lock()

    def read_page(self, page_id: int) -> bytearray:
        offset = page_id * self.page_size
        return bytearray(self.mmap[offset: offset + self.page_size])

    def write_page(self, page_id: int, data: Union[bytes, bytearray]) -> None:
        if len(data) > self.page_size:
            raise ValueError(f"Serialized page exceeds page size: {len(data)} > {self.page_size}")

        offset = page_id * self.page_size
        exact_data = data[:self.page_size].ljust(self.page_size, b'\x00')

        self.mmap[offset: offset + self.page_size] = exact_data

    def allocate_page(self) -> int:
        with self.lock:
            current_size = self.mmap.size()
            page_id = current_size // self.page_size

            self.file.seek(current_size + self.page_size - 1)
            self.file.write(b'\x00')
            self.file.flush()

            self.mmap.resize(current_size + self.page_size)
            return page_id

    def publish_root(self, root_page_id: int) -> None:
        self.mmap[1:9] = struct.pack("<Q", root_page_id)

    def publish_freelist_head(self, freelist_head_id: int) -> None:
        self.mmap[9:17] = struct.pack("<Q", freelist_head_id)

    def sync(self) -> None:
        """
        Durably syncs memory-mapped changes back to the physical file.
        """

        self.mmap.flush()
        os.fsync(self.file.fileno())

    def close(self) -> None:
        self.mmap.close()
        self.file.close()
