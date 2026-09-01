import mmap
import os
import threading
from typing import Dict, Protocol, Union

from constants import PAGE_SIZE


class Pager(Protocol):
    """
    Abstract interface for page-based storage backends.
    """

    def read_page(self, page_id: int) -> bytearray:
        """
        Reads a fixed-size page from storage.
        """
        pass

    def write_page(self, page_id: int, data: Union[bytes, bytearray]) -> None:
        """
        Writes data into a specific page boundary.
        """
        pass

    def allocate_page(self) -> int:
        """
        Expands storage by one page and returns its ID.
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

    def __init__(self, page_size: int = PAGE_SIZE) -> None:
        self.page_size = page_size
        self.pages: Dict[int, bytearray] = {0: bytearray(self.page_size)}
        self.lock = threading.Lock()
        self.next_page_id = 1

    def read_page(self, page_id: int) -> bytearray:
        return bytearray(self.pages.get(page_id, bytearray(self.page_size)))

    def write_page(self, page_id: int, data: Union[bytes, bytearray]) -> None:
        self.pages[page_id] = bytearray(data[:self.page_size].ljust(self.page_size, b'\x00'))

    def allocate_page(self) -> int:
        with self.lock:
            page_id = self.next_page_id

            self.next_page_id += 1
            self.pages[page_id] = bytearray(self.page_size)

            return page_id

    def sync(self) -> None:
        pass

    def close(self) -> None:
        pass


class MmapPager(Pager):
    """
    Real memory-mapped file backend for durable data persistence.
    """

    def __init__(self, filepath: str, page_size: int = PAGE_SIZE) -> None:
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

    def sync(self) -> None:
        """
        Durably syncs memory-mapped changes back to the physical file.
        """

        self.mmap.flush()

    def close(self) -> None:
        self.mmap.close()
        self.file.close()
