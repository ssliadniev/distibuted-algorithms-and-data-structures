import os
import sys
import threading
from pathlib import Path

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from src.btree import BTree
from src.constants import DEFAULT_PAGE_SIZE
from src.pager import MmapPager


@pytest.fixture
def temp_mmap_file(tmp_path: Path) -> str:
    """
    Provides an isolated temporary file path for concurrent testing.
    """

    return str(tmp_path / "test_concurrency.db")


def test_concurrent_readers_and_writer(temp_mmap_file: str) -> None:
    """
    Many concurrent readers during ongoing writes (no lost or torn reads).
    """

    pager = MmapPager(temp_mmap_file, page_size=DEFAULT_PAGE_SIZE)
    tree = BTree(pager)

    key = b"shared_key"
    tree.put(key, b"v0")

    stop_event = threading.Event()
    read_errors: list[str] = []

    def reader_task() -> None:
        while not stop_event.is_set():
            try:
                val = tree.get(key)

                if val is None or not val.startswith(b"v"):
                    read_errors.append(f"Torn or lost read detected: {val}")

            except Exception as error:
                read_errors.append(f"Exception in reader: {error}")

    try:
        readers = [threading.Thread(target=reader_task) for _ in range(10)]
        for r in readers:
            r.start()

        for i in range(1, 1000):
            tree.put(key, f"v{i}".encode())

        stop_event.set()
        for r in readers:
            r.join()

        assert len(read_errors) == 0, f"Concurrency errors found: {read_errors[:5]}"
    finally:
        pager.close()


def test_space_reuse_bounded_file_size(temp_mmap_file: str) -> None:
    """
    Repeatedly overwrites keys and asserts the file size stays bounded (proving space reuse).
    """

    pager = MmapPager(temp_mmap_file, page_size=DEFAULT_PAGE_SIZE)
    tree = BTree(pager)

    key = b"volatile_key"

    try:
        tree.put(key, b"initial_value")

        for i in range(2000):
            val = f"value_{i:04d}".encode().ljust(100, b'X')
            tree.put(key, val)

        final_size = os.path.getsize(temp_mmap_file)

        assert final_size < 80 * 1024, f"File grew unbounded to {final_size} bytes!"
    finally:
        pager.close()
