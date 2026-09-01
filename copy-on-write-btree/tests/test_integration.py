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
    Provides an isolated temporary file path for testing.
    """

    return str(tmp_path / "test_store.db")


def test_mmap_durability_and_reopen(temp_mmap_file: str) -> None:
    """
    Verifies open-or-create behavior and data durability across restarts.
    """

    pager1 = MmapPager(temp_mmap_file, page_size=DEFAULT_PAGE_SIZE)
    tree1 = BTree(pager1)

    try:
        tree1.put(key=b"user:1", value=b"alice")
        tree1.put(key=b"user:2", value=b"bob")

        assert tree1.get(b"user:1") == b"alice"
    finally:
        pager1.close()

    pager2 = MmapPager(temp_mmap_file, page_size=DEFAULT_PAGE_SIZE)
    tree2 = BTree(pager2)

    try:
        assert tree2.get(b"user:1") == b"alice"
        assert tree2.get(b"user:2") == b"bob"

        tree2.put(key=b"user:2", value=b"robert")
        tree2.put(key=b"user:3", value=b"charlie")
    finally:
        pager2.close()

    pager3 = MmapPager(temp_mmap_file, page_size=DEFAULT_PAGE_SIZE)
    tree3 = BTree(pager3)

    try:
        assert tree3.get(b"user:1") == b"alice"
        assert tree3.get(b"user:2") == b"robert"
        assert tree3.get(b"user:3") == b"charlie"
    finally:
        pager3.close()


def test_space_reuse_across_restarts(temp_mmap_file: str) -> None:
    """
    Repeated open -> overwrite -> close -> assert bounded file size.
    """

    key = b"persistent_key"
    val = b"X" * 1000

    pager = MmapPager(temp_mmap_file, page_size=DEFAULT_PAGE_SIZE)

    tree = BTree(pager)
    tree.put(key, val)
    pager.close()

    baseline_size = os.path.getsize(temp_mmap_file)

    for _ in range(30):
        p = MmapPager(temp_mmap_file, page_size=DEFAULT_PAGE_SIZE)
        t = BTree(p)
        t.put(key, b"Y" * 1000)
        p.close()

    final_size = os.path.getsize(temp_mmap_file)

    assert final_size <= baseline_size + (5 * DEFAULT_PAGE_SIZE), f"File leaked space across restarts: {final_size} bytes"


def test_freelist_head_reuse_with_long_reader_and_reopen(temp_mmap_file: str) -> None:
    """
    Tests that unlinked free-list metadata pages are durably persisted
    when recycled under the pressure of a genuinely active legacy reader thread.
    """

    pager = MmapPager(temp_mmap_file, page_size=DEFAULT_PAGE_SIZE)
    tree = BTree(pager)

    for i in range(100):
        tree.put(f"k{i:03d}".encode(), b"V" * 200)

    entered = threading.Event()
    release = threading.Event()
    original_get = tree._get_recursive

    def blocked_get(page_id, key):
        entered.set()
        release.wait()
        return original_get(page_id, key)

    tree._get_recursive = blocked_get

    reader = threading.Thread(target=lambda: tree.get(b"k001"))
    reader.start()

    entered.wait()

    for i in range(100, 250):
        tree.put(f"k{i:03d}".encode(), b"V" * 200)

    release.set()
    reader.join()

    tree._get_recursive = original_get
    pager.close()

    pager2 = MmapPager(temp_mmap_file, page_size=DEFAULT_PAGE_SIZE)
    tree2 = BTree(pager2)

    tree2.put(key=b"new_key", value=b"safe_value")
    assert tree2.get(b"new_key") == b"safe_value"

    pager2.close()
