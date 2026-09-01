import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from src.btree import BTree
from src.constants import PAGE_SIZE
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

    pager1 = MmapPager(temp_mmap_file, page_size=PAGE_SIZE)
    tree1 = BTree(pager1)

    try:
        tree1.put(key=b"user:1", value=b"alice")
        tree1.put(key=b"user:2", value=b"bob")

        assert tree1.get(b"user:1") == b"alice"
    finally:
        pager1.close()

    pager2 = MmapPager(temp_mmap_file, page_size=PAGE_SIZE)
    tree2 = BTree(pager2)

    try:
        assert tree2.get(b"user:1") == b"alice"
        assert tree2.get(b"user:2") == b"bob"

        tree2.put(key=b"user:2", value=b"robert")
        tree2.put(key=b"user:3", value=b"charlie")
    finally:
        pager2.close()

    pager3 = MmapPager(temp_mmap_file, page_size=PAGE_SIZE)
    tree3 = BTree(pager3)

    try:
        assert tree3.get(b"user:1") == b"alice"
        assert tree3.get(b"user:2") == b"robert"
        assert tree3.get(b"user:3") == b"charlie"
    finally:
        pager3.close()
