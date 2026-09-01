import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from src.btree import BTree
from src.constants import PAGE_SIZE
from src.pager import InMemoryPager


@pytest.fixture
def tree() -> BTree:
    """
    Provides a fresh in-memory tree for each test.
    """

    pager = InMemoryPager(page_size=PAGE_SIZE)
    return BTree(pager)

def test_missing_key_returns_null(tree: BTree) -> None:
    assert tree.get(b"nonexistent") is None

def test_insert_and_get(tree: BTree) -> None:
    tree.put(key=b"hello", value=b"world")

    assert tree.get(b"hello") == b"world"

def test_upsert(tree: BTree) -> None:
    tree.put(key=b"key1", value=b"original_value")
    tree.put(key=b"key1", value=b"updated_value")

    assert tree.get(b"key1") == b"updated_value"
    assert tree.get(b"key1") != b"original_value"

def test_leaf_split(tree: BTree) -> None:
    for i in range(50):
        key = f"key_{i:04d}".encode().ljust(100, b'K')
        val = f"val_{i:04d}".encode().ljust(100, b'V')

        tree.put(key, val)

    for i in range(50):
        key = f"key_{i:04d}".encode().ljust(100, b'K')
        val = f"val_{i:04d}".encode().ljust(100, b'V')

        assert tree.get(key) == val

def test_multi_level_growth(tree: BTree) -> None:
    for i in range(1000):
        key = f"key_{i:04d}".encode().ljust(100, b'X')
        val = f"val_{i:04d}".encode().ljust(100, b'Y')

        tree.put(key, val)

    for i in range(1000):
        key = f"key_{i:04d}".encode().ljust(100, b'X')
        val = f"val_{i:04d}".encode().ljust(100, b'Y')

        assert tree.get(key) == val
