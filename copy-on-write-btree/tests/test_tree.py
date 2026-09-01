import os
import random
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from src.btree import BTree
from src.constants import DEFAULT_PAGE_SIZE
from src.pager import InMemoryPager


@pytest.fixture
def tree() -> BTree:
    """
    Provides a fresh in-memory tree for each test.
    """

    pager = InMemoryPager(page_size=DEFAULT_PAGE_SIZE)
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

def test_uneven_variable_size_split(tree: BTree) -> None:
    tree.put(b'b', b'X' * 700)
    tree.put(b'c', b'X' * 500)
    tree.put(b'd', b'X' * 500)
    tree.put(b'a', b'X' * 3500)

    assert len(tree.get(b'b')) == 700
    assert len(tree.get(b'c')) == 500
    assert len(tree.get(b'd')) == 500
    assert len(tree.get(b'a')) == 3500


def test_custom_page_size() -> None:
    pager = InMemoryPager(page_size=512)
    tree = BTree(pager)

    for i in range(15):
        tree.put(f"key{i}".encode(), b"V" * 100)

    for i in range(15):
        assert tree.get(f"key{i}".encode()) == b"V" * 100


def test_internal_split(tree: BTree) -> None:
    for i in range(200):
        key = f"key_{i:04d}".encode().ljust(300, b'K')
        val = f"val_{i:04d}".encode()
        tree.put(key, val)

    for i in range(200):
        key = f"key_{i:04d}".encode().ljust(300, b'K')
        assert tree.get(key) == f"val_{i:04d}".encode()


def test_pager_rejects_oversized_page() -> None:
    pager = InMemoryPager(page_size=512)

    with pytest.raises(ValueError, match="exceeds page size"):
        pager.write_page(1, b"X" * 513)


def test_randomized_variable_length_entries(tree: BTree) -> None:
    random.seed(42)
    truth = {}

    for _ in range(500):
        k_len = random.randint(5, 400)
        v_len = random.randint(5, 800)

        k = bytes(random.choices(range(256), k=k_len))
        v = bytes(random.choices(range(256), k=v_len))

        truth[k] = v
        tree.put(k, v)

    for k, v in truth.items():
        assert tree.get(k) == v
