import bisect
import threading
from typing import Optional, Tuple

from constants import (INTERNAL_ENTRY_OVERHEAD, INTERNAL_HEADER_SIZE,
                       LEAF_ENTRY_OVERHEAD, LEAF_HEADER_SIZE, MARKER_INTERNAL,
                       MARKER_LEAF, POINTER_SIZE)
from freelist import FreeListManager, TransactionTracker
from node import HeaderNode, InternalNode, LeafNode
from pager import Pager

SplitResult = Tuple[int, Optional[bytes], Optional[int]]


class BTree:
    """
    A persistent, copy-on-write B+-tree key-value store.
    """

    header: HeaderNode

    def __init__(self, pager: Pager):
        self.pager = pager
        self.tracker = TransactionTracker()
        self.write_lock = threading.Lock()

        header_data = self.pager.read_page(0)
        if header_data[0] == 0:
            self._init_empty_tree()
        else:
            self.header = HeaderNode.deserialize(header_data)

        self.freelist = FreeListManager(self.pager, self.header)

    def _init_empty_tree(self) -> None:
        root_page_id = self.pager.allocate_page()
        empty_root = LeafNode(page_id=root_page_id, keys=[], values=[])
        self.pager.write_page(root_page_id, empty_root.serialize(self.pager.page_size))

        self.header = HeaderNode(root_page_id=root_page_id, freelist_head=0)
        self.pager.write_page(0, self.header.serialize(self.pager.page_size))

    def _allocate_page(self) -> int:
        """
        Allocates a page, preferring the free-list before extending the file.

        Returns:
            int: A usable page identifier.
        """

        page_id = self.freelist.allocate()
        if page_id is not None:
            return page_id

        return self.pager.allocate_page()

    def _split_leaf(self, node: LeafNode) -> tuple[int, bytes, int]:
        """
        Splits an oversized leaf node around half its encoded size.

        Args:
            node (LeafNode): The oversized leaf node requiring a split.

        Returns:
            SplitResult: A tuple containing the new left page ID, the promoted median key and the new right page ID.
        """

        total_size = LEAF_HEADER_SIZE + sum(
            LEAF_ENTRY_OVERHEAD + len(k) + len(v) for k, v in zip(node.keys, node.values)
        )
        target_half = total_size // 2

        split_idx = 1
        left_size = LEAF_HEADER_SIZE

        for i in range(len(node.keys)):
            entry_size = LEAF_ENTRY_OVERHEAD + len(node.keys[i]) + len(node.values[i])

            if left_size + entry_size > self.pager.page_size and i > 0:
                split_idx = i
                break

            left_size += entry_size

            if left_size >= target_half and i < len(node.keys) - 1:
                split_idx = i + 1
                break

        split_key = node.keys[split_idx]

        left_node = LeafNode(node.page_id, node.keys[:split_idx], node.values[:split_idx])
        right_page_id = self._allocate_page()
        right_node = LeafNode(right_page_id, node.keys[split_idx:], node.values[split_idx:])

        self.pager.write_page(left_node.page_id, left_node.serialize(self.pager.page_size))
        self.pager.write_page(right_node.page_id, right_node.serialize(self.pager.page_size))

        return left_node.page_id, split_key, right_node.page_id

    def _split_internal(self, node: InternalNode) -> tuple[int, bytes, int]:
        """
        Splits an oversized internal node around half its encoded size.

        Args:
            node (InternalNode): The oversized internal node requiring a split.

        Returns:
            SplitResult: A tuple containing the new left page ID, the promoted median key and the new right page ID.
        """

        total_size = INTERNAL_HEADER_SIZE + POINTER_SIZE + sum(
            INTERNAL_ENTRY_OVERHEAD + len(k) + POINTER_SIZE for k in node.keys
        )
        target_half = total_size // 2

        split_idx = 1
        left_size = INTERNAL_HEADER_SIZE + POINTER_SIZE

        for i in range(len(node.keys)):
            entry_size = INTERNAL_ENTRY_OVERHEAD + len(node.keys[i]) + POINTER_SIZE

            if left_size + entry_size > self.pager.page_size and i > 0:
                split_idx = i
                break

            left_size += entry_size

            if left_size >= target_half and i < len(node.keys) - 1:
                split_idx = i + 1
                break

        split_key = node.keys[split_idx]

        left_node = InternalNode(node.page_id, node.keys[:split_idx], node.child_page_ids[:split_idx + 1])

        right_page_id = self._allocate_page()
        right_node = InternalNode(right_page_id, node.keys[split_idx + 1:], node.child_page_ids[split_idx + 1:])

        self.pager.write_page(left_node.page_id, left_node.serialize(self.pager.page_size))
        self.pager.write_page(right_node.page_id, right_node.serialize(self.pager.page_size))

        return left_node.page_id, split_key, right_node.page_id

    def _get_recursive(self, page_id: int, target_key: bytes) -> Optional[bytes]:
        """
        Recursively traverses the tree to find a key using binary search.
        """

        page_data = self.pager.read_page(page_id)
        marker = page_data[0]

        if marker == MARKER_LEAF:
            node = LeafNode.deserialize(page_data)
            idx = bisect.bisect_left(node.keys, target_key)

            if idx < len(node.keys) and node.keys[idx] == target_key:
                return node.values[idx]

            return None


        elif marker == MARKER_INTERNAL:
            node = InternalNode.deserialize(page_data)
            idx = bisect.bisect_right(node.keys, target_key)
            child_id = node.child_page_ids[idx]

            return self._get_recursive(child_id, target_key)

        raise ValueError(f"Corrupt tree state: Invalid page marker '{marker}' at page {page_id}")

    def _put_recursive(
        self, page_id: int, key: bytes, value: bytes, orphaned: list[int]
    ) -> tuple[int, Optional[bytes], Optional[int]]:
        """
        Recursively traverses down to insert a key, returning new node pointers.
        """

        orphaned.append(page_id)
        page_data = self.pager.read_page(page_id)
        marker = page_data[0]

        if marker == MARKER_LEAF:
            return self._upsert_leaf(page_data, key, value)
        elif marker == MARKER_INTERNAL:
            return self._upsert_internal(page_data, key, value, orphaned)

        raise ValueError(f"Corrupt tree state: Invalid page marker '{marker}' at page {page_id}")

    def _upsert_leaf(
        self, page_data: bytearray, key: bytes, value: bytes
    ) -> tuple[int, Optional[bytes], Optional[int]]:
        """
        Handles upsert logic at the leaf level, splitting if necessary.
        """

        node = LeafNode.deserialize(page_data)
        idx = bisect.bisect_left(node.keys, key)

        if idx < len(node.keys) and node.keys[idx] == key:
            node.values[idx] = value
        else:
            node.keys.insert(idx, key)
            node.values.insert(idx, value)

        new_page_id = self._allocate_page()
        node.page_id = new_page_id
        serialized = node.serialize(self.pager.page_size)

        if len(serialized) > self.pager.page_size:
            return self._split_leaf(node)

        self.pager.write_page(new_page_id, serialized)
        return new_page_id, None, None

    def _upsert_internal(
        self, page_data: bytearray, key: bytes, value: bytes, orphaned: list[int]
    ) -> tuple[int, Optional[bytes], Optional[int]]:
        """
        Handles routing the upsert through internal nodes, managing child splits.
        """

        node = InternalNode.deserialize(page_data)
        idx = bisect.bisect_right(node.keys, key)
        child_id = node.child_page_ids[idx]

        new_child_id, split_key, split_right_id = self._put_recursive(child_id, key, value, orphaned)
        node.child_page_ids[idx] = new_child_id

        if split_key is not None and split_right_id is not None:
            node.keys.insert(idx, split_key)
            node.child_page_ids.insert(idx + 1, split_right_id)

        new_page_id = self._allocate_page()
        node.page_id = new_page_id
        serialized = node.serialize(self.pager.page_size)

        if len(serialized) > self.pager.page_size:
            return self._split_internal(node)

        self.pager.write_page(new_page_id, serialized)
        return new_page_id, None, None

    def get(self, key: bytes) -> Optional[bytes]:
        """
        Retrieves a value by its key. Logically lock-free, observes a complete and consistent tree snapshot.

        Args:
            key (bytes): The key to look up.

        Returns:
            Optional[bytes]: The associated value, or None if the key is missing.
        """

        tid = threading.get_ident()
        self.tracker.begin_read(tid)

        try:
            return self._get_recursive(self.header.root_page_id, key)
        finally:
            self.tracker.end_read(tid)

    def put(self, key: bytes, value: bytes) -> None:
        """
        Inserts or updates a key-value pair using a copy-on-write operation.

        Args:
            key (bytes): The key to insert or update.
            value (bytes): The value to store.
        """

        with self.write_lock:
            reclaimable = self.tracker.get_reclaimable_pages()

            if reclaimable:
                self.freelist.add_free_pages(reclaimable)
                self.pager.publish_freelist_head(self.header.freelist_head)

            orphaned_pages: list[int] = []

            new_root_id, split_key, split_right_id = self._put_recursive(
                self.header.root_page_id, key, value, orphaned_pages
            )

            if split_key is not None and split_right_id is not None:
                new_super_root_id = self._allocate_page()
                new_root = InternalNode(new_super_root_id, [split_key], [new_root_id, split_right_id])

                self.pager.write_page(new_super_root_id, new_root.serialize(self.pager.page_size))
                new_root_id = new_super_root_id

            self.pager.sync()

            self.header.root_page_id = new_root_id
            self.pager.publish_root(new_root_id)

            self.pager.sync()

            self.tracker.commit_write(orphaned_pages)

            immediate_reclaim = self.tracker.get_reclaimable_pages()
            if immediate_reclaim:
                self.freelist.add_free_pages(immediate_reclaim)
                self.pager.publish_freelist_head(self.header.freelist_head)

                self.pager.sync()
