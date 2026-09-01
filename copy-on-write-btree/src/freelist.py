import threading
from typing import Dict, List, Optional

from constants import FREELIST_METADATA_SIZE
from node import FreeListNode, HeaderNode
from pager import Pager


class TransactionTracker:
    """
    Tracks active readers using an epoch-based concurrency model.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.current_version = 0
        self.active_readers: Dict[int, int] = {}
        self.pending_frees: Dict[int, List[int]] = {}

    def begin_read(self, thread_id: int) -> None:
        """
        Registers a reader at the current epoch version.
        """

        with self.lock:
            self.active_readers[thread_id] = self.current_version

    def end_read(self, thread_id: int) -> None:
        """
        Unregisters a reader when its transaction completes.
        """

        with self.lock:
            self.active_readers.pop(thread_id, None)

    def commit_write(self, orphaned_pages: List[int]) -> None:
        """
        Stages orphaned pages for future reclamation and advances the version.
        """

        with self.lock:
            self.pending_frees[self.current_version] = orphaned_pages
            self.current_version += 1

    def get_reclaimable_pages(self) -> List[int]:
        """
        Identifies and removes pages retired by versions no longer visible to any reader.

        Returns:
            List[int]: A list of page IDs safe to be added back to the free-list.
        """

        with self.lock:
            min_active_version = min(self.active_readers.values()) if self.active_readers else self.current_version

            reclaimable: List[int] = []
            versions_to_delete: List[int] = []

            for version, pages in self.pending_frees.items():
                if version < min_active_version:
                    reclaimable.extend(pages)
                    versions_to_delete.append(version)

            for v in versions_to_delete:
                del self.pending_frees[v]

            return reclaimable


class FreeListManager:
    """
    Manages the linked list of free pages for space reuse.
    """

    def __init__(self, pager: Pager, header: HeaderNode):
        self.pager = pager
        self.header = header
        self.max_ids_per_page = (self.pager.page_size - FREELIST_METADATA_SIZE) // 8

    def add_free_pages(self, page_ids: list[int]) -> None:
        """
        Appends reclaimed pages to the free-list, chaining new nodes if necessary.

        Args:
            page_ids (List[int]): The orphaned page IDs ready for reuse.
        """

        if not page_ids:
            return

        head_id = self.header.freelist_head
        if head_id == 0:
            head_id = self.pager.allocate_page()
            self.header.freelist_head = head_id

            node = FreeListNode(head_id, 0, [])
        else:
            node = FreeListNode.deserialize(self.pager.read_page(head_id))

        for pid in page_ids:
            if len(node.free_page_ids) >= self.max_ids_per_page:
                self.pager.write_page(node.page_id, node.serialize(self.pager.page_size))

                new_head_id = self.pager.allocate_page()
                node = FreeListNode(new_head_id, node.page_id, [pid])

                self.header.freelist_head = new_head_id
            else:
                node.free_page_ids.append(pid)

        self.pager.write_page(node.page_id, node.serialize(self.pager.page_size))

    def allocate(self) -> Optional[int]:
        """
        Allocates from the free-page list before extending the file.

        Returns:
            Optional[int]: A reclaimed page ID, or None if the free-list is empty.
        """

        if self.header.freelist_head == 0:
            return None

        page_data = self.pager.read_page(self.header.freelist_head)
        node = FreeListNode.deserialize(page_data)

        if node.free_page_ids:
            allocated_id = node.free_page_ids.pop()
            self.pager.write_page(node.page_id, node.serialize(self.pager.page_size))

            return allocated_id

        old_head_id = node.page_id
        self.header.freelist_head = node.next_page_id

        self.pager.publish_freelist_head(node.next_page_id)

        self.pager.sync()

        return old_head_id
