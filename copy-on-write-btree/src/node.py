import struct
from dataclasses import dataclass
from typing import List

from constants import (MARKER_FREELIST, MARKER_HEADER, MARKER_INTERNAL,
                       MARKER_LEAF, PAGE_SIZE)


@dataclass
class HeaderNode:
    """
    Layout: Marker (1B) + Root Page ID (8B) + Free-List Head (8B).
    """

    root_page_id: int
    freelist_head: int

    def serialize(self) -> bytearray:
        buf = bytearray(PAGE_SIZE)
        struct.pack_into("<B Q Q", buf, 0, MARKER_HEADER, self.root_page_id, self.freelist_head)
        return buf

    @classmethod
    def deserialize(cls, data: bytearray) -> 'HeaderNode':
        _, root_page_id, freelist_head = struct.unpack_from("<B Q Q", data, 0)
        return cls(root_page_id, freelist_head)


@dataclass
class LeafNode:
    """
    Layout: Marker (1B) + Page ID (8B) + Key Count (2B) + [Key Len, Key, Val Len, Val] repeats.
    """

    page_id: int
    keys: List[bytes]
    values: List[bytes]

    def serialize(self) -> bytearray:
        req_size = 11 + sum(4 + len(k) + len(v) for k, v in zip(self.keys, self.values))
        buf = bytearray(max(PAGE_SIZE, req_size))

        struct.pack_into("<B Q H", buf, 0, MARKER_LEAF, self.page_id, len(self.keys))
        offset = 11

        for key, val in zip(self.keys, self.values):
            struct.pack_into("<H", buf, offset, len(key))
            offset += 2

            buf[offset:offset + len(key)] = key
            offset += len(key)

            struct.pack_into("<H", buf, offset, len(val))
            offset += 2

            buf[offset:offset + len(val)] = val
            offset += len(val)

        return buf

    @classmethod
    def deserialize(cls, data: bytearray) -> 'LeafNode':
        _, page_id, key_count = struct.unpack_from("<B Q H", data, 0)

        offset = 11
        keys, values = [], []

        for _ in range(key_count):
            key_len = struct.unpack_from("<H", data, offset)[0]
            offset += 2

            keys.append(bytes(data[offset:offset + key_len]))
            offset += key_len

            val_len = struct.unpack_from("<H", data, offset)[0]
            offset += 2

            values.append(bytes(data[offset:offset + val_len]))
            offset += val_len

        return cls(page_id, keys, values)


@dataclass
class InternalNode:
    """
    Layout: Marker (1B) + Page ID (8B) + Key Count (2B) + [Key Len, Key] repeats + [Child Page IDs].
    """

    page_id: int
    keys: List[bytes]
    child_page_ids: List[int]

    def serialize(self) -> bytearray:
        req_size = 11 + sum(2 + len(k) for k in self.keys) + (8 * len(self.child_page_ids))
        buf = bytearray(max(PAGE_SIZE, req_size))

        struct.pack_into("<B Q H", buf, 0, MARKER_INTERNAL, self.page_id, len(self.keys))
        offset = 11

        for key in self.keys:
            struct.pack_into("<H", buf, offset, len(key))
            offset += 2

            buf[offset:offset + len(key)] = key
            offset += len(key)

        for child_id in self.child_page_ids:
            struct.pack_into("<Q", buf, offset, child_id)
            offset += 8

        return buf

    @classmethod
    def deserialize(cls, data: bytearray) -> 'InternalNode':
        _, page_id, key_count = struct.unpack_from("<B Q H", data, 0)

        offset = 11
        keys = []

        for _ in range(key_count):
            key_len = struct.unpack_from("<H", data, offset)[0]
            offset += 2

            keys.append(bytes(data[offset:offset + key_len]))
            offset += key_len

        child_page_ids = []
        for _ in range(key_count + 1):
            child_id = struct.unpack_from("<Q", data, offset)[0]
            offset += 8

            child_page_ids.append(child_id)

        return cls(page_id, keys, child_page_ids)


@dataclass
class FreeListNode:
    """
    Layout: Marker (1B) + Page ID (8B) + Next Page ID (8B) + Count (2B) + [Free Page IDs].
    """

    page_id: int
    next_page_id: int
    free_page_ids: List[int]

    def serialize(self) -> bytearray:
        buf = bytearray(PAGE_SIZE)
        struct.pack_into(
            "<B Q Q H", buf, 0, MARKER_FREELIST, self.page_id, self.next_page_id, len(self.free_page_ids)
        )

        offset = 19
        for fid in self.free_page_ids:
            struct.pack_into("<Q", buf, offset, fid)
            offset += 8

        return buf

    @classmethod
    def deserialize(cls, data: bytearray) -> 'FreeListNode':
        _, page_id, next_page_id, count = struct.unpack_from("<B Q Q H", data, 0)

        offset = 19
        free_page_ids = []

        for _ in range(count):
            fid = struct.unpack_from("<Q", data, offset)[0]
            offset += 8
            free_page_ids.append(fid)

        return cls(page_id, next_page_id, free_page_ids)
