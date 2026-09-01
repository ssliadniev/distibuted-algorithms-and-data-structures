# storage configuration
DEFAULT_PAGE_SIZE = 4096

# node-type byte markers
MARKER_HEADER = 0x2A
MARKER_LEAF = 0x01
MARKER_INTERNAL = 0x02
MARKER_FREELIST = 0x03

# a free-list page requires 19 bytes for metadata:
# marker (1B) + page ID (8B) + next Page ID (8B) + count (2B)
FREELIST_METADATA_SIZE = 19

# node header sizes (marker + Page ID + key count)
LEAF_HEADER_SIZE = 11
INTERNAL_HEADER_SIZE = 11


# entry overhead sizes
POINTER_SIZE = 8
LEAF_ENTRY_OVERHEAD = 4     # key length (2B) + value length (2B)
INTERNAL_ENTRY_OVERHEAD = 2 # key length (2B)
