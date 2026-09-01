# storage configuration
PAGE_SIZE = 4096

# node-type byte markers
MARKER_HEADER = 0x2A
MARKER_LEAF = 0x01
MARKER_INTERNAL = 0x02
MARKER_FREELIST = 0x03

# a Free-List page requires 19 bytes for metadata:
# Marker (1B) + Page ID (8B) + Next Page ID (8B) + Count (2B)
FREELIST_METADATA_SIZE = 19
