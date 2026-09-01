## How to Run Tests

The project includes a comprehensive test suite covering unit operations, concurrency, and durability constraints. You can run the tests using Docker or locally.

**Using Docker (Recommended)**  
To run the tests in an isolated container that matches the submission environment:
```bash
make test-docker
```
---

## On-Disk Page Format
The store is persisted to a file divided into fixed-size, configurable pages (e.g., 4096 bytes). 
Each page contains a specific serialized structure identified by a leading 1-byte node-type marker. 

| Node Type | Marker | Byte Layout Structure |
| :--- | :--- | :--- |
| **Header** | `0x2A` | `Marker` (1B) + `Root Page ID` (8B) + `Free-List Head` (8B) + `Unused` |
| **Leaf** | `0x01` | `Marker` (1B) + `Page ID` (8B) + `Key Count` (2B) + `[Key Len, Key, Val Len, Val]` repeats + `Unused` |
| **Internal** | `0x02` | `Marker` (1B) + `Page ID` (8B) + `Key Count` (2B) + `[Key Len, Key]` repeats + `[Child Page IDs]` + `Unused` |
| **Free-List** | `0x03` | `Marker` (1B) + `Page ID` (8B) + `Next Page ID` (8B) + `Count` (2B) + `[Free Page IDs]` repeats + `Unused` |

Entries are strictly sorted within each node to facilitate $O(\log n)$ intra-node binary searches.  
Serialized node sizes dictate tree growth: if a page exceeds the configured size limit during an insert, it is dynamically split.

## Copy-on-Write and Commit Process
To guarantee isolated reads and data integrity, B-tree nodes are never modified in place. 
* **Path Copying:** Every `put` operation creates entirely new copies of all nodes along the path from the root down to the modified leaf. 
* **Atomic Commit:** The store publishes a new tree version via a single, atomic overwrite of the `root_page_id` in the header page (page 0). 
* **Persistence:** Synchronization (`fsync`) ensures the file state is durably flushed to disk, allowing existing files to be safely reopened and recovered.

## Space Reclamation and Thread-Safety
Without space reuse, copy-on-write operations would cause unbounded file growth. Pages orphaned during updates are tracked and recycled using an epoch-based concurrency scheme.
* **Lock-Free Readers:** `get()` requests execute concurrently without blocking writers by relying on a snapshot-isolated view of the tree. Each reader registers its active thread upon entry.
* **Safe Reclaim:** Orphaned pages are placed in a pending state. They are only transitioned into the reclaimable free-list once all readers that were active prior to the commit have finished.
* **In-Place Metadata:** Unlike B-tree data nodes, the free-list pages are explicitly permitted to be updated in place. All new allocations attempt to pull from this free-list before extending the underlying memory-mapped file bounds.