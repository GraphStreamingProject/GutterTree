# Guttering Systems
Data-structures for efficient external-memory and in-memory guttering systems.

The following describes some of the basics of how the GutterTree works.

## GutterTree
This class specifies most of the meta-data and functions for handling a gutter tree. Specifically, the gutter tree maintains a list of nodes that compose the tree in addition to some other buffers used to enable efficient IO when either flushing or reading data from a node.

### Tree Structure
Tree includes parameters `M` the size of a buffer, `B` the branching factor, and finally `N` the number of graph node ids we expect to ingest.

```
           ---root---
         /      |     \
     node       node    node
   /  |  \       |  \     |  \
node node node node node node node
```

Each internal node contains a buffer of size `M + page_size` and has at most B children. We construct the tree so that there is a unique node mapping to each of the `N` graph nodes. The leaf nodes are of size proportional to a graph streaming sketch (`polylog(N)`). In the above example B is 3 and N is 7. The bottom level would be full if N equal to 3^2=9.

### Flushing
When a either a root node or an internal node of the tree stores data of size ≥ M then it is ready to be flushed. This flush may happen asynchronously if desired so long as the data stored in the buffer does not exceed M.

When flushing we utilize `flush_buffers` to achieve efficient file writing. The data in the node is scanned and, based upon the source node of each update, is placed into the appropriate `flush_buffer`. When these buffers become full their contents are written to the corresponding child. The flush buffers are of size roughly equal to a page and therefore these IOs should be efficient.

A flush of a leaf node is simply accomplished by moving the data in question into the `WorkQueue`. If the WorkQueue is full then this insertion is blocked until the queue is no longer full.


### BufferControlBlock
Encodes the meta-data associated with a block including its `file_offset` and `storage_ptr`. These two attributes represent the location of a node within the large and physically contiguous `backing_store` file. The nodes of the tree are stored in the file following a breadth first search. Specifically, the data stored within the buffer at each node is what is held within the file. Therefore the data in each level is contiguous on disk.

Example:  
```
------------------------------------------------
|Node 1| Node 2| Node 3| Node 4| Node 5| Node 6|
------------------------------------------------
```

This buffer encodes the following tree with `B=2` and `N=4`
```
      ---root---
       /      \      
   node1      node2    
    /  \       /  \  
node3 node4 node5 node6
```

Note that the root does not appear in the backing store. This is because it is stored entirely in RAM. There is also no BufferControlBlock for the root node for the same reason.

## WorkQueue
When a node leaf node is ready to be processed by the user its data is placed into the WorkQueue. The WorkQueue is an entirely in RAM structure designed to eliminate IO contention between adding data to and getting data out of the gutter tree. With the WorkQueue, requests to the GutterTree for data take place entirely in RAM.

A leaf is ''ready'' to be processed when it is full and, if it was any other node, would normally be flushed. Instead we place the data stored at this leaf into the queue and then delete the data from the leaf node. An analogy for this process is that the leaves are growing fruits (the update buffers) and when these fruits are big and ripe they will naturally fall from the tree.

The WorkQueue is designed with a limited size. This is because RAM usage needs to be minimal and has the added benefit of naturally rate limiting the gutter tree to match the speed at which data can be taken out of it. Operations that need to take data from an empty queue or that need to insert to a full queue are blocked until their preconditions are met.

The structure of the WorkQueue is as follows
```
                ---------- -> ---------- -> ----------
producer_queue: | empty  |    | empty  |    | empty  |  
                ---------- <- ---------- <- ----------

                ----------- -> ----------- -> ----------- -> -----------
consumer_queue: | updates |    | updates |    | updates |    | updates |    
                ----------- <- ----------- <- ----------- <- -----------
```

The `producer queue` contains empty gutters ready to be filled and placed into the `consumer_queue`. `get_data()` calls return the head of the `consumer_queue`. Callbacks are necessary to place gutters back into the producer queue.
