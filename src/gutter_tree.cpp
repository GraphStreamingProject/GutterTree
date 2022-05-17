#include "../include/gutter_tree.h"
#include "../include/buffer_flusher.h"
#include "../include/gt_file_errors.h"

#include <utility>
#include <unistd.h> //open and close
#include <string.h> //memcpy
#include <fcntl.h>  //posix_fallocate
#include <errno.h>
#include <fstream>

/*
 * Constructor
 * Sets up the gutter_tree given the storage directory, buffer size, number of children
 * and the number of nodes we will insert(N)
 * We assume that node indices begin at 0 and increase to N-1
 */
GutterTree::GutterTree(std::string dir, node_id_t nodes, int workers, const GutteringConfiguration &conf, 
 bool reset) 
: GutteringSystem(nodes, workers, conf, true), dir(dir), num_nodes(nodes) {
  if (buffer_size < page_size) {
    printf("Invalid buffer size. Must be larger than page size.");
    exit(EXIT_FAILURE);
  }
  
  // setup universal variables
  max_level       = ceil(log(num_nodes) / log(fanout));
  backing_EOF     = 0;

  leaf_size = leaf_gutter_size * serial_update_size; // bytes per leaf

  // create memory for cache and flushing
  flush_data = new flush_struct(this); // must be done after setting up universal variables
  cache = (char *) malloc(fanout * ((uint64_t)buffer_size + page_size));

  // open the file which will be our backing store for the non-root nodes
  // create it if it does not already exist
  int file_flags = O_RDWR | O_CREAT;
  if (reset) {
    file_flags |= O_TRUNC;
  }

  std::string file_name = dir + "gutter_tree_v0.4.data";
  printf("opening file %s\n", file_name.c_str());
  backing_store = open(file_name.c_str(), file_flags, S_IRUSR | S_IWUSR);
  if (backing_store == -1) {
    throw GTFileOpenError(strerror(errno));
  }

  setup_tree(); // setup the gutter tree

  // start the buffer flushers
  printf("number of flushers %i\n", num_flushers);
  flushers = (BufferFlusher **) malloc(sizeof(BufferFlusher *) * num_flushers);
  for (unsigned i = 0; i < num_flushers; i++) {
    flushers[i] = new BufferFlusher(i, this);
  }

  printf("Successfully created gutter tree\n");
}

GutterTree::~GutterTree() {
  printf("Closing GutterTree\n");

  // free malloc'd memory
  delete flush_data;
  free(cache);
  for(uint32_t i = 0; i < buffers.size(); i++) {
    if (buffers[i] != nullptr)
      delete buffers[i];
  }

  for(unsigned i = 0; i < num_flushers; i++) {
    delete flushers[i];
  }
  free(flushers);
  close(backing_store);
}

void print_tree(std::vector<BufferControlBlock *>bcb_list) {
  for(uint32_t i = 0; i < bcb_list.size(); i++) {
    if (bcb_list[i] != nullptr) 
      bcb_list[i]->print();
  }
}

// TODO: clean up this function
void GutterTree::setup_tree() {
  printf("Creating a tree of depth %i\n", max_level);
  File_Pointer size = 0;

  // create the BufferControlBlocks
  for (uint32_t l = 0; l < max_level; l++) { // loop through all levels
    if(l == 1) size = 0; // reset the size because the first level is held in cache

    uint32_t level_size    = pow(fanout, l+1); // number of blocks in this level
    uint32_t plevel_size   = pow(fanout, l);
    uint32_t start         = buffers.size();
    node_id_t key          = 0;

    double parent_keys = num_nodes;
    uint32_t options   = fanout;
    bool skip          = false;
    uint32_t parent    = 0;
    File_Pointer index = 0;

    buffers.reserve(start + level_size);
    for (uint32_t i = 0; i < level_size; i++) { // loop through all blocks in the level
      // get the parent of this node if not level 1 and if we have a new parent
      if (l > 0 && (i-start) % fanout == 0) {
        parent      = start + i/fanout - plevel_size; // this logic should check out because only the last level is ever not full
        parent_keys = buffers[parent]->max_key - buffers[parent]->min_key + 1;
        key         = buffers[parent]->min_key;
        options     = fanout;
        skip        = (parent_keys == 1)? true : false; // if parent leaf then skip
      }
      if (skip || parent_keys == 0) {
        continue;
      }

      BufferControlBlock *bcb = new BufferControlBlock(start + index, size, l);
      bcb->min_key     = key;
      key              += ceil(parent_keys/options);
      bcb->max_key     = key - 1;

      if (l != 0)
        buffers[parent]->add_child(start + index);
      
      parent_keys -= ceil(parent_keys/options);
      options--;
      buffers.push_back(bcb);
      index++; // seperate variable because sometimes we skip stuff
      if(bcb->is_leaf())
        size += leaf_size + page_size; // leaves are of size == sketch
      else 
        size += buffer_size + page_size;
    }
  }

    // allocate file space for all the nodes to prevent fragmentation
  #ifdef LINUX_FALLOCATE
    fallocate(backing_store, 0, 0, size); // linux only but fast
  #endif
    #ifdef WINDOWS_FILEAPI
    // https://stackoverflow.com/questions/455297/creating-big-file-on-windows/455302#455302
    // TODO: implement the above
    throw std::runtime_error("Windows is not currently supported by GutterTree");
  #endif
  #ifdef POSIX_FCNTL
    // taken from https://github.com/trbs/fallocate
    fstore_t store_options = {
      F_ALLOCATECONTIG,
      F_PEOFPOSMODE,
      0,
      static_cast<off_t>(size),
      0
    };
    fcntl(backing_store, F_PREALLOCATE, &store_options);
  #endif
    
    backing_EOF = size;
    // print_tree(buffers);
}

// serialize an update to a data location (should only be used for root I think)
inline void GutterTree::serialize_update(char *dst, update_t src) {
  node_id_t node1 = src.first;
  node_id_t node2 = src.second;

  memcpy(dst, &node1, sizeof(node_id_t));
  memcpy(dst + sizeof(node_id_t), &node2, sizeof(node_id_t));
}

inline update_t GutterTree::deserialize_update(char *src) {
  update_t dst;
  dst.first  = *((node_id_t *) src);
  dst.second = *((node_id_t *) (src + sizeof(node_id_t)));

  return dst;
}

// copy two serailized updates between two locations
inline void GutterTree::copy_serial(char *src, char *dst) {
  memcpy(dst, src, serial_update_size);
}

/*
 * Load a key from a given location
 */
inline node_id_t GutterTree::load_key(char *location) {
  node_id_t key;
  memcpy(&key, location, sizeof(node_id_t));
  return key;
}

/*
 * Helper function which determines which child we should flush to
 */
inline uint32_t which_child(node_id_t key, node_id_t min_key, node_id_t max_key, uint16_t options) {
  node_id_t total = max_key - min_key + 1;
  double div = (double)total / options;

  uint32_t larger_kids = total % options;
  uint32_t larger_count = larger_kids * ceil(div);
  node_id_t idx = key - min_key;

  if (idx >= larger_count)
    return ((idx - larger_count) / (int)div) + larger_kids;
  else
    return idx / ceil(div);
}

/*
 * Perform an insertion to the buffer-tree
 * Insertions always go to the root
 */
insert_ret_t GutterTree::insert(const update_t &upd) {
  // printf("inserting to gutter_tree . . . \n");
  
  // first calculate which of the roots we're inserting to
  node_id_t key = upd.first;
  buffer_id_t r_id = which_child(key, 0, num_nodes-1, fanout); // TODO: Here we are assuming that num_nodes >= fanout
  BufferControlBlock *root = buffers[r_id];
  // printf("Insertion to buffer %i of size %llu\n", r_id, root->size());

  // perform the write and block if necessary
  while (true) {
    std::unique_lock<std::mutex> lk(BufferControlBlock::buffer_ready_lock);
    BufferControlBlock::buffer_ready.wait(lk, [root, this]{return (root->size() < buffer_size + page_size);});
    if (root->size() < buffer_size + page_size) {
      lk.unlock();
      root->lock_rw(); // ensure that a worker isn't flushing this node.
      serialize_update(GutterTree::cache + root->size() + root->offset(), upd);
      root->set_size(root->size() + serial_update_size);
      // printf("Did an insertion. Root %i size now %lu\n", r_id, root->size());
      break;
    }
    lk.unlock();
  }

  // if the buffer is full enough, push it to the flush_queue
  if (root->size() > buffer_size && root->size() - serial_update_size <= buffer_size) {
    BufferFlusher::queue_lock.lock();
    BufferFlusher::flush_queue.push(r_id);
    BufferFlusher::queue_lock.unlock();
    BufferFlusher::flush_ready.notify_one();
    // printf("Added to %i to flush queue\n", r_id);
  }
  root->unlock_rw(); // no longer need root lock
}

/*
 * Function for perfoming a flush anywhere in the tree agnostic to position.
 * this function should perform correctly so long as the parameters are correct.
 *
 * IMPORTANT: after perfoming the flush it is the caller's responsibility to reset
 * the number of elements in the buffer and associated pointers.
 *
 * IMPORTANT: Only one thread should be flushing any given buffer at a time. We 
 * currently enforce this by maintaining a lock on a root node while flushing
 * the associated sub-tree
 */
flush_ret_t GutterTree::do_flush(flush_struct &flush_from, uint32_t data_size, uint32_t begin, 
  node_id_t min_key, node_id_t max_key, uint16_t options, uint8_t level) {
  // setup
  uint32_t full_flush = page_size;

  char **flush_pos = flush_from.flush_positions[level];
  char **flush_buf = flush_from.flush_buffers[level];

  char *data_start = flush_from.read_buffers[level];
  char *data       = data_start;
  for (uint32_t i = 0; i < fanout; i++) {
    flush_pos[i] = flush_buf[i];
  }

  while (data - data_start < data_size) { // loop through all the data to be flushed
    node_id_t key = load_key(data);
    uint32_t child  = which_child(key, min_key, max_key, options);
    if (child > fanout - 1) {
      printf("ERROR: incorrect child %u abandoning insert key=%u min=%u max=%u\n", child, key, min_key, max_key);
      printf("first child = %u\n", buffers[begin]->get_id());
      printf("data pointer = %lu data_start=%lu data_size=%u\n", (uint64_t) data, (uint64_t) data_start, data_size);
      throw KeyIncorrectError();
    }
    if (buffers[child+begin]->min_key > key || buffers[child+begin]->max_key < key) {
      printf("ERROR: bad key %u for child %u, child min = %u, max = %u\n", 
        key, child, buffers[child+begin]->min_key, buffers[child+begin]->max_key);
      throw KeyIncorrectError();
    }
 
    copy_serial(data, flush_pos[child]);
    flush_pos[child] += serial_update_size;

    if (flush_pos[child] - flush_buf[child] >= full_flush) {
      // write to our child, return value indicates if it needs to be flushed
      uint32_t size = flush_pos[child] - flush_buf[child];
      if(buffers[begin+child]->write(this, flush_buf[child], size)) {
        flush_control_block(flush_from, buffers[begin+child]);
      }
      flush_pos[child] = flush_buf[child]; // reset the flush_position
    }
    data += serial_update_size; // go to next thing to flush
  }

  // loop through the flush buffers and write out any non-empty ones
  for (uint32_t i = 0; i < fanout; i++) {
    if (flush_pos[i] - flush_buf[i] > 0) {
      // write to child i, return value indicates if it needs to be flushed
      uint32_t size = flush_pos[i] - flush_buf[i];
      if(buffers[begin+i]->write(this, flush_buf[i], size)) {
        flush_control_block(flush_from, buffers[begin+i]);
      }
    }
  }
}

flush_ret_t GutterTree::flush_control_block(flush_struct &flush_from, BufferControlBlock *bcb) {
  bcb->lock_rw();
  // printf("flushing "); bcb->print();
  if(bcb->size() == 0) {
    bcb->unlock_rw();
    return; // don't flush empty control blocks
  }

  if (bcb->is_leaf()) {
    return flush_leaf_node(flush_from, bcb);
  }
  return flush_internal_node(flush_from, bcb);
}

flush_ret_t inline GutterTree::flush_internal_node(flush_struct &flush_from, BufferControlBlock *bcb) {
  uint8_t level = bcb->level;
  if (level == 0) { // we have this in cache
    uint32_t data_size = bcb->size();
    memcpy(flush_from.read_buffers[level], cache+bcb->offset(), data_size); // move the data over
    bcb->set_size(); // data is copied out so no need to save it

    bcb->lock_flush();
    bcb->unlock_rw(); // allow read/writes to this buffer but maintain flush lock
    do_flush(flush_from, data_size, bcb->first_child, bcb->min_key, bcb->max_key, bcb->children_num, bcb->level);
    bcb->unlock_flush();
    return;
  } 

  // sub level 0 flush
  uint32_t data_to_read = bcb->size();
  uint32_t offset = 0;
  while(data_to_read > 0) {
    int len = pread(backing_store, flush_from.read_buffers[level] + offset, data_to_read, bcb->offset() + offset);
    if (len == -1) {
      throw GTFileReadError(strerror(errno), bcb->get_id());
    }
    data_to_read -= len;
    offset += len;
  }
  do_flush(flush_from, bcb->size(), bcb->first_child, bcb->min_key, bcb->max_key, bcb->children_num, bcb->level);
  bcb->set_size(); // set size if sub level 0 flush
}

// helper function that converts memory address to vector of updates and push to work queue
void GutterTree::mem_to_wq(node_id_t node_idx, char *mem_addr, uint32_t size) {
  std::vector<node_id_t> data_vec;
  data_vec.reserve(size / serial_update_size);
  uint32_t offset = 0;
  while (offset < size) {
    update_t upd = deserialize_update(mem_addr + offset);
    if (upd.first != node_idx) {
      printf("upd key %u and node_idx %u do not match in mem_to_wq()\n", upd.first, node_idx);
      printf("offset = %u size = %u\n", offset, size);
      throw KeyIncorrectError();
    }
    data_vec.push_back(upd.second);
    offset += serial_update_size;
  }
  std::vector<update_batch> batch_vec;
  batch_vec.push_back({node_idx, data_vec});
  wq.push(batch_vec);
}

flush_ret_t inline GutterTree::flush_leaf_node(flush_struct &flush_from, BufferControlBlock *bcb) {
  uint8_t level = bcb->level;
  if (level == 0) {
    mem_to_wq(bcb->min_key, cache + bcb->offset(), bcb->size());
    bcb->set_size();
    bcb->unlock_rw(); // don't need rw lock anymore
    return;
  }

  // sub level flush
  uint32_t data_to_read = bcb->size();
  uint32_t offset = 0;
  while(data_to_read > 0) {
    int len = pread(backing_store, flush_from.read_buffers[level] + offset, data_to_read, bcb->offset() + offset);
    if (len == -1) {
      throw GTFileReadError(strerror(errno), bcb->get_id());
    }
    data_to_read -= len;
    offset += len;
  }

  mem_to_wq(bcb->min_key, flush_from.read_buffers[level], bcb->size()); // add the data we read to the circular queue
    
  // reset the BufferControlBlock
  bcb->set_size();
}

// Helper function for force flush. This function flushes an entire subtree rooted at one
// of our cached root buffers
flush_ret_t GutterTree::flush_subtree(flush_struct &flush_from, BufferControlBlock *root) {
  flush_control_block(flush_from, root);

  root->lock_flush(); // re-acquire the flush lock for flushing sub-tree

  buffer_id_t first_child = root->first_child;
  buffer_id_t num_children = root->children_num;
  for(int l = 0; l < max_level; l++) {
    buffer_id_t new_first_child  = 0;
    buffer_id_t new_num_children = 0;
    for (buffer_id_t idx = 0; idx < num_children; idx++) {
      BufferControlBlock *cur = buffers[idx + first_child];
      if (idx == 0) new_first_child = cur->first_child;
      new_num_children += cur->children_num;

      flush_control_block(flush_from, cur);
    }
    first_child  = new_first_child;
    num_children = new_num_children;
  }
  // done flushing sub-tree so unlock root
  root->unlock_flush();
}

flush_ret_t GutterTree::force_flush() {
  // Tell the BufferFlushers to flush the entire subtrees
  BufferFlusher::force_flush = true;

  // add each of the roots to the flush_queue
  BufferFlusher::queue_lock.lock();
  for (buffer_id_t idx = 0; idx < fanout && idx < buffers.size(); idx++) {
    BufferFlusher::flush_queue.push(idx);
  }
  BufferFlusher::queue_lock.unlock();
  BufferFlusher::flush_ready.notify_all();

  // help flush the subtrees by pulling from the queue
  while(true) {
    BufferFlusher::queue_lock.lock();
    if(!BufferFlusher::flush_queue.empty()) {
      buffer_id_t idx = BufferFlusher::flush_queue.front();
      BufferFlusher::flush_queue.pop();
      BufferFlusher::queue_lock.unlock();
      // printf("main thread flushing buffer %u\n", idx);
      flush_subtree(*flush_data, buffers[idx]);
      // printf("main thread done\n");
    } else {
      BufferFlusher::queue_lock.unlock();
      break;
    }
  }

  // wait for every worker to be done working
  while(true) {
    std::unique_lock<std::mutex> lk(BufferControlBlock::buffer_ready_lock);
    BufferControlBlock::buffer_ready.wait_for(lk, std::chrono::milliseconds(500),
    [this]{
      for (unsigned i = 0; i < num_flushers; i++)
        if (flushers[i]->get_working()) return false;
      return true;
    });

    // double check the condition to catch spurious wake-ups
    bool exit_loop = true;
    for (unsigned i = 0; i < num_flushers; i++) {
      if (flushers[i]->get_working()) {
        exit_loop = false;
        break;
      }
    }
    lk.unlock();
    if (exit_loop)
      break; // we can now safely return
  }
}
