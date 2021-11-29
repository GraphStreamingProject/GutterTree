#include "../include/gutter_tree.h"
#include "../include/buffer_flusher.h"

#include <utility>
#include <unistd.h> //sysconf
#include <string.h> //memcpy
#include <fcntl.h>  //posix_fallocate
#include <errno.h>
#include <fstream>
#include <queue>

/**
 * Returns the minimum fanout necessary to achieve the a tree of depth max_level
 * @param num_nodes    total number of nodes in the graph
 * @param max_level    the depth of the tree when using maximum fanout
 * @param max_fanout   the maximum fanout of the graph 
 */
static int min_fanout(node_id_t num_nodes, int max_level, int max_fanout) {
  int min = 2;
  int max = max_fanout;
  int mid = ((max - min) / 2) + min;

  while (mid != min && mid != max) {
    int new_depth = ceil(log(num_nodes) / log(mid));
    if (new_depth > max_level) 
      min = mid;
    else 
      max = mid;

    mid = ((max - min) / 2) + min;
  }
  return max;
}

/*
 * Constructor
 * Sets up the gutter_tree given the storage directory, buffer size, number of children
 * and the number of nodes we will insert(N)
 * We assume that node indices begin at 0 and increase to N-1
 */
GutterTree::GutterTree(std::string dir, node_id_t nodes, int workers, bool reset=false) : dir(dir), num_nodes(nodes) {
  if (num_nodes <= 1) {
    printf("ERROR: Cannot create a GutterTree with fewer than 2 ids to buffer\n");
    exit(EXIT_FAILURE);
  }

  configure_tree();

  page_size = (page_size % serial_update_size == 0)? page_size : page_size + serial_update_size - page_size % serial_update_size;
  if (buffer_size < page_size) {
    printf("WARNING: requested buffer size smaller than page_size. Set to page_size.\n");
    buffer_size = page_size;
  }
  
  // setup universal variables
  max_level       = ceil(log(num_nodes) / log(fanout));
  fanout          = min_fanout(num_nodes, max_level, fanout); // minimum fanout which preserves depth
  backing_EOF     = 0;

  // leaf size should be proportional to a sketch
  leaf_size       = floor(gutter_factor * sketch_size(num_nodes));
  leaf_size       = (leaf_size % serial_update_size == 0)? leaf_size : leaf_size + serial_update_size - leaf_size % serial_update_size;

  // create memory for flushing
  flush_data = new flush_struct(this); // must be done after setting up universal variables
  

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
    fprintf(stderr, "Failed to open backing storage file! error=%s\n", strerror(errno));
    exit(1);
  }

  setup_tree(); // setup the structure of the GutterTree

  // create the work queue in which we will place full leaves
  wq = new WorkQueue(queue_factor*workers, leaf_size + page_size);

  // start the buffer flushers
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
  delete wq;

  for(unsigned i = 0; i < num_flushers; i++) {
    delete flushers[i];
  }
  free(flushers);
  close(backing_store);
}

void GutterTree::configure_tree() {
  uint32_t buffer_exp  = 20;
  uint16_t branch      = 64;
  int queue_f          = 2;
  int page_factor      = 1;
  int n_fushers        = 1;
  float gutter_f       = 1;
  std::string line;
  std::ifstream conf("./buffering.conf");
  if (conf.is_open()) {
    while(getline(conf, line)) {
      if (line[0] == '#' || line[0] == '\n') continue;
      if(line.substr(0, line.find('=')) == "buffer_exp") {
        buffer_exp  = std::stoi(line.substr(line.find('=') + 1));
        if (buffer_exp > 30 || buffer_exp < 10) {
          printf("WARNING: buffer_exp out of bounds [10,30] using default(20)\n");
          buffer_exp = 20;
        }
      }
      if(line.substr(0, line.find('=')) == "branch") {
        branch = std::stoi(line.substr(line.find('=') + 1));
        if (branch > 2048 || branch < 2) {
          printf("WARNING: branch out of bounds [2,2048] using default(64)\n");
          branch = 64;
        }
      }
      if(line.substr(0, line.find('=')) == "queue_factor") {
        queue_f = std::stoi(line.substr(line.find('=') + 1));
        if (queue_f > 16 || queue_f < 1) {
          printf("WARNING: queue_factor out of bounds [1,16] using default(2)\n");
          queue_f = 2;
        }
      }
      if(line.substr(0, line.find('=')) == "page_factor") {
        page_factor = std::stoi(line.substr(line.find('=') + 1));
        if (page_factor > 50 || page_factor < 1) {
          printf("WARNING: page_factor out of bounds [1,50] using default(1)\n");
          page_factor = 1;
        }
      }
      if(line.substr(0, line.find('=')) == "num_threads") {
        n_fushers = std::stoi(line.substr(line.find('=') + 1));
        if (n_fushers > 20 || n_fushers < 1) {
          printf("WARNING: num_threads out of bounds [1,20] using default(1)\n");
          n_fushers = 1;
        }
      }
      if(line.substr(0, line.find('=')) == "gutter_factor") {
        gutter_f = std::stof(line.substr(line.find('=') + 1));
        if (gutter_f < 1 && gutter_f > -1) {
          printf("WARNING: gutter_factor must be outside of range -1 < x < 1 using default(1)\n");
          gutter_f = 1;
        }
      }
    }
  } else {
    printf("WARNING: Could not open buffering configuration file! Using default setttings.\n");
  }
  buffer_size = 1 << buffer_exp;
  fanout = branch;
  queue_factor = queue_f;
  num_flushers = n_fushers;
  page_size = page_factor * sysconf(_SC_PAGE_SIZE); // works on POSIX systems (alternative is boost)
  // Windows may need https://docs.microsoft.com/en-us/windows/win32/api/sysinfoapi/nf-sysinfoapi-getnativesysteminfo?redirectedfrom=MSDN

  gutter_factor = gutter_f;
  if (gutter_factor < 0)
    gutter_factor = 1 / (-1 * gutter_factor); // gutter factor reduces size if negative
}

void print_tree(std::vector<RootControlBlock *>rcb_list, std::vector<BufferControlBlock *>bcb_list) {
  for(uint32_t i = 0; i < rcb_list.size(); i++) {
    if (rcb_list[i] != nullptr)
      rcb_list[i]->print();
  }
  for(uint32_t i = 0; i < bcb_list.size(); i++) {
    if (bcb_list[i] != nullptr) 
      bcb_list[i]->print();
  }
}

void GutterTree::setup_tree() {
  printf("Creating a tree of depth %i\n", max_level);
  File_Pointer size = 0;
  std::queue<BufferControlBlock *> bfs_queue; // stack of internal nodes to build is BFS pattern

  // Create the roots of the trees
  double total = num_nodes;
  node_id_t key  = 0;
  for (uint32_t i = 0; i < fanout; i++) {
    // set min_key and max_key to be appropriate based upon the keys allocated to roots
    // and the fanout. (example: if we have fanout = 3 and 10 keys, roots are: 0-3, 4-6, 7-9)
    buffer_id_t first_key = key;
    key += ceil(total / (fanout - i));
    buffer_id_t second_key = key - 1;
    total -= ceil(total / (fanout - i));
    
    RootControlBlock *rcb;
    if (first_key == second_key)
      rcb = new RootControlBlock(i, size, leaf_size);
    else
      rcb = new RootControlBlock(i, size, buffer_size);

    // get the first BufferControlBlock within the root and update that
    // We will update the other bcb at the end of this process
    BufferControlBlock *bcb = rcb->get_buf(0);
    bcb->min_key = first_key;
    bcb->max_key = second_key;

    if (bcb->min_key != bcb->max_key) {
      bfs_queue.push(bcb); // need to process this rcb's children
      size += buffer_size * 2;
    }
    else {
      size += leaf_size * 2; // keep two buffers of size buffer_size for each root
    }

    roots.push_back(rcb);
  }

  cache = (char *) malloc(size); // malloc memory for the root buffers

  // handle the internal nodes and leaves now by pulling from stack until empty
  size = 0; // reset size because roots are in cache. Size now refers to size on disk
  while (!bfs_queue.empty()) {
    BufferControlBlock *parent = bfs_queue.front();
    bfs_queue.pop(); // remove parent from stack

    int key = parent->min_key;
    double total = parent->max_key - parent->min_key + 1;
    int num_kids = (total < fanout)? total : fanout;

    for (int i = 0; i < num_kids; i++) {
      BufferControlBlock *bcb = new BufferControlBlock(buffers.size(), size, parent->level + 1);

      bcb->min_key = key;
      key += ceil(total / (num_kids - i));
      bcb->max_key = key - 1;

      total -= ceil(total / (num_kids - i));
      if (bcb->min_key != bcb->max_key) {
        bfs_queue.push(bcb); // need to process this one's children
        size += buffer_size + page_size; // this bcb is an internal node
      }
      else {
        size += leaf_size + page_size; // this bcb is a leaf
      }
      parent->add_child(bcb->get_id());
      buffers.push_back(bcb);
    }
  }

  // loop through the roots and call finish_setup which ensures the buffers have
  // appropriate meta-data
  for (RootControlBlock *rcb : roots)
    rcb->finish_setup();

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
  // print_tree(roots, buffers);
}

/*
 * Function to convert an update_t to a char array
 * @param   dst the memory location to put the serialized data
 * @param   src the edge update
 * @return  nothing
 */
inline static void serialize_update(char *dst, update_t src) {
  node_id_t node1 = src.first;
  node_id_t node2 = src.second;

  memcpy(dst, &node1, sizeof(node_id_t));
  memcpy(dst + sizeof(node_id_t), &node2, sizeof(node_id_t));
}

/*
 * Function to covert char array to update_t
 * @param src the memory location to load serialized data from
 * @param dst the edge update to put stuff into
 * @return nothing
 */
inline static update_t deserialize_update(char *src) {
  update_t dst;
  dst.first  = *((node_id_t *) src);
  dst.second = *((node_id_t *) (src + sizeof(node_id_t)));

  return dst;
}

/*
 * Copy the serialized data from one location to another
 * @param src data to copy from
 * @param dst data to copy to
 * @return nothing
 */
inline static void copy_serial(char *src, char *dst) {
  memcpy(dst, src, GutterTree::serial_update_size);
}

/*
 * Load a key from serialized data
 * @param location data to pull from
 * @return the key pulled from the data
 */
inline static node_id_t load_key(char *location) {
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
  Node key = upd.first;
  buffer_id_t root_id = which_child(key, 0, num_nodes-1, fanout);
  RootControlBlock *root = roots[root_id];
  // printf("Insertion to buffer %i of size %llu\n", r_id, root->size());

  root->check_block(); // check if this insertion needs to be blocked
  BufferControlBlock *bcb = root->get_buf(root->cur_which());
  char *write_loc = cache + bcb->size() + bcb->offset();
  serialize_update(write_loc, upd);

  // Advance the size of the root and check if it's full
  bcb->set_size(bcb->size() + serial_update_size);
  root->check_cur_full(); // check if we need to switch to the other buffer
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
    // printf("moving update of key %u to child %u min_key=%u, max_key=%u\n", key, child+begin, min_key, max_key);
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
  // printf("flushing "); bcb->print();
  if(bcb->size() == 0) {
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
    bcb->set_size(); // data is copied out so no need to save it TODO: experimentally evaluate this

    do_flush(flush_from, data_size, bcb->first_child, bcb->min_key, bcb->max_key, bcb->children_num, bcb->level);
    return;
  }

  // sub level 0 flush
  uint32_t data_to_read = bcb->size();
  uint32_t offset = 0;
  while(data_to_read > 0) {
    int len = pread(backing_store, flush_from.read_buffers[level] + offset, data_to_read, bcb->offset() + offset);
    if (len == -1) {
      printf("ERROR flush failed to read from buffer %i, %s\n", bcb->get_id(), strerror(errno));
      exit(EXIT_FAILURE);
    }
    data_to_read -= len;
    offset += len;
  }
  do_flush(flush_from, bcb->size(), bcb->first_child, bcb->min_key, bcb->max_key, bcb->children_num, bcb->level);
  bcb->set_size(); // set size if sub level 0 flush
}

flush_ret_t inline GutterTree::flush_leaf_node(flush_struct &flush_from, BufferControlBlock *bcb) {
  uint8_t level = bcb->level;
  if (level == 0) {
    wq->push(cache + bcb->offset(), bcb->size());
    bcb->set_size();
    return;
  }

  // sub level flush
  uint32_t data_to_read = bcb->size();
  uint32_t offset = 0;
  while(data_to_read > 0) {
    int len = pread(backing_store, flush_from.read_buffers[level] + offset, data_to_read, bcb->offset() + offset);
    if (len == -1) {
      printf("ERROR flush failed to read from buffer %i, %s\n", bcb->get_id(), strerror(errno));
      exit(EXIT_FAILURE);
    }
    data_to_read -= len;
    offset += len;
  }
  wq->push(flush_from.read_buffers[level], bcb->size()); // add the data we read to the circular queue
    
  // reset the BufferControlBlock
  bcb->set_size();
}

// ask the gutter_tree for data
// this function may sleep until data is available
bool GutterTree::get_data(data_ret_t &data) {
  File_Pointer idx = 0;

  // make a request to the circular buffer for data
  std::pair<int, queue_elm> queue_data;
  bool got_data = wq->peek(queue_data);

  if (!got_data)
    return false; // we got no data so return not valid

  int i         = queue_data.first;
  queue_elm elm = queue_data.second;
  char *serial_data = elm.data;
  uint32_t len      = elm.size;

  if (len == 0) {
    wq->pop(i);
    return false; // we got no data so return not valid
  }

  data.second.clear(); // remove any old data from the vector
  uint32_t vec_len  = len / serial_update_size;
  data.second.reserve(vec_len); // reserve space for our updates

  // assume the first key is correct so extract it
  node_id_t key = load_key(serial_data);
  data.first = key;

  while(idx < (uint64_t) len) {
    update_t upd = deserialize_update(serial_data + idx);
    // printf("got update: %lu %lu\n", upd.first, upd.second);
    if (upd.first == 0 && upd.second == 0) {
      break; // got a null entry so done
    }

    if (upd.first != key) {
      // error to handle some weird unlikely gutter_tree shenanigans
      printf("source node %u and key %u do not match in get_data()\n", upd.first, key);
      printf("idx = %lu  len = %u\n", idx, len);
      throw KeyIncorrectError();
    }

    // printf("query to node %lu got edge to node %lu\n", key, upd.second);
    data.second.push_back(upd.second);
    idx += serial_update_size;
  }

  wq->pop(i); // mark the wq entry as clean
  return true;
}

// Helper function for force flush. This function flushes an entire subtree rooted at one
// of our cached root buffers
flush_ret_t GutterTree::flush_subtree(flush_struct &flush_from, BufferControlBlock *root) {
  flush_control_block(flush_from, root);

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
}

flush_ret_t GutterTree::force_flush() {
  // Tell the BufferFlushers to flush the entire subtrees
  BufferFlusher::force_flush = true;

  // add each of the roots to the flush_queue
  BufferFlusher::queue_lock.lock();
  for (buffer_id_t r_id = 0; r_id < fanout; r_id++) {
    BufferFlusher::flush_queue.push({roots[r_id], roots[r_id]->cur_which()});
  }
  BufferFlusher::queue_lock.unlock();
  BufferFlusher::flush_ready.notify_all();

  // help flush the subtrees by pulling from the queue
  while(true) {
    BufferFlusher::queue_lock.lock();
    if(!BufferFlusher::flush_queue.empty()) {
      flush_queue_elm elm = BufferFlusher::flush_queue.front();
      BufferFlusher::flush_queue.pop();
      BufferFlusher::queue_lock.unlock();
      RootControlBlock *rcb = elm.rcb;
      rcb->lock_flush();
      BufferControlBlock *bcb = rcb->get_buf(elm.which_buf);

      // printf("main thread flushing buffer %u\n", idx);
      flush_subtree(*flush_data, bcb);
      rcb->unlock_flush();
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

void GutterTree::set_non_block(bool block) {
  if (block) {
    wq->no_block = true; // circular queue operations should no longer block
    wq->wq_empty.notify_all();
    wq->wq_full.notify_all();
  }
  else {
    wq->no_block = false; // set circular queue to block if necessary
  }
}
