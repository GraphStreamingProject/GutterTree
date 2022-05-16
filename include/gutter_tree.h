#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <queue>
#include <mutex>
#include <math.h>
#include "types.h"
#include "buffer_control_block.h"
#include "work_queue.h"
#include "guttering_system.h"

typedef void insert_ret_t;
typedef void flush_ret_t;

class BufferFlusher;
struct flush_struct;

/*
 * Structure of the GutterTree
 */
class GutterTree : public GutteringSystem {
private:
  // root directory of tree
  std::string dir;

  // memory for flushing
  flush_struct *flush_data;

  /*
   * Functions for flushing the roots of our subtrees and for the BufferFlushers to call.
   * Throws GTFileReadError if there is an error reading from a buffer.
   */
  flush_ret_t flush_internal_node(flush_struct &flush_from, BufferControlBlock *bcb);
  flush_ret_t flush_leaf_node(flush_struct &flush_from, BufferControlBlock *bcb);

  /*
   * function which actually carries out the flush. Designed to be
   * called either upon the root or upon a buffer at any level of the tree
   * @param flush_from  the data to flush and associated memory buffers
   * @param size        the size of the data in bytes
   * @param begin       the smallest id of the node's children
   * @param min_key     the smalleset key this node is responsible for
   * @param max_key     the largest key this node is responsible for
   * @param options     the number of children this node has
   * @param level       the level of the buffer being flushed (0 is root)
   * @returns nothing
   */
  flush_ret_t do_flush(flush_struct &flush_from, uint32_t size, uint32_t begin, 
    node_id_t min_key, node_id_t max_key, uint16_t options, uint8_t level);

  void mem_to_wq(node_id_t node_idx, char *mem_addr, uint32_t size);

  /*
   * Variables which track universal information about the buffer tree which
   * we would like to be accesible to all the bufferControlBlocks
   */
  uint8_t  max_level;    // max depth of the tree
  uint32_t num_nodes;    // number of unique ids to buffer
  uint64_t backing_EOF;  // file to write tree to
  uint64_t leaf_size;    // size of a leaf buffer

  //File descriptor of backing file for storage
  int backing_store;
  // a chunk of memory we reserve to cache the first level of the buffer tree
  char *cache;

  // Buffer Flusher threads
  BufferFlusher **flushers;

public:
  /**
   * Generates a new homebrew buffer tree.
   * @param dir     file path of the data structure root directory, relative to
   *                the executing workspace.
   * @param nodes   number of nodes in the graph.
   * @param workers the number of workers which will be using this buffer tree (defaults to 1).
   * @param reset   should truncate the file storage upon opening.
   * @param conf    (optional) defines the configuration for the gutter tree to pull from
   * 
   * @throw GTFileOpenError if the backing file cannot be opened.
   */
  GutterTree(std::string dir, node_id_t nodes, int workers, const GutteringConfiguration &conf, 
    bool reset=false);
  GutterTree(std::string dir, node_id_t nodes, int workers, bool reset=false) :
    GutterTree(dir, nodes, workers, GutteringConfiguration(), reset) {};
  ~GutterTree();

  /**
   * Puts an update into the data structure.
   * @param upd the edge update.
   * @return nothing.
   */
  insert_ret_t insert(const update_t &upd);

  /**
   * Flushes the entire tree down to the leaves.
   * @return nothing.
   */
  flush_ret_t force_flush();

  /**
   * Functions for flushing bcbs or subtrees of the graph
   * @param flush_from      The memory to use when flushing - associated with a given thread
   * @param bcb             The buffer_control_block to flush
   * 
   * @throw GTFileReadError if there is an error reading from a buffer.
   */
  flush_ret_t flush_subtree(flush_struct &flush_from, BufferControlBlock *bcb);
  flush_ret_t flush_control_block(flush_struct &flush_from, BufferControlBlock *bcb);

  /*
   * Access the maximum number of updates per gutter added to the work queue
   */
  int upds_per_gutter() { return (leaf_size + page_size) / serial_update_size; }

  /*
   * Function to convert an update_t to a char array
   * @param   dst the memory location to put the serialized data
   * @param   src the edge update
   * @return  nothing
   */
  void serialize_update(char *dst, update_t src);

  /*
   * Function to covert char array to update_t
   * @param src the memory location to load serialized data from
   * @param dst the edge update to put stuff into
   * @return nothing
   */
  update_t deserialize_update(char *src);
 
  /*
   * Copy the serialized data from one location to another
   * @param src data to copy from
   * @param dst data to copy to
   * @return nothing
   */
  static void copy_serial(char *src, char *dst);

  /*
   * Load a key from serialized data
   * @param location data to pull from
   * @return the key pulled from the data
   */
  static node_id_t load_key(char *location);

  /*
   * Creates the entire buffer tree to produce a tree of depth log_B(N)
   */
  void setup_tree();

  // metadata control block(s)
  // level 1 blocks take indices 0->(B-1). So on and so forth from there
  std::vector<BufferControlBlock*> buffers;

  /*
   * A bunch of functions for accessing buffer tree variables
   */
  inline uint32_t get_page_size()    { return page_size; };
  inline uint8_t  get_max_level()    { return max_level; };
  inline uint32_t get_buffer_size()  { return buffer_size; };
  inline uint64_t get_leaf_size()    { return leaf_size; };
  inline uint32_t get_fanout()       { return fanout; };
  inline uint32_t get_num_nodes()    { return num_nodes; };
  inline uint64_t get_file_size()    { return backing_EOF; };
  inline uint32_t get_queue_factor() { return queue_factor; };

  inline int get_fd()       { return backing_store; };
  inline char * get_cache() { return cache; };

  static const uint32_t serial_update_size = sizeof(node_id_t) + sizeof(node_id_t); // size in bytes of an update
};

struct flush_struct {
  char ***flush_buffers;
  char ***flush_positions;
  char  **read_buffers;

  uint32_t max_level;
  uint32_t fanout;

  flush_struct(GutterTree *gt) : max_level(gt->get_max_level()), fanout(gt->get_fanout()) {
    // malloc the memory used when flushing
    flush_buffers   = (char ***) malloc(sizeof(char **) * max_level);
    flush_positions = (char ***) malloc(sizeof(char **) * max_level);
    read_buffers    = (char **)  malloc(sizeof(char *)  * max_level);
    for (unsigned l = 0; l < max_level; l++) {
      flush_buffers[l]   = (char **) malloc(sizeof(char *) * fanout);
      flush_positions[l] = (char **) malloc(sizeof(char *) * fanout);
      read_buffers[l]    = (char *) malloc(sizeof(char) * (gt->get_buffer_size() + gt->get_page_size()));
      for (unsigned i = 0; i < fanout; i++) {
        flush_buffers[l][i] = (char *) calloc(gt->get_page_size(), sizeof(char));
      }
    }
  }
  
  ~flush_struct() {
    for(unsigned l = 0; l < max_level; l++) {
      free(flush_positions[l]);
      free(read_buffers[l]);
      for (unsigned i = 0; i < fanout; i++) {
        free(flush_buffers[l][i]);
      }
      free(flush_buffers[l]);
    }
    free(flush_buffers);
    free(flush_positions);
    free(read_buffers);
  }
};

class BufferFullError : public std::exception {
private:
  int id;
public:
  BufferFullError(int id) : id(id) {};
  virtual const char* what() const throw() {
    if (id == -1)
      return "Root buffer is full";
    else
      return "Non-Root buffer is full";
  }
};

class KeyIncorrectError : public std::exception {
public:
  virtual const char * what() const throw() {
    return "The key was not correct for the associated buffer";
  }
};
