#pragma once
#include "guttering_system.h"
#include <array>
#include <cassert>

// gcc seems to be one of few complilers where log2 is a constexpr 
// so this is a log2 function that is constexpr (bad performance, only use at compile time)
// Input 'num' must be a power of 2
constexpr int log2_constexpr(size_t num) {
  int power = 0;
  while (num > 1) { num >>= 1; ++power; }
  return power;
}

class CacheGuttering : public GutteringSystem {
 private:
  size_t inserters;
  node_id_t num_nodes;

  // TODO: use cmake to establish some compiler constants for these variables
  // currently these are the values for bigboi
  static constexpr size_t cache_line     = 64; // number of bytes in a cache_line
  static constexpr size_t block_size     = 8 * cache_line;
  static constexpr double buffer_growth_factor = 2;

  // basic 'tree' params
  static constexpr size_t level1_fanout       = 16;
  static constexpr size_t level1_bufs         = 8; // number of root buffers. Must be power of 2
  static constexpr size_t level1_elms_per_buf = level1_fanout * block_size / sizeof(update_t);
  static constexpr size_t level2_bufs         = level1_bufs * level1_fanout;
  static constexpr size_t level2_elms_per_buf = level1_elms_per_buf * buffer_growth_factor;
  static constexpr size_t level3_bufs         = level2_bufs * level1_fanout * buffer_growth_factor;
  static constexpr size_t level3_elms_per_buf = level2_elms_per_buf * buffer_growth_factor;
  static constexpr size_t max_level4_bufs     = level3_bufs * level1_fanout * buffer_growth_factor * buffer_growth_factor;

  // bit length variables
  static constexpr int level1_bits = log2_constexpr(level1_bufs);
  static constexpr int level2_bits = log2_constexpr(level2_bufs);
  static constexpr int level3_bits = log2_constexpr(level3_bufs);
  static constexpr int level4_bits = log2_constexpr(max_level4_bufs);

  // bit position variables. Depend upon num_nodes
  const int level1_pos;
  const int level2_pos;
  const int level3_pos;
  const int level4_pos;

  // variables for controlling the fanout and size of optional 4th level
  size_t level4_fanout   = 0;
  size_t level4_elms_per_buf = 0;

  // offset for insertion re-labelling
  node_id_t relabelling_offset = 0;

  using RAM_Gutter  = std::vector<update_t>;
  using Leaf_Gutter = std::vector<node_id_t>;
  template <size_t num_slots>
  struct Cache_Gutter {
    std::array<update_t, num_slots> data;
    size_t num_elms = 0;
    size_t max_elms = 3*num_slots / 4 + rand() % (num_slots/4);
  };
  struct WQ_Buffer {
    std::vector<update_batch> batches;
    size_t size = 0;
  };

  class InsertThread {
   private:
    CacheGuttering &CGsystem; // reference to associated CacheGuttering system

    // thread local gutters
    std::array<Cache_Gutter<level1_elms_per_buf>, level1_bufs> level1_gutters;
    std::array<Cache_Gutter<level2_elms_per_buf>, level2_bufs> level2_gutters;
    std::array<Cache_Gutter<level3_elms_per_buf>, level3_bufs> level3_gutters;

   public:
    InsertThread(CacheGuttering &CGsystem) : CGsystem(CGsystem) {
      local_wq_buffer.batches.resize(CGsystem.wq_batch_per_elm);
      for (auto &batch : local_wq_buffer.batches)
        batch.upd_vec.reserve(CGsystem.leaf_gutter_size);
    };

    // insert an update into the local buffers
    void insert(update_t upd);

    // functions for flushing local buffers
    void flush_buf_l1(const node_id_t idx);
    void flush_buf_l2(const node_id_t idx);
    void flush_buf_l3(const node_id_t idx);
    void flush_buf_l4(const node_id_t idx);
    void flush_all(); // flush entire structure
    void wq_push_helper(node_id_t node_idx, Leaf_Gutter &leaf);
    void flush_wq_buf();

    // Buffer for performing batch push to work queue
    WQ_Buffer local_wq_buffer;

    // no copying for you
    InsertThread(const InsertThread &) = delete;
    InsertThread &operator=(const InsertThread &) = delete;
  
    // moving is allowed
    InsertThread (InsertThread &&) = default;
  };

  // locks for flushing Level3 buffers
  std::mutex *level3_flush_locks;

  // buffers shared amongst all threads
  RAM_Gutter *level4_gutters = nullptr; // additional RAM layer if necessary
  Leaf_Gutter *leaf_gutters;          // final layer that holds node gutters

  friend class InsertThread;

  std::vector<InsertThread> insert_threads; // vector of InsertThreads
 public:
  /**
   * Constructs a new guttering systems using a tree like structure for cache efficiency.
   * @param nodes       number of nodes in the graph.
   * @param workers     the number of workers which will be removing batches
   * @param inserters   the number of inserter buffers
   */
  CacheGuttering(node_id_t nodes, uint32_t workers, uint32_t inserters, const GutteringConfiguration &conf);
  CacheGuttering(node_id_t nodes, uint32_t workers, uint32_t inserters) : 
    CacheGuttering(nodes, workers, inserters, GutteringConfiguration()) {};

  ~CacheGuttering();

  /**
   * Puts an update into the data structure.
   * @param upd the edge update.1
   * @param which, which thread is inserting this update
   * @return nothing.
   */
  insert_ret_t insert(const update_t &upd, size_t which) { 
    assert(which < inserters);
    insert_threads[which].insert(upd);
  }
  
  // pure virtual functions don't like default params, so default to 'which' of 0
  insert_ret_t insert(const update_t &upd) { insert_threads[0].insert(upd); }

  /**
   * Flushes all pending buffers. When this function returns there are no more updates in the
   * guttering system
   * @return nothing.
   */
  flush_ret_t force_flush();

  /**
   * Set the "offset" for incoming edges. That is, if we set an offset of x, an incoming edge
   * {i,j} will be stored internally as an edge {i - x, j}. Use only for integration with
   * distributed guttering. If you don't know what that means, don't use this function!
   * 
   * @param offset 
   * @return a reference to the parent CacheGuttering object.
   */
  CacheGuttering& set_offset(node_id_t offset) { relabelling_offset = offset; return *this; }

  /*
   * Helper function for tracing a root to leaf path. Prints path to stdout
   * @param src   the node id to trace
   */
  void print_r_to_l(node_id_t src);
  void print_fanouts();
};
