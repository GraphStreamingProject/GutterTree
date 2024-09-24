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
  const size_t inserters;
  const node_id_t num_nodes;

  static constexpr size_t cache_line      = 64;                             // bytes in cache_line
  static constexpr size_t block_size      = 4 * cache_line;                 // 256 bytes
  static constexpr size_t block_elms      = 2 * block_size / sizeof(update_t);  // 32 updates
  static constexpr size_t block_leaf_elms = 2 * block_size / sizeof(node_id_t); // 64 updates
  static constexpr double buffer_growth_factor = 2;

  // params for thread local levels
  static constexpr size_t local_fanout     = 64;
  static constexpr size_t level1_bufs      = 64;
  static constexpr size_t level1_buf_bytes = block_size * local_fanout;               // 16 KiB
  static constexpr size_t level2_bufs      = level1_bufs * local_fanout;              // 4096
  static constexpr size_t level2_buf_bytes = level1_buf_bytes * buffer_growth_factor; // 32 KiB

  // params for shared levels (these are optional, existence depends upon num_vertices)
  static constexpr size_t global_fanout    = 128;
  static constexpr size_t max_level3_bufs  = level2_bufs * local_fanout;              // 2^18
  static constexpr size_t level3_buf_bytes = level2_buf_bytes * buffer_growth_factor; // 64 KiB
  static constexpr size_t max_level4_bufs  = max_level3_bufs * global_fanout;         // 2^25
  static constexpr size_t level4_buf_bytes = level3_buf_bytes * buffer_growth_factor; // 128 KiB

  // precompute number of updates per local buf
  static constexpr size_t level1_elms_per_buf = level1_buf_bytes / sizeof(update_t);
  static constexpr size_t level2_elms_per_buf = level2_buf_bytes / sizeof(update_t);
  static constexpr size_t level3_elms_per_buf = level3_buf_bytes / sizeof(update_t);
  static constexpr size_t level4_elms_per_buf = level4_buf_bytes / sizeof(update_t);

  // bit length variables
  static constexpr int level1_bits = log2_constexpr(level1_bufs);
  static constexpr int level2_bits = log2_constexpr(level2_bufs);
  static constexpr int level3_bits = log2_constexpr(max_level3_bufs);
  static constexpr int level4_bits = log2_constexpr(max_level4_bufs);

  // bit position variables. Depend upon num_nodes
  const int level1_pos;
  const int level2_pos;
  const int level3_pos;
  const int level4_pos;

  // Bits we use when placing in level1, 2, 3, 4
  // Level 5 will always be 0 if used
  const int positions[4] = {level1_pos, level2_pos, level3_pos, level4_pos};

  // variables for controlling the optional levels
  const size_t num_level3_bufs = 0;
  const size_t num_level4_bufs = 0;
  const size_t num_shared_levels = 0;

  // fanouts: L1->L2, L2->L3, L3->L4, L4->L5 (if not all 5 levels present then 0s)
  const size_t fanout_bits[4] = {(size_t) level1_pos - level2_pos,
                                 (size_t) level2_pos - level3_pos,
                                 (size_t) level3_pos - level4_pos,
                                 (size_t) level4_pos};

  // for identifying which child we're referencing
  const size_t fanout_masks[4] = {~(size_t(-1) << fanout_bits[0]),
                                  ~(size_t(-1) << fanout_bits[1]),
                                  ~(size_t(-1) << fanout_bits[2]),
                                  ~(size_t(-1) << fanout_bits[3])};


  struct FanoutMetaData {
    int bit_position;   // bits from this index and left = buffer index where we place upd
    size_t fanout_bits; // number of bits that determine the fanout
    size_t fanout_mask; // mask for determining which of our children a index corresponds to
  };

  FanoutMetaData get_level_update_metadata(int level) {
    return {positions[level - 1], fanout_bits[level - 2], fanout_masks[level - 2]};
  }

  // offset for insertion re-labelling
  node_id_t relabelling_offset = 0;

  template<size_t size>
  struct LocalGutter {
    std::array<update_t, size> data;
    size_t num_elms = 0;
    const size_t capacity = size;
  };

  // forward declarations
  class InsertThread;

  class SharedGutter {
   private:
    CacheGuttering &CGsystem;
   public:
    update_t *data;
    std::atomic<size_t> insert_pos;
    std::atomic<int> active_inserts;
    node_id_t index;
    const size_t capacity;
    const size_t level;

    // true init
    SharedGutter(CacheGuttering &CGsystem, size_t size, size_t level, size_t index)
        : CGsystem(CGsystem),
          data(new update_t[size]),
          insert_pos(0),
          active_inserts(0),
          index(index),
          capacity(size),
          level(level) {}
    ~SharedGutter() {
      delete[] data;
    }

    bool batch_insert(CacheGuttering::InsertThread &thr, SharedGutter *&gut_ptr,
                      const std::vector<update_t> &updates);
    void flush(InsertThread &thr, SharedGutter *&gut_ptr, size_t num_upd_flush,
               const std::vector<update_t> &updates);
  };

  class LeafGutter {
   private:
    CacheGuttering &CGsystem;
   public:
    std::vector<node_id_t> data;
    std::atomic<size_t> insert_pos;
    std::atomic<int> active_inserts;
    node_id_t index;
    const size_t capacity;

    LeafGutter(CacheGuttering &CGsystem, size_t size, size_t index)
        : CGsystem(CGsystem),
          data(size),
          insert_pos(0),
          active_inserts(0),
          index(index),
          capacity(size) {}

    bool batch_insert(CacheGuttering::InsertThread &thr, LeafGutter *&gut_ptr,
                      const std::vector<node_id_t> &updates);
    void flush(InsertThread &thr, LeafGutter *&gut_ptr, size_t num_upd_flush,
               const std::vector<node_id_t> &updates);
  };

  struct WQ_Buffer {
    std::vector<update_batch> batches;
    size_t size = 0;
  };

  class InsertThread {
   private:
    static constexpr size_t root_buffer_capacity = 256;
    size_t root_buffer_size = 0;
    CacheGuttering &CGsystem; // reference to associated CacheGuttering system

    // thread local gutters
    update_t root_buffer[root_buffer_capacity];
    std::array<LocalGutter<level1_elms_per_buf>, level1_bufs> level1_gutters;
    std::array<LocalGutter<level2_elms_per_buf>, level2_bufs> level2_gutters;

   public:
    InsertThread(CacheGuttering &CGsystem)
        : CGsystem(CGsystem),
          l3_insert_bufs(local_fanout),
          l4_insert_bufs(global_fanout),
          leaf_insert_bufs(global_fanout) {
      local_wq_buffer.batches.resize(CGsystem.wq_batch_per_elm);
      for (auto &batch : local_wq_buffer.batches)
        batch.upd_vec.reserve(CGsystem.leaf_gutter_size);

      for (auto &buf : l3_insert_bufs)
        buf.reserve(block_elms);
      for (auto &buf : l4_insert_bufs)
        buf.reserve(block_elms);
      for (auto &buf : leaf_insert_bufs)
        buf.reserve(block_leaf_elms);

      extra_level3_gutter = new SharedGutter(CGsystem, level3_elms_per_buf, 3, 0);
      extra_level4_gutter = new SharedGutter(CGsystem, level4_elms_per_buf, 4, 0);
      extra_leaf = new LeafGutter(CGsystem, CGsystem.leaf_gutter_size, 0);
    };

    ~InsertThread() {
      delete extra_level3_gutter;
      delete extra_level4_gutter;
      delete extra_leaf;
    }

    // Extra memory for quick flushes
    SharedGutter *extra_level3_gutter;
    SharedGutter *extra_level4_gutter;
    LeafGutter *extra_leaf;

    SharedGutter **extra_bufs[2] = {&extra_level3_gutter, &extra_level4_gutter};

    // buffers to avoid atomic on every update for shared levels
    std::vector<std::vector<update_t>> l3_insert_bufs;
    std::vector<std::vector<update_t>> l4_insert_bufs;
    std::vector<std::vector<node_id_t>> leaf_insert_bufs;

    // insert an update into the local buffers
    void insert(update_t upd);

    void batch_insert(const update_t *upds, size_t num_updates);
    void process_stream_upd_batch(const GraphStreamUpdate *upds, size_t num_updates);

    // flush a local buffer
    void flush_l1_buf(const node_id_t buf_idx);
    void flush_l2_buf(const node_id_t buf_idx);

    void flush_all(); // flush entire structure
    void wq_push_helper(node_id_t node_idx, LeafGutter &leaf, size_t exp_size);
    void flush_wq_buf();

    void batch_shared_insert(SharedGutter **gutters, const size_t buf_idx,
                             std::vector<update_t> &updates);
    void batch_leaf_insert(const size_t buf_idx, std::vector<node_id_t> &updates);

    // Buffer for performing batch push to work queue
    WQ_Buffer local_wq_buffer;

    // no copying for you
    InsertThread(const InsertThread &) = delete;
    InsertThread &operator=(const InsertThread &) = delete;
  
    // moving is allowed
    InsertThread (InsertThread &&) = default;
  };

  // buffers shared amongst all threads
  SharedGutter **level3_gutters = nullptr;
  SharedGutter **level4_gutters = nullptr;

  // final layer that holds node gutters
  LeafGutter **leaf_gutters;

  friend class InsertThread;

  std::vector<InsertThread> insert_threads; // vector of InsertThreads
 public:
  /**
   * Constructs a new guttering systems using a tree like structure for cache efficiency.
   * @param nodes       number of nodes in the graph.
   * @param workers     the number of workers which will be removing batches
   * @param inserters   the number of inserter buffers
   */
  CacheGuttering(node_id_t nodes, uint32_t workers, uint32_t inserters,
                 GutteringConfiguration conf);
  CacheGuttering(node_id_t nodes, uint32_t workers, uint32_t inserters) : 
    CacheGuttering(nodes, workers, inserters, GutteringConfiguration()) {};

  ~CacheGuttering();

  /**
   * Puts an update into the data structure.
   * @param upd the edge update.1
   * @param which, which thread is inserting this update
   * @return nothing.
   */
  insert_ret_t insert(const update_t &upd, size_t which) override { 
    assert(which < inserters);
    insert_threads[which].insert(upd);
  }

  insert_ret_t batch_insert(const update_t *batch, size_t num_updates, size_t which) override {
    assert(which < inserters);
    insert_threads[which].batch_insert(batch, num_updates);
  }

  insert_ret_t process_stream_upd_batch(const GraphStreamUpdate *batch, size_t num_updates,
                                        size_t which) override {
    assert(which < inserters);
    insert_threads[which].process_stream_upd_batch(batch, num_updates);
  }

  // pure virtual functions don't like default params, so default to 'which' of 0
  insert_ret_t insert(const update_t &upd) { insert_threads[0].insert(upd); }

  void flush_leaf(CacheGuttering::InsertThread &thr, LeafGutter *&gut_ptr,
                    const std::vector<node_id_t> &updates);

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
