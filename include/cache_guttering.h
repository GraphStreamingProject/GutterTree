#include "guttering_system.h"
#include <array>

class CacheGuttering : public GutteringSystem {
private:
  size_t inserters;
  node_id_t num_nodes;

  // TODO: use cmake to establish some compiler constants for these variables
  // currently these are the values for evan's laptop ;)
  static constexpr size_t l1_cache_size   = 32768;    // l1 cache bytes per cpu
  static constexpr size_t l2_cache_size   = 1048576;  // l2 cache bytes per cpu
  static constexpr size_t l3_cache_size   = 33554432; // l3 cache bytes in total
  static constexpr size_t cache_line      = 64; // number of bytes in a cache_line
  static constexpr size_t cache_bytes_per_child = 2 * cache_line;
  static constexpr size_t RAM_bytes_per_child   = 8 * cache_line;

  // basic 'tree' params, hardcoded for now. TODO: Determine by sizes later
  static constexpr size_t fanout          = 32; // fanout for cache level gutters. Must be power of 2
  static constexpr size_t num_l1_bufs     = 8;
  static constexpr size_t num_l2_bufs     = 256;
  static constexpr size_t num_l3_bufs     = 8192;
  static constexpr size_t max_RAM1_bufs   = num_l3_bufs * fanout;
  static constexpr size_t l1l2buffer_elms = cache_bytes_per_child * fanout / sizeof(update_t);
  static constexpr size_t l3buffer_elms   = cache_bytes_per_child * fanout / sizeof(update_t);

  // bit length variables
  static constexpr int l1_bits   = ceil(log2(num_l1_bufs));
  static constexpr int l2_bits   = ceil(log2(num_l2_bufs));
  static constexpr int l3_bits   = ceil(log2(num_l3_bufs));
  static constexpr int RAM1_bits = ceil(log2(max_RAM1_bufs));

  // bit position variables. Depend upon num_nodes
  const int l1_pos;
  const int l2_pos;
  const int l3_pos;
  const int RAM1_pos;

  // some params for RAM1 if it is necessary
  size_t RAM1_fanout   = 0;
  size_t RAM1_buf_elms = 0;

  using RAM_Gutter  = std::vector<update_t>;
  using Leaf_Gutter = std::vector<node_id_t>;
  template <size_t num_slots>
  struct Cache_Gutter {
    std::array<update_t, num_slots> data;
    size_t num_elms = 0;
  };

  class InsertThread {
  private:
    CacheGuttering &CGsystem;

    std::array<Cache_Gutter<l1l2buffer_elms>, num_l1_bufs> l1_gutters;
    std::array<Cache_Gutter<l1l2buffer_elms>, num_l2_bufs> l2_gutters;
  public:
    InsertThread(CacheGuttering &CGsystem) : CGsystem(CGsystem) {};

    // insert an update into the local buffers
    void insert(const update_t &upd);

    // functions for flushing local buffers
    void flush_buf_l1(const node_id_t idx);
    void flush_buf_l2(const node_id_t idx);

    // no copying for you
    InsertThread(const InsertThread &) = delete;
    InsertThread &operator=(const InsertThread &) = delete;
  
    // moving is allowed
    InsertThread (InsertThread &&) = default;

    std::array<int, num_l2_bufs> l2_flush_counts;
  };

  // locks for flushing L2 buffers
  std::mutex *L2_flush_locks;

  // buffers shared amongst all threads
  std::array<Cache_Gutter<l3buffer_elms>, num_l3_bufs> l3_gutters; // shared cache layer in L3
  RAM_Gutter *RAM1_gutters = nullptr; // additional RAM layer if necessary
  Leaf_Gutter *leaf_gutters;          // final layer that holds node gutters

  friend class InsertThread;

  std::vector<InsertThread> insert_threads; // vector of InsertThreads

  void flush_buf_l3(const node_id_t idx);
  void flush_RAM_l1(const node_id_t idx);
public:
  /**
   * Constructs a new guttering systems using a tree like structure for cache efficiency.
   * @param nodes       number of nodes in the graph.
   * @param workers     the number of workers which will be removing batches
   * @param inserters   the number of inserter buffers
   */
  CacheGuttering(node_id_t nodes, uint32_t workers, uint32_t inserters);

  ~CacheGuttering();

  /**
   * Puts an update into the data structure.
   * @param upd the edge update.
   * @param which, which thread is inserting this update
   * @return nothing.
   */
  insert_ret_t insert(const update_t &upd, int which) { insert_threads[which].insert(upd); }
  
  // pure virtual functions don't like default params, so default to 'which' of 0
  insert_ret_t insert(const update_t &upd) { insert_threads[0].insert(upd); }

  /**
   * Flushes all pending buffers. When this function returns there are no more updates in the
   * guttering system
   * @return nothing.
   */
  flush_ret_t force_flush();

  /*
   * Helper function for printing root to leaf paths
   */
  void print_r_to_l(node_id_t src);
};
