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
  static constexpr size_t l1_cache_size   = 32768;    // l1 cache bytes per cpu
  static constexpr size_t l2_cache_size   = 1048576;  // l2 cache bytes per cpu
  static constexpr size_t l3_cache_size   = 33554432; // l3 cache bytes in total

  static constexpr size_t cache_sizes[3] = {32768, 1048576, 33554432};
  static constexpr size_t cache_line      = 64; // number of bytes in a cache_line
  static constexpr size_t divisor         = 1;
  
  static constexpr size_t buf_bytes[4] = {cache_line, 2*cache_line, 4*cache_line, 16*cache_line};
  static constexpr size_t buf_elems[4] = {buf_bytes[0] / sizeof(update_t), buf_bytes[1] / sizeof(update_t),
    buf_bytes[2] / sizeof(update_t), buf_bytes[3] / sizeof(update_t)};

  static constexpr size_t num_bufs[3] = {cache_sizes[0] / (buf_bytes[0] * divisor), cache_sizes[1] / (buf_bytes[1] * divisor), 
    cache_sizes[2] / (buf_bytes[2] * divisor)};
  static constexpr size_t max_RAM1_bufs = num_bufs[2] * 32;
  
  static constexpr int bits[4] = {log2_constexpr(num_bufs[0]), log2_constexpr(num_bufs[1]), 
    log2_constexpr(num_bufs[2]), log2_constexpr(max_RAM1_bufs)};

  // bit position variables. Depend upon num_nodes
  const int l1_pos;
  const int l2_pos;
  const int l3_pos;
  const int RAM1_pos;

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
    CacheGuttering &CGsystem; // reference to associated CacheGuttering system

    // thread local gutters
    std::array<Cache_Gutter<buf_elems[0]>, num_bufs[0]> l1_gutters;
    std::array<Cache_Gutter<buf_elems[1]>, num_bufs[1]> l2_gutters;
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
  };

  // locks for flushing L2 buffers
  std::mutex *L2_flush_locks;

  // buffers shared amongst all threads
  std::array<Cache_Gutter<buf_elems[2]>, num_bufs[2]> l3_gutters; // shared cache layer in L3
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
   * @param upd the edge update.1
   * @param which, which thread is inserting this update
   * @return nothing.
   */
  insert_ret_t insert(const update_t &upd, int which) { 
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

  /*
   * Helper function for tracing a root to leaf path. Prints path to stdout
   * @param src   the node id to trace
   */
  void print_r_to_l(node_id_t src);
  void print_fanouts();
};
