#include "guttering_system.h"
#include <array>

class CacheGuttering : public GutteringSystem {
private:
  // TODO: use cmake to establish some compiler constants for these variables
  // currently these are the values for evan's laptop ;)
  static constexpr size_t l1_cache_size   = 32768;   // l1 cache bytes
  static constexpr size_t l2_cache_size   = 262144;  // l2 cache bytes
  static constexpr size_t l3_cache_size   = 8388608; // l3 cache bytes
  static constexpr size_t cache_line      = 64; // number of bytes in a cache_line

  // basic 'tree' params, hardcoded for now. TODO: Determined by sizes later
  static constexpr size_t fanout          = 32;
  static constexpr size_t bytes_per_child = 2 * cache_line;
  static constexpr size_t num_l1_bufs     = 2;
  static constexpr size_t num_l2_bufs     = 64;
  static constexpr size_t num_l3_bufs     = 2048;
  static constexpr size_t max_RAM1_bufs   = num_l3_bufs * fanout;
  static constexpr size_t buffer_elms     = 4096 / sizeof(update_t);

  struct Shared_Cache_Gutter {
    std::mutex mux;
    std::array<update_t, buffer_elms> buffer;
    size_t num_elms = 0;
  };
  using RAM_Gutter  = std::vector<update_t>;
  using Leaf_Gutter = std::vector<node_id_t>;

  class InsertThread {
  private:
    struct Cache_Gutter {
      std::array<update_t, buffer_elms> buffer;
      size_t num_elms = 0;
    };

    std::array<Cache_Gutter, num_l1_bufs> l1_buffer;
    std::array<Cache_Gutter, num_l2_bufs> l2_buffer;
  public:
    InsertThread(int thr_id);

    // insert an update into the local buffers
    void insert(const update_t &upd);

    // helpers for force_flush for local buffers
    void flush_all_l1();
    void flush_buf_l2(const size_t idx);
  };

  // buffers shared amongst all threads
  static std::array<Shared_Cache_Gutter, num_l3_bufs> l3_buffer; // shared cache layer in L3
  static std::unique_ptr<RAM_Gutter[]> RAM1_buffer;        // additional RAM layer if necessary
  static std::unique_ptr<Leaf_Gutter[]> leaf_buffers;      // final layer that holds node gutters

  friend class InsertThread;
public:
  /**
   * Constructs a new guttering systems using a tree like structure for cache efficiency.
   * @param nodes       number of nodes in the graph.
   * @param workers     the number of workers which will be removing batches
   * @param inserters   the number of inserter buffers
   */
  CacheGuttering(node_id_t nodes, uint32_t workers, uint32_t inserters);

  /**
   * Puts an update into the data structure.
   * @param upd the edge update.
   * @param which, which thread is inserting this update
   * @return nothing.
   */
  insert_ret_t insert(const update_t &upd, int which);
  
  // pure virtual functions don't like default params, so call with default 'which' of 0
  insert_ret_t insert(const update_t &upd) { insert(upd, 0); }

  /**
   * Flushes all pending buffers.
   * @return nothing.
   */
  flush_ret_t force_flush();
};
