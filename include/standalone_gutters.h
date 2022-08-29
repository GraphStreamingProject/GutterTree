#pragma once
#include "work_queue.h"
#include "types.h"
#include "guttering_system.h"

/**
 * In-memory wrapper to offer the same interface as a buffer tree.
 */
class StandAloneGutters : public GutteringSystem {
private:
  struct Gutter { 
    std::mutex mux;
    std::vector<node_id_t> buffer;
  };
  static constexpr uint8_t local_buf_size = 8;
  struct LocalGutter {
		uint8_t count = 0;
    node_id_t buffer[local_buf_size];
  };
  uint32_t buffer_size; // size of a buffer (including metadata)
  std::vector<Gutter> gutters; // gutters containing updates
  const uint32_t inserters;
  std::vector<std::vector<LocalGutter>> local_buffers; // array dump of numbers for performance:
  
  /**
   * Puts an update into the data structure from the local buffer. Must hold both buffer locks
   * @param upd the edge update.
   * @return nothing.
   */
  insert_ret_t insert_batch(size_t which, node_id_t gutterid);

public:
  /**
   * Constructs a new guttering systems using only leaf gutters.
   * @param nodes       number of nodes in the graph.
   * @param workers     the number of workers which will be removing batches
   * @param inserters   the number of inserter buffers
   */
  StandAloneGutters(node_id_t nodes, uint32_t workers, uint32_t inserters, 
    const GutteringConfiguration &conf);
  StandAloneGutters(node_id_t nodes, uint32_t workers, uint32_t inserters) : 
    StandAloneGutters(nodes, workers, inserters, GutteringConfiguration()) {};

  /**
   * Puts an update into the data structure.
   * @param upd the edge update.
   * @return nothing.
   */
  insert_ret_t insert(const update_t &upd, size_t which);
  // pure virtual functions don't like default params
  insert_ret_t insert(const update_t &upd);

  /**
   * Flushes all pending buffers.
   * @return nothing.
   */
  flush_ret_t force_flush();
};
