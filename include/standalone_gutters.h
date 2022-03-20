#pragma once
#include "work_queue.h"
#include "types.h"
#include "guttering_system.h"

/**
 * In-memory wrapper to offer the same interface as a buffer tree.
 */
class StandAloneGutters : public GutteringSystem {
private:
  std::vector<std::vector<node_id_t>> buffers; // array dump of numbers for performance:
                                               // DO NOT try to access directly!

public:
  /**
   * Constructs a new .
   * @param size        the total length of a buffer, in updates.
   * @param nodes       number of nodes in the graph.
   * @param workers     the number of workers which will be removing batches
   */
  StandAloneGutters(node_id_t nodes, int workers);

  /**
   * Puts an update into the data structure.
   * @param upd the edge update.
   * @return nothing.
   */
  insert_ret_t insert(const update_t &upd);

  /**
   * Flushes all pending buffers.
   * @return nothing.
   */
  flush_ret_t force_flush();
};