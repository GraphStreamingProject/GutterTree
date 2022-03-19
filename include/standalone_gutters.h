#pragma once
#include "work_queue.h"
#include "types.h"
#include "guttering_system.h"

/**
 * In-memory wrapper to offer the same interface as a buffer tree.
 */
class StandAloneGutters : public GutteringSystem {
private:
  uint32_t buffer_size; // size of a buffer (including metadata)
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

  ~StandAloneGutters();

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

  /*
   * Access the size of a leaf gutter through the GutteringSystem abstract class
   */
  int upds_per_gutter() { return buffer_size / sizeof(node_id_t); }
};