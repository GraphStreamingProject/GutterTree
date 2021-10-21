#pragma once
#include "work_queue.h"
#include "types.h"

/**
 * In-memory wrapper to offer the same interface as a buffer tree.
 */
class StandAloneGutters {
private:
  uint32_t buffer_size; // size of a buffer (including metadata)
  WorkQueue *wq;
  std::vector<node_id_t*> buffers; // array dump of numbers for performance: DO NOT
  // try to access directly!

  /**
   * Flushes the corresponding buffer to the queue.
   * @param buffer      a pointer to the head of the buffer to flush.
   * @param num_bytes   the number of bytes to flush.
   */
  void flush(node_id_t* buffer, uint32_t num_bytes);

  /**
   * Use buffering.conf configuration file to determine parameters of the StandAloneGutters
   * Sets the following variables
   * Queue_Factor :   The number of queue slots per worker removing data from the queue
   * Size_Factor  :   Decrease the amount of bytes used per node by this multiplicative factor
   */
  void configure();
public:
  /**
   * Constructs a new .
   * @param size        the total length of a buffer, in updates.
   * @param nodes       number of nodes in the graph.
   * @param workers     the number of workers which will be removing batches
   */
  StandAloneGutters(Node nodes, int workers);

  ~StandAloneGutters();

  /**
   * Puts an update into the data structure.
   * @param upd the edge update.
   * @return nothing.
   */
  insert_ret_t insert(update_t upd);

  /**
   * Ask the buffer queue for data and sleep if necessary until it is available.
   * @param data        to store the fetched data.
   * @return            true if got valid data, false if unable to get data.
   */
  bool get_data(data_ret_t& data);

  /**
   * Flushes all pending buffers.
   * @return nothing.
   */
  flush_ret_t force_flush();

  /**
   * Notifies all threads waiting on condition variables that they should
   * check their wait condition again. Useful when switching from blocking to
   * non-blocking calls to the circular queue.
   * For example: we set this to true when shutting down the graph_workers.
   * @param    block is true if we should turn on non-blocking operations
   *           and false if we should turn them off.
   */
  void set_non_block(bool block);

  static const uint32_t serial_update_size = sizeof(node_id_t);
  static uint32_t gutter_factor;
  static uint32_t queue_factor;
};
