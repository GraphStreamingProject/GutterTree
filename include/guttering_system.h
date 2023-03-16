#pragma once
#include "types.h"
#include "work_queue.h"
#include "guttering_configuration.h"
#include <cmath>
#include <iostream>

class GutteringSystem {
public:
  // Constructor for programmatic configuration
  GutteringSystem(node_id_t num_nodes, int workers, const GutteringConfiguration &conf, bool page_slots=false) : 
   page_size(conf._page_size), buffer_size(conf._buffer_size), 
   fanout(conf._fanout), num_flushers(conf._num_flushers), gutter_factor(conf._gutter_factor),
   queue_factor(conf._queue_factor), wq_batch_per_elm(conf._wq_batch_per_elm),
   leaf_gutter_size(std::max((int)(conf._gutter_factor * upds_per_sketch(num_nodes)), 1)),
   wq(workers * queue_factor,
    page_slots ? leaf_gutter_size + page_size / sizeof(node_id_t) : leaf_gutter_size, 
    wq_batch_per_elm) {}
  
  virtual ~GutteringSystem() {};
  virtual insert_ret_t insert(const update_t &upd) = 0; //insert an element to the guttering system
  virtual insert_ret_t insert(const update_t &upd, size_t which) {insert(upd);(void)(which);}; //discard second argument by default
  virtual flush_ret_t force_flush() = 0;                //force all data out of buffers

  // get the size of a work queue elmement in bytes
  int gutter_size() {
    return leaf_gutter_size * sizeof(node_id_t);
  }

  // returns the number of node_id_t types that fit in a sketch
  static size_t upds_per_sketch(node_id_t num_nodes) {
    return 18 * pow(log2(num_nodes), 2) / (log2(3) - 1);
  }

  // get data out of the guttering system either one gutter at a time or in a batched fashion
  bool get_data(WorkQueue::DataNode *&data) { return wq.peek(data); }
  void get_data_callback(WorkQueue::DataNode *data) { wq.peek_callback(data); }
  void set_non_block(bool block) { wq.set_non_block(block);} //set non-blocking calls in wq
protected:
  // parameters of the GutteringSystem, defined by the GutteringConfiguration param or config file
  const uint32_t page_size;      // guttertree -- write granularity
  const uint32_t buffer_size;    // guttertree -- internal node buffer size
  const uint32_t fanout;         // guttertree -- max children per node
  const uint32_t num_flushers;   // guttertree -- the number of flush threads
  const float gutter_factor;     // factor which increases/decreases the leaf gutter size
  const uint32_t queue_factor;   // total number of batches in queue is this factor * num_workers
  const size_t wq_batch_per_elm; // number of batches each queue element holds

  node_id_t leaf_gutter_size;
  WorkQueue wq;
};
