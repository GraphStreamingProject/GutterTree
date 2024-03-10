#pragma once
#include <cmath>
#include <iostream>

#include "guttering_configuration.h"
#include "types.h"
#include "work_queue.h"

class GutteringSystem {
 public:
  // Constructor for programmatic configuration
  GutteringSystem(node_id_t num_nodes, int workers, GutteringConfiguration conf,
                  bool page_slots = false)
      : page_size((conf.set_defaults())._page_size),  // set defaults first to default init params
        buffer_size(conf._buffer_size),
        fanout(conf._fanout),
        num_flushers(conf._num_flushers),
        queue_factor(conf._queue_factor),
        wq_batch_per_elm(conf._wq_batch_per_elm),
        num_nodes(num_nodes),
        group_gutter_size(conf._gutter_bytes / sizeof(node_id_t)),
        wq(workers * queue_factor,
           page_slots ? group_gutter_size + page_size / sizeof(node_id_t) : group_gutter_size,
           wq_batch_per_elm) {
    std::cout << conf << std::endl;
  }
  virtual ~GutteringSystem(){};

  // insert an element to the guttering system
  // optionally, define insert to include the thread id
  virtual insert_ret_t insert(const update_t &upd) = 0;
  virtual insert_ret_t insert(const update_t &upd, size_t thr) {
    insert(upd);
    (void)(thr);
  }

  // force all data out of buffers
  virtual flush_ret_t force_flush() = 0;

  // get the size of a work queue elmement in bytes
  // size_t gutter_size() { return leaf_gutter_size * sizeof(node_id_t); }
  size_t group_gutter_size() {return group_gutter_size * sizeof(node_id_t);};

  // get data out of the guttering system either one gutter at a time or in a batched fashion
  bool get_data(WorkQueue::DataNode *&data) { return wq.peek(data); }
  void get_data_callback(WorkQueue::DataNode *data) { wq.peek_callback(data); }
  void set_non_block(bool block) { wq.set_non_block(block); }  // set non-blocking calls in wq
 protected:
  // parameters of the GutteringSystem, defined by the GutteringConfiguration param or config file
  const size_t page_size;         // guttertree -- write granularity
  const size_t buffer_size;       // guttertree -- internal node buffer size
  const size_t fanout;            // guttertree -- max children per node
  const size_t num_flushers;      // guttertree -- the number of flush threads
  const size_t queue_factor;      // total number of batches in queue is this factor * num_workers
  const size_t wq_batch_per_elm;  // number of batches each queue element holds

  const node_id_t num_nodes;
  // const node_id_t leaf_gutter_size;
  const node_id_t group_gutter_size;
  WorkQueue wq;
};
