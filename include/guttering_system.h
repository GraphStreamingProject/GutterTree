#pragma once
#include <cmath>
#include <iostream>

#include "guttering_configuration.h"
#include "types.h"
#include "work_queue.h"
#include "stream_types.h"

struct update_batch {
  node_id_t node_idx;
  std::vector<node_id_t> upd_vec;
};

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
        leaf_gutter_size(conf._gutter_bytes / sizeof(node_id_t)),
        wq(workers * queue_factor, wq_batch_per_elm) {
    size_t batch_len =
        page_slots ? leaf_gutter_size + page_size / sizeof(node_id_t) : leaf_gutter_size;
    std::vector<std::vector<update_batch>> wq_data;
    for (size_t i = 0; i < workers * queue_factor; i++) {
      wq_data.push_back(std::vector<update_batch>(wq_batch_per_elm));
      for (size_t j = 0; j < wq_batch_per_elm; j++) {
        wq_data[i][j].upd_vec.reserve(batch_len);
      }
    }
    wq.populate_queue(wq_data);

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

  // optionally define a function that applies many updates at once
  // otherwise, just calls insert repeatedly
  virtual insert_ret_t batch_insert(const update_t *batch, size_t num_updates, size_t thr) {
    for (size_t i = 0; i < num_updates; i++) {
      insert(batch[i], thr);
    }
  }

  // Specialized function for processing GraphStream objects
  // optionally define a function that applies many GraphStreamUpdates
  // otherwise, just calls insert repeatedly
  virtual insert_ret_t process_stream_upd_batch(const GraphStreamUpdate *batch, size_t num_updates,
                                         size_t thr) {
    for (size_t i = 0; i < num_updates; i++) {
      insert({batch[i].edge.src, batch[i].edge.dst}, thr);
      insert({batch[i].edge.dst, batch[i].edge.src}, thr);
    }
  }

  // force all data out of buffers
  virtual flush_ret_t force_flush() = 0;

  // get the size of a work queue elmement in bytes
  size_t gutter_size() { return leaf_gutter_size * sizeof(node_id_t); }

  // get data out of the guttering system either one gutter at a time or in a batched fashion
  bool get_data(WorkQueue<update_batch>::DataNode *&data) { return wq.pop(data); }
  void get_data_callback(WorkQueue<update_batch>::DataNode *data) { wq.pop_callback(data); }
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
  const node_id_t leaf_gutter_size;
  WorkQueue<update_batch> wq;
};
