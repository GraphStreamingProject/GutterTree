#include "numa_pht.h"
#include <exception>

inline static node_id_t extract_left_bits(node_id_t number, size_t pos) {
  return number >> pos;
}


NumaPHT::NumaPHT(node_id_t num_gutters, size_t num_consumers, size_t num_inserters,
                 size_t num_trees, GutteringConfiguration conf)
    : GutteringSystem(num_gutters, num_consumers, conf),
      num_gutters(num_gutters),
      inserters(num_inserters),
      num_trees(num_trees),
      tree_bits(log2(num_trees)) {

  if (__builtin_popcount(num_trees) != 1) {
    throw std::invalid_argument("ERROR: NumaPHT -- Number of trees must be a power of 2.");
  }

  if (inserters < num_trees) {
    throw std::invalid_argument(
        "ERROR: NumaPHT -- Number of inserter threads must be greater than number of trees");
  }

  if (inserters % num_trees != 0) {
    throw std::invalid_argument(
        "ERROR: NumaPHT -- Number of inserter threads must be muiltiple of number of trees");
  }

  phts = new CacheGuttering *[num_trees];

  for (size_t i = 0; i < num_trees; i++) {
    phts[i] =
        new CacheGuttering(num_gutters / num_trees, num_consumers, num_inserters / num_trees, conf);
    phts[i]->set_offset(float(i) * num_gutters / num_trees);
  }

  // create buffers for insertion threads
  thread_buffers = new ThreadBuffer[num_inserters * num_trees];

  tree_queues = new WorkQueue<ThreadBuffer> *[num_trees];
  for (size_t i = 0; i < num_trees; i++) {
    tree_queues[i] = new WorkQueue<ThreadBuffer>(num_inserters);
  }
}

NumaPHT::~NumaPHT() {
  for (size_t i = 0; i < num_trees; i++) {
    delete phts[i];
    delete tree_queues[i];
  }
  delete[] phts;
  delete[] tree_queues;
  delete[] thread_buffers;
}

insert_ret_t NumaPHT::insert(const update_t &upd, size_t thr_id) {
  // first, map this update to which of our trees we want to use
  node_id_t which_tree = extract_left_bits(upd.first, tree_bits);
  size_t thr_tree = thr_id >> tree_bits;

  // the thread id the appropriate tree knows this thread by
  size_t tree_thr_id = thr_id & ((1 << tree_bits) - 1);

  // if its our tree, apply the update
  if (which_tree == thr_tree) {
    phts[which_tree]->insert(upd, thr_id);
  }

  // otherwise, place into appropriate buffer
  auto &buf = thread_buffers[thr_id * num_trees + which_tree];
  buf.buffer[buf.size++] = upd;
  if (buf.size >= buf.capacity) {
    // swap our buffer for other tree with a fresh buffer and add to that queue
    tree_queues[which_tree]->push(buf);
    buf.size = 0;
  }

  WorkQueue<ThreadBuffer>::DataNode *data;
  if (tree_queues[thr_tree]->pop(data)) {
    // process this data from the queue
    phts[thr_tree]->batch_insert(data->get_data().buffer, data->get_data().size, thr_id);

    tree_queues[thr_tree]->pop_callback(data);
  }
}

insert_ret_t NumaPHT::batch_insert(const update_t *batch, size_t num_updates, size_t thr_id) {
  for (size_t i = 0; i < num_updates; i++) {
    insert(batch[i], thr_id);
  }
}

insert_ret_t NumaPHT::process_stream_upd_batch(const GraphStreamUpdate *batch, size_t num_updates,
                                               size_t thr_id) {
  for (size_t i = 0; i < num_updates; i++) {
    const GraphStreamUpdate &upd = batch[i];
    insert({upd.edge.src, upd.edge.dst}, thr_id);
  }
}

insert_ret_t NumaPHT::force_flush() {
  // 1. move all data in thread buffers to queues
  for (size_t i = 0; i < inserters; i++) {
    for (size_t t = 0; t < num_trees; t++) {
      if (thread_buffers[i * num_trees + t].size > 0) {
        tree_queues[t]->push(thread_buffers[i * num_trees + t]);
      }
    }
  }

  // 2. move all data out of queues
  size_t thr_id = 0;
  for (size_t i = 0; i < num_trees; i++) {
    WorkQueue<ThreadBuffer>::DataNode *data;
    while (tree_queues[i]->pop(data)) {
      phts[i]->batch_insert(data->get_data().buffer, data->get_data().size, thr_id);
      tree_queues[i]->pop_callback(data);
    }
  }

  // 3. flush each tree
  for (size_t i = 0; i < num_trees; i++) {
    phts[i]->force_flush();
  }
}
