#include "numa_pht.h"
#include <exception>

thread_local size_t NumaPHT::thr_insert_num;

inline static node_id_t extract_left_bits(node_id_t number, size_t pos) {
  return number >> pos;
}


NumaPHT::NumaPHT(node_id_t num_gutters, size_t num_consumers, size_t num_inserters,
                 size_t num_trees, GutteringConfiguration conf)
    : PipelineHyperTree(num_gutters, num_consumers, num_inserters, conf),
      num_gutters(num_gutters),
      num_inserters(num_inserters),
      num_trees(num_trees),
      tree_bits(log2(num_trees)),
      vert_bits(ceil(log2(num_gutters))) {

  std::cout << "tree_bits = " << tree_bits << std::endl;
  std::cout << "vert_bits = " << vert_bits << std::endl;

  if (__builtin_popcount(num_trees) != 1) {
    throw std::invalid_argument("ERROR: NumaPHT -- Number of trees must be a power of 2.");
  }

  if (num_inserters < num_trees) {
    throw std::invalid_argument(
        "ERROR: NumaPHT -- Number of inserter threads must be greater than number of trees");
  }

  if (num_inserters % num_trees != 0) {
    throw std::invalid_argument(
        "ERROR: NumaPHT -- Number of inserter threads must be muiltiple of number of trees");
  }

  // create buffers for insertion threads
  thread_buffers = new ThreadBuffer[num_inserters * num_trees];

  tree_queues = new WorkQueue<ThreadBuffer> *[num_trees];
  for (size_t i = 0; i < num_trees; i++) {
    tree_queues[i] = new WorkQueue<ThreadBuffer>(num_inserters * 4);
    tree_queues[i]->set_non_block(true); // tree queues are non-blocking
  }
}

NumaPHT::~NumaPHT() {
  for (size_t i = 0; i < num_trees; i++) {
    delete tree_queues[i];
  }
  delete[] tree_queues;
  delete[] thread_buffers;
}

insert_ret_t NumaPHT::insert(const update_t &upd, size_t thr_id) {
  // std::cout << "insert" << std::endl;
  // first, map this update to which of our trees we want to use
  node_id_t which_tree = extract_left_bits(upd.first, vert_bits - tree_bits);
  size_t thr_tree = thr_id >> (tree_bits - 1);

  // std::cout << "thr_id = " << thr_id << " which tree = " << which_tree << " thr_tree = " << thr_tree << std::endl;

  // if its our tree, apply the update
  if (which_tree == thr_tree) {
    PipelineHyperTree::insert(upd, thr_id);
  } else {
    // std::cout << "buffer" << std::endl;
    // otherwise, place into appropriate buffer
    auto &buf = thread_buffers[thr_id * num_trees + which_tree];
    buf.buffer[buf.size++] = upd;
    if (buf.size >= buf.capacity) {
      // swap our buffer for other tree with a fresh buffer and add to that queue
      if (!tree_queues[which_tree]->push(buf, false)) {
        // tree queue is full, so handle the buffer ourselves
        PipelineHyperTree::batch_insert(buf.buffer, buf.size, thr_id);
      }
      buf.size = 0; 
    }
  }

  // check tree queue every 255 updates
  if ((++thr_insert_num & 0xFF) == 0) {
    WorkQueue<ThreadBuffer>::DataNode *data;
    while (tree_queues[thr_tree]->pop(data)) {
      // process this data from the queue
      PipelineHyperTree::batch_insert(data->get_data().buffer, data->get_data().size, thr_id);

      tree_queues[thr_tree]->pop_callback(data);
    }
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
  size_t thr_id = 0;
  for (size_t i = 0; i < num_inserters; i++) {
    for (size_t t = 0; t < num_trees; t++) {
      auto& buf = thread_buffers[i * num_trees + t];
      if (buf.size > 0) {
        PipelineHyperTree::batch_insert(buf.buffer, buf.size, thr_id);
        buf.size = 0;
      }
    }
  }

  // 2. move all data out of queues
  for (size_t i = 0; i < num_trees; i++) {
    WorkQueue<ThreadBuffer>::DataNode *data;
    while (tree_queues[i]->pop(data)) {
      PipelineHyperTree::batch_insert(data->get_data().buffer, data->get_data().size, thr_id);
      tree_queues[i]->pop_callback(data);
    }
  }

  // 3. flush each tree
  PipelineHyperTree::force_flush();
}
