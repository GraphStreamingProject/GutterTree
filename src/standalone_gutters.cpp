#include <cassert>
#include <fstream>
#include "../include/standalone_gutters.h"

StandAloneGutters::StandAloneGutters(node_id_t num_nodes, int workers) : 
 GutteringSystem(num_nodes, workers), buffers(num_nodes) {
  for (node_id_t i = 0; i < num_nodes; ++i) {
    buffers[i].reserve(leaf_gutter_size);
  }
}

insert_ret_t StandAloneGutters::insert(const update_t &upd) {
  std::vector<node_id_t> &ptr = buffers[upd.first];
  ptr.push_back(upd.second);
  if (ptr.size() == leaf_gutter_size) { // full, so request flush
    wq.push(upd.first, ptr);
    ptr.clear();
  }
}

flush_ret_t StandAloneGutters::force_flush() {
  for (node_id_t node_idx = 0; node_idx < buffers.size(); node_idx++) {
    if (!buffers[node_idx].empty()) { // have stuff to flush
      wq.push(node_idx, buffers[node_idx]);
      buffers[node_idx].clear();
    }
  }
}
