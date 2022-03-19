#include <cassert>
#include <fstream>
#include "../include/standalone_gutters.h"

StandAloneGutters::StandAloneGutters(node_id_t num_nodes, int workers) : buffers(num_nodes) {
  configure_system(); // read buffering configuration file

  // size of leaf proportional to size of sketch
  buffer_size = gutter_factor * sketch_size(num_nodes) / sizeof(node_id_t);
  if (buffer_size < 4) buffer_size = 4;
  wq = new WorkQueue(workers * queue_factor, buffer_size);

  for (node_id_t i = 0; i < num_nodes; ++i) {
    buffers[i].reserve(buffer_size);
  }
}

StandAloneGutters::~StandAloneGutters() {
  delete wq;
}

insert_ret_t StandAloneGutters::insert(const update_t &upd) {
  std::vector<node_id_t> &ptr = buffers[upd.first];
  ptr.push_back(upd.second);
  if (ptr.size() == buffer_size) { // full, so request flush
    wq->push(upd.first, ptr);
    ptr.clear();
  }
}

flush_ret_t StandAloneGutters::force_flush() {
  for (node_id_t node_idx = 0; node_idx < buffers.size(); node_idx++) {
    if (buffers[node_idx].size() > 1) { // have stuff to flush
      wq->push(node_idx, buffers[node_idx]);
      buffers[node_idx].clear();
    }
  }
}
