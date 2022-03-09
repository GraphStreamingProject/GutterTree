#include <cassert>
#include <fstream>
#include "../include/standalone_gutters.h"

StandAloneGutters::StandAloneGutters(node_id_t num_nodes, int workers) : buffers(num_nodes) {
  configure(); // read buffering configuration file

  // size of leaf proportional to size of sketch (add 1 because we have 1 metadata slots per buffer)
  uint32_t bytes_size = floor(gutter_factor * sketch_size(num_nodes)) + sizeof(node_id_t);
  buffer_size = bytes_size / sizeof(node_id_t);

  wq = new WorkQueue(workers * queue_factor, bytes_size);

  for (node_id_t i = 0; i < num_nodes; ++i) {
    buffers[i].reserve(buffer_size);
    buffers[i].push_back(i); // second spot identifies the node to which the buffer
  }
}

StandAloneGutters::~StandAloneGutters() {
  delete wq;
}

// Read the configuration file to determine a variety of Buffering params
void StandAloneGutters::configure() {
  int queue_f  = 2;
  float gutter_f = 1;

  std::string line;
  std::ifstream conf("./buffering.conf");
  if (conf.is_open()) {
    while(getline(conf, line)) {
      if (line[0] == '#' || line[0] == '\n') continue;
      if(line.substr(0, line.find('=')) == "queue_factor") {
        queue_f = std::stoi(line.substr(line.find('=') + 1));
        if (queue_f > 16 || queue_f < 1) {
          printf("WARNING: queue_factor out of bounds [1,16] using default(2)\n");
          queue_f = 2;
        }
      }
      if(line.substr(0, line.find('=')) == "gutter_factor") {
        gutter_f = std::stof(line.substr(line.find('=') + 1));
        if (gutter_f < 1 && gutter_f > -1) {
          printf("WARNING: gutter_factor must be outside of range -1 < x < 1 using default(1)\n");
          gutter_f = 1;
        }
      }
    }
  } else {
    printf("WARNING: Could not open buffering configuration file! Using default setttings.\n");
  }
  queue_factor  = queue_f;
  gutter_factor = gutter_f;
  if (gutter_factor < 0)
    gutter_factor = 1 / (-1 * gutter_factor); // gutter factor reduces size if negative
}

void StandAloneGutters::flush(std::vector<node_id_t> &buffer, uint32_t num_bytes) {
  wq->push(reinterpret_cast<char *>(buffer.data()), num_bytes);
}

insert_ret_t StandAloneGutters::insert(const update_t &upd) {
  std::vector<node_id_t> &ptr = buffers[upd.first];
  ptr.push_back(upd.second);
  if (ptr.size() == buffer_size) { // full, so request flush
    flush(ptr, buffer_size*sizeof(node_id_t));
    ptr.clear();
    ptr.push_back(upd.first);
  }
}

// basically a copy of BufferTree::get_data()
bool StandAloneGutters::get_data(data_ret_t &data) {
  // make a request to the circular buffer for data
  std::pair<int, queue_elm> queue_data;
  bool got_data = wq->peek(queue_data);

  if (!got_data)
    return false; // we got no data so return not valid

  int i         = queue_data.first;
  queue_elm elm = queue_data.second;
  auto *serial_data = reinterpret_cast<node_id_t *>(elm.data);
  uint32_t len      = elm.size;
  assert(len % sizeof(node_id_t) == 0);

  if (len == 0)
    return false; // we got no data so return not valid

  // assume the first key is correct so extract it
  node_id_t key = serial_data[0];
  data.first = key;

  data.second.clear(); // remove any old data from the vector
  uint32_t vec_len  = len / sizeof(node_id_t);
  data.second.reserve(vec_len); // reserve space for our updates

  for (uint32_t j = 1; j < vec_len; ++j) {
    data.second.push_back(serial_data[j]);
  }

  wq->pop(i); // mark the wq entry as clean
  return true;
}

flush_ret_t StandAloneGutters::force_flush() {
  for (auto & buffer : buffers) {
    if (buffer.size() > 1) { // have stuff to flush
      node_id_t i = buffer[0];
      flush(buffer, buffer.size()*sizeof(node_id_t));
      buffer.clear();
      buffer.push_back(i);
    }
  }
}

void StandAloneGutters::set_non_block(bool block) {
  if (block) {
    wq->no_block = true; // circular queue operations should no longer block
    wq->wq_empty.notify_all();
  } else {
    wq->no_block = false; // set circular queue to block if necessary
  }
}
