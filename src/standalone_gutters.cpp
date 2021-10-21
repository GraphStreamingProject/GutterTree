#include <cassert>
#include <fstream>
#include <math.h>
#include "../include/standalone_gutters.h"

uint32_t StandAloneGutters::queue_factor;
uint32_t StandAloneGutters::gutter_factor;
const unsigned first_idx = 2;

StandAloneGutters::StandAloneGutters(Node num_nodes, int workers) :
buffers(num_nodes) {
  configure(); // read buffering configuration file

  uint32_t bytes_size = floor(24 * pow(log2(num_nodes), 3)); // size of leaf proportional to size of sketch
  bytes_size   = (bytes_size % serial_update_size == 0)? bytes_size : bytes_size + serial_update_size - bytes_size % serial_update_size;
  buffer_size = bytes_size / sizeof(node_id_t);

  wq = new WorkQueue(workers * queue_factor, bytes_size);

  for (Node i = 0; i < num_nodes; ++i) {
    buffers[i] = static_cast<node_id_t *>(malloc(bytes_size));
    buffers[i][0] = first_idx; // first spot will point to the next free space
    buffers[i][1] = i; // second spot identifies the node to which the buffer
  }
}

StandAloneGutters::~StandAloneGutters() {
  for (auto & buffer : buffers) {
    free(buffer);
  }
  delete wq;
}

// Read the configuration file to determine a variety of Buffering params
void StandAloneGutters::configure() {
  int queue_f  = 2;
  int gutter_f = 1;

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
        gutter_factor = std::stoi(line.substr(line.find('=') + 1));
        if (gutter_factor > 10 || gutter_factor < 1) {
          printf("WARNING: gutter_factor out of bounds [1,50] using default(1)\n");
          gutter_factor = 1;
        }
      }
    }
  } else {
    printf("WARNING: Could not open buffering configuration file! Using default setttings.\n");
  }
  queue_factor  = queue_f;
  gutter_factor = gutter_f; // works on POSIX systems (alternative is boost)
}

void StandAloneGutters::flush(node_id_t *buffer, uint32_t num_bytes) {
  wq->push(reinterpret_cast<char *>(buffer), num_bytes);
}

insert_ret_t StandAloneGutters::insert(update_t upd) {
  node_id_t& idx = buffers[upd.first][0];
  buffers[upd.first][idx] = (node_id_t) upd.second;
  ++idx;
  if (idx == buffer_size) { // full, so request flush
    flush(buffers[upd.first], buffer_size*sizeof(node_id_t));
    idx = first_idx;
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
  Node key = serial_data[1];
  data.first = key;

  data.second.clear(); // remove any old data from the vector
  uint32_t vec_len  = len / sizeof(node_id_t);
  data.second.reserve(vec_len); // reserve space for our updates

  for (uint32_t j = first_idx; j < vec_len; ++j) {
    data.second.push_back(serial_data[j]);
  }

  wq->pop(i); // mark the wq entry as clean
  return true;
}

flush_ret_t StandAloneGutters::force_flush() {
  for (auto & buffer : buffers) {
    if (buffer[0] != first_idx) { // have stuff to flush
      flush(buffer, buffer[0]*sizeof(node_id_t));
      buffer[0] = first_idx;
    }
  }
}

void StandAloneGutters::set_non_block(bool block) {
  if (block) {
    wq->no_block = true; // circular queue operations should no longer block
    wq->cirq_empty.notify_all();
  } else {
    wq->no_block = false; // set circular queue to block if necessary
  }
}
