#include "cache_gutter_tree.h"

void CacheGutterTree::CacheGutterTreeNode::flush(bool force) {
  if (leafNode) {
    for (const auto &update : buffer) {
      auto &outputBuffer = outputBuffers[update.first - updateSpan.first];
      outputBuffer.push_back(update.second);
      if (outputBuffer.size() == config.bufferSize) {
        int data_size = config.bufferSize * sizeof(node_id_t);
        config.wq.push(reinterpret_cast<char *>(outputBuffer.data()), data_size);
        outputBuffer.clear();
        outputBuffer.push_back(updateSpan.first + update.first);
      }
    }
    if (force) {
      const size_t numOutputBuffers = updateSpan.second - updateSpan.first + 1;
      for (size_t i = 0; i < numOutputBuffers; ++i) {
        std::vector<node_id_t> buffer = outputBuffers[i];
        if (buffer.size() > 0) {
          int data_size = buffer.size() * sizeof(node_id_t);
          config.wq.push(reinterpret_cast<char *>(buffer.data()), data_size);
          buffer.clear();
          buffer.push_back(updateSpan.first + i);
        }
      }      
    }
  } else {
    const size_t spanQuanta = calculateSpanQuanta(updateSpan);
    for (const auto &update : buffer) {
      const size_t bucket = std::min((update.first-updateSpan.first) / spanQuanta, numChildren - 1);
      childNodes[bucket].insert(update);
    }
  }
  buffer.clear();
}
