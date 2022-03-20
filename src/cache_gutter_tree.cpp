#include "cache_gutter_tree.h"

void CacheGutterTree::CacheGutterTreeNode::flush(bool force) {
  if (leafNode) {
    for (const auto &update : buffer) {
      auto &outputBuffer = outputBuffers[update.first - updateSpan.first];
      outputBuffer.push_back(update.second);
      if (outputBuffer.size() == config.bufferSize) {
        config.wq.push(update.first - updateSpan.first, buffer);
        outputBuffer.clear();
      }
    }
    if (force) {
      const size_t numOutputBuffers = updateSpan.second - updateSpan.first + 1;
      for (size_t i = 0; i < numOutputBuffers; ++i) {
        std::vector<node_id_t> &buffer = outputBuffers[i];
        if (buffer.size() > 0) {
          config.wq.push(i, buffer);
          buffer.clear();
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
