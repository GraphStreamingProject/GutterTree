#pragma once
#include <cassert>
#include <cmath>
#include <variant>

#include "work_queue.h"
#include "types.h"
#include "guttering_system.h"

constexpr size_t M = 32768; // The size in bytes of the L1 cache
constexpr size_t B = 64;    // The size of a cache line
static_assert(M % B == 0);

/*
 * CacheGutterTree is a version of the StandaloneGutters guttering system
 * It is designed to be highly efficient when the guttering system is entirely in memory
 * and when the number of nodes in the graph very high by maintaining a gutter tree like
 * structure but for cache rather than disk.
 */
class CacheGutterTree : public GutteringSystem {
 private:
  static size_t getNumLevels(size_t numLeaves) {
    const double fractionalNumLevels = std::log(numLeaves) / std::log(M / B);
    const bool dropTopLevel = (fractionalNumLevels - std::floor(fractionalNumLevels)) < epsilon;
    const size_t numLevels = dropTopLevel? static_cast<size_t>(fractionalNumLevels) - 1 : 
                                           static_cast<size_t>(fractionalNumLevels);
    return numLevels;
  }

  struct CacheGutterTreeConfig {
    const size_t numLevels;
    const size_t bufferSize;
    WorkQueue &wq;
  };

  class CacheGutterTreeNode {
   private:
    static constexpr size_t maxOccupancy = M / sizeof(update_t);
    static constexpr size_t numChildren = M / B;

    const CacheGutterTreeConfig &config;
    const std::pair<size_t, size_t> updateSpan;
    std::vector<update_t> buffer;
    const bool leafNode;

    // I'd like to use variant here, but I can't because we use too old a
    // version of C++, and union isn't worth the trouble.
    std::vector<CacheGutterTreeNode> childNodes;
    std::vector<std::vector<node_id_t>> outputBuffers;

   public:
    static constexpr size_t calculateSpanQuanta(std::pair<size_t, size_t> updateSpan) {
      return (updateSpan.second - updateSpan.first) / (M / B);
    }

    std::pair<size_t, size_t> calculateSubspan (std::pair<size_t, size_t> updateSpan, size_t i) {
      const size_t spanQuanta = calculateSpanQuanta(updateSpan);
      const size_t spanBegin = updateSpan.first + i * spanQuanta;
      const size_t spanEnd = i + 1 == numChildren ? updateSpan.second : spanBegin + spanQuanta - 1;
      return std::pair<size_t, size_t>(spanBegin, spanEnd);
    }

    CacheGutterTreeNode(std::pair<size_t, size_t> updateSpan, size_t level,
                        const CacheGutterTreeConfig &config) : config(config),
                        updateSpan(updateSpan), buffer(), leafNode(level == config.numLevels) {
      buffer.reserve(maxOccupancy);
      if (leafNode) {
        const size_t numOutputBuffers = updateSpan.second - updateSpan.first + 1;
        for (size_t i = 0; i < numOutputBuffers; ++i) {
          outputBuffers.emplace_back();
          outputBuffers.back().reserve(config.bufferSize);
          outputBuffers.back().push_back(updateSpan.first + i);
        }
      } else {
        for (size_t i = 0; i < numChildren; ++i) {
          childNodes.emplace_back(calculateSubspan(updateSpan, i), level + 1, config);
        }
      }
    }

    CacheGutterTreeNode(const CacheGutterTreeNode &) = default;

    void insert(update_t upd) {
      buffer.push_back(upd);
      if (buffer.size() == maxOccupancy) {
        flush();
      }
    }

    void flush(bool force = false);
    
    // flush this node and all its children of any updates
    void flush_tree() {
      flush(true);
      for (auto child : childNodes) {
        child.flush_tree();
      }
    }
  };
  uint32_t buffer_size; // number of updates per leaf

  static constexpr double epsilon = 0.1;
  CacheGutterTreeConfig config;
  CacheGutterTreeNode root;

 public:
  /*
   * Constructs a new CacheGutterTree
   * @param nodes     the number of nodes in the graph
   * @param workers   the number of workers that will be removing batches
   * Note: using comma operator in a hacky way to configure the system before initializing
   */
  CacheGutterTree(node_id_t nodes, int workers) :
    GutteringSystem(nodes, workers),
    config{getNumLevels(nodes), buffer_size, wq},
    root{std::pair<size_t, size_t>(0, nodes - 1), 0, config} {}

  /**
   * Puts an update into the data structure.
   * @param upd the edge update.
   * @return nothing.
   */
  insert_ret_t insert(const update_t &upd) { root.insert(upd); }

  /**
   * Flushes all pending buffers.
   * @return nothing.
   */
  flush_ret_t force_flush() { root.flush_tree(); }
};
