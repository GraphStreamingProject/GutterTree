#include "cache_guttering.h"

#include <iostream>
#include <thread>

inline static node_id_t extract_left_bits(node_id_t number, int pos) {
  number >>= pos;
  return number;
}

void CacheGuttering::print_r_to_l(node_id_t src) {
  std::cout << "src: " << src;
  std::cout << "->(L1)" << extract_left_bits(src, level1_pos);
  std::cout << "->(L2)" << extract_left_bits(src, level2_pos);
  if (level3_gutters)
    std::cout << "->(L3)" << extract_left_bits(src, level3_pos);
  if (level4_gutters)
    std::cout << "->(L4)" << extract_left_bits(src, level4_pos);
  std::cout << std::endl;
}

CacheGuttering::CacheGuttering(node_id_t num_nodes, uint32_t workers, uint32_t inserters,
                               GutteringConfiguration conf)
    : GutteringSystem(num_nodes, workers, conf),
      inserters(inserters),
      num_nodes(num_nodes),
      level1_pos(std::max((int)ceil(log2(num_nodes)) - level1_bits, 0)),
      level2_pos(std::max((int)ceil(log2(num_nodes)) - level2_bits, 0)),
      level3_pos(max_level3_bufs > num_nodes
                     ? 0
                     : std::max(level2_pos / 2, (int)ceil(log2(num_nodes)) - level3_bits)),
      level4_pos(std::max((int)ceil(log2(num_nodes)) - level4_bits, 0)),
      num_level3_bufs(level3_pos == 0 ? 0 : 1 << ((int) ceil(log2(num_nodes)) - level3_pos)),
      num_level4_bufs(level4_pos == 0 ? 0 : 1 << ((int) ceil(log2(num_nodes)) - level4_pos)),
      num_shared_levels((level3_pos >= 0) + (level4_pos >= 0)) {
  // initialize storage for inserter threads
  insert_threads.reserve(inserters);
  for (uint32_t t = 0; t < inserters; t++)
    insert_threads.emplace_back(*this);

  // initialize level3_gutters if necessary
  if (num_level3_bufs > 0) {
    std::cout << " Using level 3 gutters" << std::endl;
    std::cout << "  number of buffers = " << num_level3_bufs << std::endl;

    level3_gutters = new SharedGutter*[num_level3_bufs];
    for (node_id_t i = 0; i < num_level3_bufs; ++i)
      level3_gutters[i] = new SharedGutter(*this, level3_elms_per_buf, 3, i);
  }

  if (num_level4_bufs > 0) {
    std::cout << " Using level 4 gutters" << std::endl;
    std::cout << "  number of buffers = " << num_level4_bufs << std::endl;

    for (node_id_t i = 0; i < num_level4_bufs; ++i)
      level4_gutters[i] = new SharedGutter(*this, level4_elms_per_buf, 4, i);
  }

  // initialize leaf gutters
  leaf_gutters = new LeafGutter*[num_nodes];
  for (node_id_t i = 0; i < num_nodes; ++i) {
    leaf_gutters[i] =
        new LeafGutter(*this, leaf_gutter_size, i);
  }

  // for debugging -- print out root to leaf paths for every id
  // for (node_id_t i = 0; i < num_nodes; i++)
  //  print_r_to_l(i);
}

CacheGuttering::~CacheGuttering() {
  if (level3_gutters != nullptr) {
    for (node_id_t i = 0; i < num_level4_bufs; i++) {
      delete level3_gutters[i];
    }
    delete[] level3_gutters;
  }

  if (level4_gutters != nullptr) {
    for (node_id_t i = 0; i < num_level4_bufs; i++) {
      delete level4_gutters[i];
    }
    delete[] level4_gutters;
  }

  for (node_id_t i = 0; i < num_nodes; i++) {
    delete leaf_gutters[i];
  }
  delete[] leaf_gutters;
}

void CacheGuttering::InsertThread::insert(update_t upd) {
  upd.first -= CGsystem.relabelling_offset;
  node_id_t l1_idx = extract_left_bits(upd.first, CGsystem.level1_pos);
  auto &gutter = level1_gutters[l1_idx];
  gutter.data[gutter.num_elms++] = upd;

  // std::cerr << "Handling update " << upd.first << ", " << upd.second << std::endl;
  // std::cerr << "Placing in L1 buffer " << l1_idx << ", num_elms = " << gutter.num_elms << std::endl;

  if (gutter.num_elms >= gutter.capacity) {
    // std::cout << "Flushing L1 gutter" << std::endl;
    flush_l1_buf(l1_idx);
  }
}

void CacheGuttering::InsertThread::flush_l1_buf(node_id_t buf_idx) {
  // std::cerr << "Flushing Level 1 buffer: " << buf_idx << std::endl;
  auto &gutter = level1_gutters[buf_idx];
  if (gutter.num_elms == 0) return;

  // std::cerr << "Non-empty!" << std::endl;
  for (size_t i = 0; i < gutter.num_elms; i++) {
    update_t upd = gutter.data[i];
    node_id_t l2_idx = extract_left_bits(upd.first, CGsystem.level2_pos);

    auto &l2_gutter = level2_gutters[l2_idx];
    l2_gutter.data[l2_gutter.num_elms++] = upd;
    if (l2_gutter.num_elms >= l2_gutter.capacity)
      flush_l2_buf(l2_idx); // flush from local L2 to shared level (L3 or leaves)
  }
  gutter.num_elms = 0;
}

void CacheGuttering::InsertThread::flush_l2_buf(node_id_t buf_idx) {
  // std::cerr << "Flushing Level 2 buffer: " << buf_idx << std::endl;

  auto &l2_gutter = level2_gutters[buf_idx];
  if (l2_gutter.num_elms == 0) return;

  // std::cerr << "Non-empty: " << l2_gutter.num_elms << std::endl;
  for (size_t i = 0; i < l2_gutter.num_elms; i++) {
    update_t upd = l2_gutter.data[i];
    node_id_t l3_idx = extract_left_bits(upd.first, CGsystem.level3_pos);
    node_id_t child_idx = l3_idx % CGsystem.fanouts[1];

    if (CGsystem.level3_gutters != nullptr) {
      // inserting to level 3 gutter
      auto &buf = l3_insert_bufs[child_idx];
      buf.push_back(upd);
      if (buf.size() >= block_elms) {
        while (!CGsystem.level3_gutters[l3_idx]->batch_insert(
            *this, CGsystem.level3_gutters[l3_idx], buf)) {
        }
        buf.clear();
      }
    } else {
      // inserting to leaf gutter
      auto &buf = leaf_insert_bufs[child_idx];
      buf.push_back(upd.second);
      if (buf.size() >= block_leaf_elms) {
        while (!CGsystem.leaf_gutters[l3_idx]->batch_insert(*this, CGsystem.leaf_gutters[l3_idx],
                                                            buf)) {
        }
        buf.clear();
      }
    }
  }

  // apply all pending updates to children
  size_t l3_idx = buf_idx * CGsystem.fanouts[1]; // first child idx
  if (CGsystem.level3_gutters != nullptr) {
    for (auto &buf : l3_insert_bufs) {
      if (buf.size() > 0) {
        while (!CGsystem.level3_gutters[l3_idx]->batch_insert(
            *this, CGsystem.level3_gutters[l3_idx], buf)) {
        }
      }
      l3_idx++;
      buf.clear();
    }
  } else {
    for (auto &buf : leaf_insert_bufs) {
      if (buf.size() > 0) {
        while (!CGsystem.leaf_gutters[l3_idx]->batch_insert(*this, CGsystem.leaf_gutters[l3_idx],
                                                            buf)) {
        }
      }
      l3_idx++;
      buf.clear();
    }
  }

  // all done, mark gutter empty
  l2_gutter.num_elms = 0;
}

bool CacheGuttering::SharedGutter::batch_insert(InsertThread &thr, SharedGutter *&gut_ptr,
                                          const std::vector<update_t> &updates) {
  // std::cerr << "SharedGutter " << index << " batch_insert(" << updates.size() << ") " << std::endl;

  size_t pos = insert_pos.fetch_add(updates.size());
  
  if (pos >= capacity) {
    active_inserts--;
    return false;
  }
  
  if (pos < capacity && pos + updates.size() >= capacity) {
    // this thread is responsible for performing the flush
    flush(thr, gut_ptr, pos, updates);
  } else {
    // normal data copy, just do it
    for (auto upd : updates) {
      data[pos++] = upd;
    }
  }
  active_inserts--;
  return true;
}

void CacheGuttering::SharedGutter::flush(InsertThread &thr, SharedGutter *&gut_ptr, size_t pos,
                                             const std::vector<update_t> &updates) {
  // std::cerr << "SharedGutter " << index << " flush()" << std::endl;

  // swap our extra gutter with the gutter we are flushing
  // after this step completes, other threads can start populating our extra buffer
  SharedGutter *&extra_buf = *(thr.extra_bufs[level - 2]);
  extra_buf->insert_pos = 0;
  extra_buf->active_inserts = 0;
  std::swap(gut_ptr, extra_buf);

  // flush our updates if non-leaf
  for (auto upd : updates) {
    node_id_t buf_idx = extract_left_bits(upd.first, CGsystem.positions[level]);
    node_id_t child_idx = buf_idx % CGsystem.fanouts[level - 1];

    if (level == 3 && CGsystem.level4_gutters != nullptr) {
      // inserting to level 4 gutter
      auto &buf = thr.l4_insert_bufs[child_idx];
      buf.push_back(upd);
      if (buf.size() >= block_elms) {
        while (!CGsystem.level4_gutters[buf_idx]->batch_insert(
            thr, CGsystem.level4_gutters[buf_idx], buf)) {}
        buf.clear();
      }
    } else {
      // inserting to leaf gutter
      auto &buf = thr.leaf_insert_bufs[child_idx];
      buf.push_back(upd.second);
      if (buf.size() >= block_leaf_elms) {
        while (!CGsystem.leaf_gutters[buf_idx]->batch_insert(
            thr, CGsystem.leaf_gutters[buf_idx], buf)) {}
        buf.clear();
      }
    }
  }

  // busy wait until all other threads finish
  while (active_inserts > 1) {}

  // actually perform the gutter's flush. The gutter is now stored in our extra gutter
  for (size_t i = 0; i < pos; i++) {
    auto upd = data[i];
    node_id_t buf_idx = extract_left_bits(upd.first, CGsystem.positions[level]);
    node_id_t child_idx = buf_idx % CGsystem.fanouts[level - 1];

    if (level == 3 && CGsystem.level4_gutters != nullptr) {
      // inserting to level 4 gutter
      auto &buf = thr.l4_insert_bufs[child_idx];
      buf.push_back(upd);
      if (buf.size() >= block_elms) {
        while (!CGsystem.level4_gutters[buf_idx]->batch_insert(
              thr, CGsystem.level4_gutters[buf_idx], buf)) {}
        buf.clear();
      }
    } else {
      // inserting to leaf gutter
      auto &buf = thr.leaf_insert_bufs[child_idx];
      buf.push_back(upd.second);
      if (buf.size() >= block_leaf_elms) {
        while (!CGsystem.leaf_gutters[buf_idx]->batch_insert(
              thr, CGsystem.leaf_gutters[buf_idx], buf)) {}
        buf.clear();
      }
    }
  }

  // apply all pending updates to children
  size_t child_idx = index * CGsystem.fanouts[level - 1]; // first child idx
  for (auto &buf : thr.l4_insert_bufs) {
    if (buf.size() > 0) {
      while (!CGsystem.level4_gutters[child_idx]->batch_insert(
            thr, CGsystem.level4_gutters[child_idx], buf)) {}
      buf.clear();
    }
    child_idx++;
  }
}

bool CacheGuttering::LeafGutter::batch_insert(InsertThread &thr, LeafGutter *&gut_ptr,
                                              const std::vector<node_id_t> &updates) {
  // std::cerr << "LeafGutter " << index << " batch_insert(" << updates.size() << ") " << std::endl;
  size_t pos = insert_pos.fetch_add(updates.size());
  
  if (pos >= capacity) {
    active_inserts--;
    return false;
  }
  
  if (pos < capacity && pos + updates.size() >= capacity) {
    // this thread is responsible for performing the flush
    flush(thr, gut_ptr, pos, updates);
  } else {
    // normal data copy, just do it
    for (auto upd : updates) {
      data[pos++] = upd;
    }
  }
  active_inserts--;
  return true;
}

void CacheGuttering::LeafGutter::flush(InsertThread &thr, LeafGutter *&gut_ptr, size_t pos,
                                const std::vector<node_id_t> &updates) {
  // std::cerr << "LeafGutter " << index << " flush()" << std::endl;
  // swap our extra gutter with the gutter we are flushing
  // after this step completes, other threads can start populating our extra buffer
  thr.extra_leaf->insert_pos = updates.size() - (capacity - pos);
  thr.extra_leaf->active_inserts = 1;
  thr.extra_leaf->index = gut_ptr->index;
  std::swap(gut_ptr, thr.extra_leaf);

  // copy data into old leaf
  for (size_t i = 0; i < (capacity - pos); i++) {
    thr.extra_leaf->data[pos + i] = updates[i];
  }

  // copy data into new leaf
  size_t upd_pos = capacity - pos;
  while (upd_pos < updates.size()) {
    size_t i;
    for (i = 0; i < capacity && upd_pos < updates.size(); i++) {
      gut_ptr->data[i] = updates[upd_pos];
      upd_pos++;
    }

    if (i >= capacity) {
      gut_ptr->insert_pos = capacity;
      thr.wq_push_helper(gut_ptr->index, *gut_ptr);
    } else if (updates.size() > capacity) {
      gut_ptr->insert_pos = i;
    }
  }
  gut_ptr->active_inserts--;

  // busy wait until all other threads finish
  while (thr.extra_leaf->active_inserts > 1) {}

  thr.extra_leaf->insert_pos = std::min(pos + updates.size(), capacity);
  thr.wq_push_helper(thr.extra_leaf->index, *thr.extra_leaf);
}

void CacheGuttering::InsertThread::wq_push_helper(node_id_t node_idx, LeafGutter &leaf) {
  // std::cerr << "Placing LeafGutter " << leaf.index << " (" << leaf.insert_pos << ", " << leaf.capacity << ")" << std::endl;

  local_wq_buffer.batches[local_wq_buffer.size].node_idx = node_idx + CGsystem.relabelling_offset;
  local_wq_buffer.batches[local_wq_buffer.size].upd_vec.assign(leaf.data,
                                                               leaf.data + leaf.insert_pos);
  ++local_wq_buffer.size;
  if (local_wq_buffer.size >= CGsystem.wq_batch_per_elm)
    flush_wq_buf();
  leaf.insert_pos = 0;
}

void CacheGuttering::InsertThread::flush_wq_buf() {
  // if nothing to flush then don't
  if (local_wq_buffer.size == 0) return;

  // if wq buffer size is less than expected
  if (local_wq_buffer.size < CGsystem.wq_batch_per_elm) {
    // clear the batches beyond wq buffer size
    for (size_t i = local_wq_buffer.size; i < CGsystem.wq_batch_per_elm; i++)
      local_wq_buffer.batches[i].upd_vec.clear();
  }

  // perform the flush
  CGsystem.wq.push(local_wq_buffer.batches);
  local_wq_buffer.size = 0;
}

void CacheGuttering::InsertThread::flush_all() {
  for (size_t i = 0; i < level1_bufs; i++)
    flush_l1_buf(i);
  for (size_t i = 0; i < level2_bufs; i++)
    flush_l2_buf(i);
}

void CacheGuttering::force_flush() {
  // task for flushing thread local buffers
  auto flush_task = [&](const size_t idx) {
    auto &thr = insert_threads[idx];
    thr.flush_all();
  };
  
  // flush thread local buffers in parallel
  std::vector<std::thread> threads;
  threads.reserve(inserters);
  for (size_t i = 0; i < inserters; i++)
    threads.emplace_back(flush_task, i);
  
  for (size_t i = 0; i < inserters; i++)
    threads[i].join();
  
  // flush level3 gutters if necessary
  if (level3_gutters != nullptr) {
    auto lower_flush_task = [&](const size_t thr, const size_t min, const size_t max) {
      for (size_t i = 0; i < num_level3_bufs; i++) {
        level3_gutters[i]->flush(insert_threads[thr], level3_gutters[i],
                                 level3_gutters[i]->insert_pos, std::vector<update_t>());
      }
      if (level4_gutters != nullptr) {
        size_t l4_min = min * (num_level4_bufs / num_level3_bufs);
        size_t l4_max = max * (num_level4_bufs / num_level3_bufs);
        for (size_t i = l4_min; i < l4_max; i++) {
          level4_gutters[i]->flush(insert_threads[thr], level4_gutters[i],
                                   level4_gutters[i]->insert_pos, std::vector<update_t>());
        }
      }
    };

    size_t min = 0;
    size_t max = num_level3_bufs / inserters;
    for (size_t i = 0; i < inserters; i++) {
      threads[i] = std::thread(lower_flush_task, i, min, max);
      min = max;
      max = (num_level3_bufs - max) / (i + 1);
    }

    for (size_t i = 0; i < inserters; i++)
      threads[i].join();
  }

  for (node_id_t i = 0; i < num_nodes; i++) {
    if (leaf_gutters[i]->insert_pos > 0) {
      // std::cerr << "flushing leaf gutter " << i << " with " << leaf_gutters[i]->insert_pos << " updates" << std::endl;
      assert(leaf_gutters[i]->insert_pos <= leaf_gutter_size);
      insert_threads[0].wq_push_helper(i, *leaf_gutters[i]);
      leaf_gutters[i]->insert_pos = 0;
    } else {
      // std::cerr << "LeafGutter " << i << " empty" << std::endl;
    }
  }

  // flush the local work queue buffer for each InsertThread
  for (size_t i = 0; i < inserters; i++)
    insert_threads[i].flush_wq_buf();
}
