#include "cache_guttering.h"

#include <iostream>
#include <thread>

inline static node_id_t extract_left_bits(node_id_t number, size_t pos) {
  return number >> pos;
}

static std::pair<node_id_t, node_id_t> buffer_bounds(size_t parent_index, size_t parent_position) {
  return {parent_index * (1 << parent_position), (parent_index + 1) * (1 << parent_position)};
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
      level3_pos(max_level3_bufs >= num_nodes
                     ? 0
                     : std::max(level2_pos / 2, (int)ceil(log2(num_nodes)) - level3_bits)),
      level4_pos(max_level4_bufs >= num_nodes
                     ? 0
                     : std::max(level3_pos / 2, (int)ceil(log2(num_nodes)) - level4_bits)),
      num_level3_bufs(level3_pos == 0 ? 0 : 1 << ((int)ceil(log2(num_nodes)) - level3_pos)),
      num_level4_bufs(level4_pos == 0 ? 0 : 1 << ((int)ceil(log2(num_nodes)) - level4_pos)),
      num_shared_levels((level3_pos >= 0) + (level4_pos >= 0)),
      level3_gutters(num_level3_bufs == 0 ? nullptr : new SharedGutter*[num_level3_bufs]),
      level4_gutters(num_level4_bufs == 0 ? nullptr : new SharedGutter*[num_level4_bufs]),
      leaf_gutters(new LeafGutter*[num_nodes]) {
  // initialize storage for inserter threads
  insert_threads.reserve(inserters);
  for (uint32_t t = 0; t < inserters; t++)
    insert_threads.emplace_back(*this);

  // initialize level3_gutters if necessary
  if (num_level3_bufs > 0) {
    std::cout << " Using level 3 gutters" << std::endl;
    std::cout << "  Number of buffers = " << num_level3_bufs << std::endl;
    std::cout << "  Fanout            = " << (1 << (level3_pos - level4_pos)) << std::endl;

    for (node_id_t i = 0; i < num_level3_bufs; ++i)
      level3_gutters[i] = new SharedGutter(*this, level3_elms_per_buf, 3, i);
  }

  if (num_level4_bufs > 0) {
    std::cout << " Using level 4 gutters" << std::endl;
    std::cout << "  Number of buffers = " << num_level4_bufs << std::endl;
    std::cout << "  Fanout            = " << (1 << level4_pos) << std::endl;

    for (node_id_t i = 0; i < num_level4_bufs; ++i)
      level4_gutters[i] = new SharedGutter(*this, level4_elms_per_buf, 4, i);
  }

  // initialize leaf gutters
  for (node_id_t i = 0; i < num_nodes; ++i) {
    leaf_gutters[i] = new LeafGutter(*this, leaf_gutter_size, i);
  }

  // for debugging -- print out root to leaf paths for every id
  // for (node_id_t i = 0; i < num_nodes; i++)
  //  print_r_to_l(i);
}

CacheGuttering::~CacheGuttering() {
  if (level3_gutters != nullptr) {
    for (node_id_t i = 0; i < num_level3_bufs; i++) {
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
  root_buffer[root_buffer_size++] = upd;

  if (root_buffer_size >= root_buffer_capacity) {
    batch_insert(root_buffer, root_buffer_capacity);
    root_buffer_size = 0;
  }
}

void CacheGuttering::InsertThread::batch_insert(const update_t *batch, size_t num_updates) {
  for (size_t i = 0; i < num_updates; i++) {
    node_id_t src = batch[i].first - CGsystem.relabelling_offset;
    node_id_t dst = batch[i].second;
    node_id_t l1_idx = extract_left_bits(src, CGsystem.level1_pos);
    auto &gutter = level1_gutters[l1_idx];
    gutter.data[gutter.num_elms++] = {src, dst};

    if (gutter.num_elms >= gutter.capacity) {
#ifdef EXIT_LEVEL1
      gutter.num_elms = 0;
#else
      // std::cout << "Flushing L1 gutter" << std::endl;
      flush_l1_buf(l1_idx);
#endif
    }
  }
}

void CacheGuttering::InsertThread::process_stream_upd_batch(const GraphStreamUpdate *upds,
                                                            size_t num_updates) {
  for (size_t i = 0; i < num_updates; i++) {
    // do once for original <src, dst>
    node_id_t src = upds[i].edge.src - CGsystem.relabelling_offset;
    node_id_t dst = upds[i].edge.dst;
    node_id_t l1_idx = extract_left_bits(src, CGsystem.level1_pos);
    auto &gutter = level1_gutters[l1_idx];
    gutter.data[gutter.num_elms++] = {src, dst};

    if (gutter.num_elms >= gutter.capacity) {
#ifdef EXIT_LEVEL1
      gutter.num_elms = 0;
#else
      // std::cout << "Flushing L1 gutter" << std::endl;
      flush_l1_buf(l1_idx);
#endif
    }

    // do again for reversed <dst, src>
    src = upds[i].edge.dst - CGsystem.relabelling_offset;
    dst = upds[i].edge.src;
    l1_idx = extract_left_bits(src, CGsystem.level1_pos);
    auto &gutter2 = level1_gutters[l1_idx];
    gutter2.data[gutter2.num_elms++] = {src, dst};

    if (gutter2.num_elms >= gutter2.capacity) {
#ifdef EXIT_LEVEL1
      gutter2.num_elms = 0;
#else
      // std::cout << "Flushing L1 gutter" << std::endl;
      flush_l1_buf(l1_idx);
#endif
    }
  }
}

void CacheGuttering::InsertThread::flush_l1_buf(node_id_t buf_idx) {
  // std::cerr << "Flushing Level 1 buffer: " << buf_idx << std::endl;
  auto &gutter = level1_gutters[buf_idx];
  if (gutter.num_elms == 0) return;

  // std::cerr << "Non-empty!" << std::endl;
  for (size_t i = 0; i < gutter.num_elms; i++) {
    node_id_t src = gutter.data[i].first;

    size_t l2_idx = extract_left_bits(src, CGsystem.level2_pos);
    auto &l2_gutter = level2_gutters[l2_idx];
    l2_gutter.data[l2_gutter.num_elms++] = gutter.data[i];

    if (l2_gutter.num_elms >= l2_gutter.capacity) {
#ifdef EXIT_LEVEL2
      l2_gutter.num_elms = 0;
#else
      flush_l2_buf(l2_idx); // flush from local L2 to shared level (L3 or leaves)
#endif
    }
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
    node_id_t child_idx = l3_idx & CGsystem.fanout_masks[1];

    if (CGsystem.level3_gutters != nullptr) {
      // inserting to level 3 gutter
      auto &buf = l3_insert_bufs[child_idx];
      buf.push_back(upd);
      if (buf.size() >= block_elms) {
        batch_shared_insert(CGsystem.level3_gutters, l3_idx, buf);
      }
    } else {
      // inserting to leaf gutter
      auto &buf = leaf_insert_bufs[child_idx];
      buf.push_back(upd.second);
      if (buf.size() >= block_leaf_elms) {
        batch_leaf_insert(l3_idx, buf);
      }
    }
  }

  // apply all pending updates to children
  size_t l3_idx = buf_idx << CGsystem.fanout_bits[1]; // first child idx
  if (CGsystem.level3_gutters != nullptr) {
    for (auto &buf : l3_insert_bufs) {
      if (buf.size() > 0) {
        batch_shared_insert(CGsystem.level3_gutters, l3_idx, buf);
      }
      l3_idx++;
      
    }
  } else {
    for (auto &buf : leaf_insert_bufs) {
      if (buf.size() > 0) {
        batch_leaf_insert(l3_idx, buf);
      }
      l3_idx++;
    }
  }

  // all done, mark gutter empty
  l2_gutter.num_elms = 0;
}

void CacheGuttering::InsertThread::batch_shared_insert(SharedGutter **gutters, const size_t buf_idx,
                                                       std::vector<update_t> &updates) {
  SharedGutter *gutter = gutters[buf_idx];
  gutter->active_inserts++; // avoids race cases. Forces threads to pick a safe instruction ordering
  bool done = false;
  while (!done) {
    if (updates.size() > block_elms) {
      std::cerr << "ERROR: UPDATES TOO BIG!" << std::endl;
      exit(EXIT_FAILURE);
    }
    done = gutters[buf_idx]->batch_insert(*this, gutters[buf_idx], updates);
  }
  gutter->active_inserts--;
  updates.clear();
}

void CacheGuttering::InsertThread::batch_leaf_insert(const size_t buf_idx,
                                                      std::vector<node_id_t> &updates) {
  bool done = false;
  LeafGutter *gutter = CGsystem.leaf_gutters[buf_idx];
  gutter->active_inserts++; // avoids race cases. Forces threads to pick a safe instruction ordering
  while (!done) {
    if (updates.size() > block_leaf_elms) {
      std::cerr << "ERROR: UPDATES TOO BIG!" << std::endl;
      exit(EXIT_FAILURE);
    }
    done = CGsystem.leaf_gutters[buf_idx]->batch_insert(*this, CGsystem.leaf_gutters[buf_idx],
                                                        updates);
  }
  gutter->active_inserts--;
  updates.clear();
}

bool CacheGuttering::SharedGutter::batch_insert(InsertThread &thr, SharedGutter *&gut_ptr,
                                                const std::vector<update_t> &updates) {
  // std::cerr << "SharedGutter " << index << " batch_insert(" << updates.size() << ") " << std::endl;
  active_inserts++;

  if (insert_pos >= capacity) {
    active_inserts--;
    return false;
  }

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
  SharedGutter *&extra_buf = *(thr.extra_bufs[level - 3]);
  extra_buf->insert_pos = 0;
  extra_buf->active_inserts = 0;
  extra_buf->index = gut_ptr->index;
  std::swap(gut_ptr, extra_buf);

  // flush our updates if non-leaf
  for (auto upd : updates) {
    node_id_t buf_idx = extract_left_bits(upd.first, CGsystem.positions[level]);
    node_id_t child_idx = buf_idx & CGsystem.fanout_masks[level - 1];

    if (level == 3 && CGsystem.level4_gutters != nullptr) {
      // inserting to level 4 gutter
      auto &buf = thr.l4_insert_bufs[child_idx];
      buf.push_back(upd);
      if (buf.size() >= block_elms) {
        thr.batch_shared_insert(CGsystem.level4_gutters, buf_idx, buf);
      }
    } else {
      // inserting to leaf gutter
      auto &buf = thr.leaf_insert_bufs[child_idx];
      buf.push_back(upd.second);
      if (buf.size() >= block_leaf_elms) {
        thr.batch_leaf_insert(buf_idx, buf);
      }
    }
  }

  // busy wait until all other threads finish (we incremented this twice TODO: OR DID WE???)
  while (active_inserts > 2) {}

  // actually perform the gutter's flush. The gutter is now stored in our extra gutter
  for (size_t i = 0; i < pos; i++) {
    auto upd = data[i];
    node_id_t buf_idx = extract_left_bits(upd.first, CGsystem.positions[level]);
    node_id_t child_idx = buf_idx & CGsystem.fanout_masks[level - 1];

    if (level == 3 && CGsystem.level4_gutters != nullptr) {
      // inserting to level 4 gutter
      auto &buf = thr.l4_insert_bufs[child_idx];
      buf.push_back(upd);
      if (buf.size() >= block_elms) {
        thr.batch_shared_insert(CGsystem.level4_gutters, buf_idx, buf);
      }
    } else {
      // inserting to leaf gutter
      auto &buf = thr.leaf_insert_bufs[child_idx];
      buf.push_back(upd.second);
      if (buf.size() >= block_leaf_elms) {
        thr.batch_leaf_insert(buf_idx, buf);
      }
    }
  }

  // apply all pending updates to children
  if (level == 3 && CGsystem.level4_gutters != nullptr) {
    size_t child_idx = index << CGsystem.fanout_bits[level - 1]; // first child idx
    for (auto &buf : thr.l4_insert_bufs) {
      if (buf.size() > 0) {
        thr.batch_shared_insert(CGsystem.level4_gutters, child_idx, buf);
      }
      child_idx++;
    }
  } else {
    size_t child_idx = index << CGsystem.fanout_bits[level - 1];
    for (auto &buf : thr.leaf_insert_bufs) {
      if (buf.size() > 0) {
        thr.batch_leaf_insert(child_idx, buf);
      }
      child_idx++;
    }
  }
}

bool CacheGuttering::LeafGutter::batch_insert(InsertThread &thr, LeafGutter *&gut_ptr,
                                              const std::vector<node_id_t> &updates) {
  // std::cerr << "LeafGutter " << index << " batch_insert(" << updates.size() << ") " << std::endl;
  active_inserts++;
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
  thr.extra_leaf->insert_pos = updates.size() == 0 ? 0 : updates.size() - (capacity - pos);
  thr.extra_leaf->active_inserts = 1;
  thr.extra_leaf->index = gut_ptr->index;
  std::swap(gut_ptr, thr.extra_leaf);

  // copy data into old leaf
  for (size_t i = 0; i < (capacity - pos) && i < updates.size(); i++) {
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
      size_t we_think_size_is = capacity;
      gut_ptr->insert_pos = we_think_size_is;
      thr.wq_push_helper(gut_ptr->index, *gut_ptr, we_think_size_is);
    } else if (updates.size() > capacity) {
      gut_ptr->insert_pos = i;
    }
  }
  gut_ptr->active_inserts--;

  // busy wait until all other threads finish (we incremented this twice TODO: OR DID WE???)
  while (thr.extra_leaf->active_inserts > 2) {}

  size_t we_think_size_is = std::min(pos + updates.size(), capacity);
  thr.extra_leaf->insert_pos = we_think_size_is;
  thr.wq_push_helper(thr.extra_leaf->index, *thr.extra_leaf, we_think_size_is);
}

void CacheGuttering::InsertThread::wq_push_helper(node_id_t node_idx, LeafGutter &leaf, size_t exp_size) {
  // std::cerr << "Placing LeafGutter " << leaf.index << " (" << leaf.insert_pos << ", " << leaf.capacity << ")" << std::endl;

  if (leaf.insert_pos > leaf.capacity) {
    std::cerr << "ERROR: LeafGutter is too big!" << std::endl;
    std::cerr << "Placing LeafGutter " << leaf.index << " (" << leaf.insert_pos << ", " << leaf.capacity << ")" << std::endl;
    std::cerr << "Expected size = " << exp_size << std::endl;
    exit(EXIT_FAILURE);
  }

  leaf.data.resize(leaf.insert_pos); // no realloc here because insert_pos <= capacity
  local_wq_buffer.batches[local_wq_buffer.size].node_idx = node_idx + CGsystem.relabelling_offset;
  std::swap(local_wq_buffer.batches[local_wq_buffer.size].upd_vec, leaf.data);
  leaf.data.resize(leaf.capacity); // set size of leaf appropriately

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
  // flush root buffer
  batch_insert(root_buffer, root_buffer_size);
  root_buffer_size = 0;

#ifndef EXIT_LEVEL1
  for (size_t i = 0; i < level1_bufs; i++)
    flush_l1_buf(i);
#ifndef EXIT_LEVEL2
  for (size_t i = 0; i < level2_bufs; i++)
    flush_l2_buf(i);
#endif
#endif
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
  
  // flush lower levels
  auto lower_flush_task = [&](const size_t thr, size_t min, size_t max) {
    if (level3_gutters != nullptr) {
      for (size_t i = min; i < max; i++) {
        level3_gutters[i]->flush(insert_threads[thr], level3_gutters[i],
                                 level3_gutters[i]->insert_pos, std::vector<update_t>());
      }
      min <<= fanout_bits[2]; // multiply by L3 fanout
      max <<= fanout_bits[2];
    }
    if (level4_gutters != nullptr) {
      for (size_t i = min; i < max; i++) {
        level4_gutters[i]->flush(insert_threads[thr], level4_gutters[i],
                                 level4_gutters[i]->insert_pos, std::vector<update_t>());
      }
      min <<= fanout_bits[3]; // multiply by L4 fanout
      max <<= fanout_bits[3];
    }
    
    for (size_t i = min; i < max && i < num_nodes; i++) {
      leaf_gutters[i]->flush(insert_threads[thr], leaf_gutters[i], leaf_gutters[i]->insert_pos,
                             std::vector<node_id_t>());
    }
  };

  size_t num_buffers = num_level3_bufs > 0 ? num_level3_bufs : num_nodes;
  size_t min = 0;
  size_t max = (double(1) / inserters) * num_buffers;
  for (size_t i = 0; i < inserters; i++) {
    // std::cerr << "Flush task " << i << ": " << min << ", " << max << std::endl;
    threads[i] = std::thread(lower_flush_task, i, min, max);
    min = max;
    max = (double(i + 2) / inserters) * num_buffers;
  }

  for (size_t i = 0; i < inserters; i++)
    threads[i].join();

  // flush the local work queue buffer for each InsertThread
  for (size_t i = 0; i < inserters; i++)
    insert_threads[i].flush_wq_buf();
}
