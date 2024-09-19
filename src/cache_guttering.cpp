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

    extra_level3_gutters = new SharedGutter*[local_fanout * inserters];
    for (node_id_t i = 0; i < local_fanout * inserters; ++i) {
      extra_level3_gutters[i] = new SharedGutter(*this, level3_elms_per_buf, 3, 0);
    }
  }

  if (num_level4_bufs > 0) {
    std::cout << " Using level 4 gutters" << std::endl;
    std::cout << "  Number of buffers = " << num_level4_bufs << std::endl;
    std::cout << "  Fanout            = " << (1 << level4_pos) << std::endl;

    for (node_id_t i = 0; i < num_level4_bufs; ++i)
      level4_gutters[i] = new SharedGutter(*this, level4_elms_per_buf, 4, i);

    extra_level4_gutters = new SharedGutter*[global_fanout * inserters];
    for (node_id_t i = 0; i < global_fanout * inserters; ++i) {
      extra_level4_gutters[i] = new SharedGutter(*this, level4_elms_per_buf, 4, 0);
    }
  }

  // initialize leaf gutters
  for (node_id_t i = 0; i < num_nodes; ++i) {
    leaf_gutters[i] = new LeafGutter(*this, leaf_gutter_size, i);
  }

  extra_leaves = new LeafGutter*[global_fanout * inserters];
  for (node_id_t i = 0; i < global_fanout * inserters; i++) {
    extra_leaves[i] = new LeafGutter(*this, leaf_gutter_size, 0);
  }

  l3_extra_buf_idx = 0;
  l4_extra_buf_idx = 0;
  leaf_extra_buf_idx = 0;

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

    for (node_id_t i = 0; i < local_fanout * inserters; ++i)
      delete extra_level3_gutters[i];
    delete[] extra_level3_gutters;
  }

  if (level4_gutters != nullptr) {
    for (node_id_t i = 0; i < num_level4_bufs; i++) {
      delete level4_gutters[i];
    }
    delete[] level4_gutters;

    for (node_id_t i = 0; i < global_fanout * inserters; ++i)
      delete extra_level4_gutters[i];
    delete[] extra_level4_gutters;
  }

  for (node_id_t i = 0; i < num_nodes; i++) {
    delete leaf_gutters[i];
  }
  delete[] leaf_gutters;

  for (node_id_t i = 0; i < global_fanout * inserters; i++) {
    delete extra_leaves[i];
  }
  delete[] extra_leaves;
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
    gutter.srcs[gutter.num_elms] = src;
    gutter.dsts[gutter.num_elms] = dst;
    ++gutter.num_elms;

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

void CacheGuttering::InsertThread::flush_l1_buf(node_id_t buf_idx) {
  // std::cerr << "Flushing Level 1 buffer: " << buf_idx << std::endl;
  auto &gutter = level1_gutters[buf_idx];
  if (gutter.num_elms == 0) return;

  // std::cerr << "Non-empty!" << std::endl;
  for (size_t i = 0; i < gutter.num_elms; i++) {
    node_id_t src = gutter.srcs[i];
    node_id_t dst = gutter.dsts[i];

    size_t l2_idx = extract_left_bits(src, CGsystem.level2_pos);
    auto &l2_gutter = level2_gutters[l2_idx];
    l2_gutter.srcs[l2_gutter.num_elms] = src;
    l2_gutter.dsts[l2_gutter.num_elms] = dst;
    ++l2_gutter.num_elms;

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

  // Copy the updates into our children
  if (CGsystem.level3_gutters != nullptr) {
    // std::cerr << "Calling: place_upds_in_gutters" << std::endl;
    place_upds_in_gutters(l2_gutter.srcs.data(), l2_gutter.dsts.data(), buf_idx,
                          l2_gutter.num_elms, 3);
  } else {
    // std::cerr << "Calling: place_upds_in_leaves" << std::endl;
    place_upds_in_leaves(l2_gutter.srcs.data(), l2_gutter.dsts.data(), buf_idx,
                         l2_gutter.num_elms, 3);
  }

  // all done, mark gutter empty
  l2_gutter.num_elms = 0;
}

void CacheGuttering::SharedGutter::flush_if_can(InsertThread &thr) {
  if (active_inserts == 0 && insert_pos >= capacity) {
    flush(thr);
  }
}

void CacheGuttering::LeafGutter::flush_if_can(InsertThread &thr) {
  if (active_inserts == 0 && insert_pos >= capacity) {
    flush(thr);
  }
}

void CacheGuttering::InsertThread::place_upds_in_gutters(const node_id_t *srcs,
                                                         const node_id_t *dsts,
                                                         size_t parent_index, size_t num_updates,
                                                         size_t update_level) {
  // std::cerr << "Placing updates in gutters" << std::endl;
  // std::cerr << "First child: " << first_child << std::endl;
  // std::cerr << "Upds: " << num_updates << std::endl;

  // metadata for the level we are flushing to
  FanoutMetaData meta = CGsystem.get_level_update_metadata(update_level);

  size_t fanout = 1 << meta.fanout_bits;
  size_t child_bits = meta.bit_position;
  node_id_t child_mask = meta.fanout_mask;
  size_t first_child = parent_index * fanout;
  auto bounds = buffer_bounds(parent_index, child_bits + meta.fanout_bits);

  SharedGutter **gutters;
  if (update_level == 3) {
    gutters = CGsystem.level3_gutters;
  } else {
    gutters = CGsystem.level4_gutters;
  }

  // compute the number of updates bound to each of the gutters
  node_id_t child_hist[fanout];
  for (size_t i = 0; i < fanout; i++) {
    child_hist[i] = 0;
  }
  for (size_t i = 0; i < num_updates; i++) {
    node_id_t child_idx = extract_left_bits(srcs[i], child_bits) & child_mask;
    assert(child_idx < fanout);
    ++child_hist[child_idx];
  }

  // Reserve the positions in the children
  SharedWritePos positions[fanout];
  for (size_t i = 0; i < fanout; i++) {
    if (child_hist[i] > 0) {
      positions[i] = CGsystem.shared_reserve(gutters, first_child + i, child_hist[i]);

      // number of updates that don't go into first gutter
      child_hist[i] -= (positions[i].last_pos - positions[i].cur_idx);
    }
  }

  // Write updates to children
  for (size_t i = 0; i < num_updates; i++) {
    node_id_t src = srcs[i];
    node_id_t dst = dsts[i];
    node_id_t child_idx = extract_left_bits(src, child_bits) & child_mask;

    assert(src >= bounds.first && src < bounds.second);

    auto &write_pos = positions[child_idx];
    assert(write_pos.cur_idx < write_pos.last_pos);
    write_pos.gutter->srcs[write_pos.cur_idx] = src;
    write_pos.gutter->dsts[write_pos.cur_idx] = dst;
    ++write_pos.cur_idx;

    if (write_pos.cur_idx == write_pos.last_pos && child_hist[child_idx] > 0) {
      write_pos.gutter->active_inserts--;
      write_pos.gutter->flush_if_can(*this);

      write_pos = CGsystem.shared_reserve(gutters, first_child + child_idx, child_hist[child_idx]);

      // number of updates that don't go into next gutter
      assert(child_hist[child_idx] >= write_pos.last_pos - write_pos.cur_idx);
      child_hist[child_idx] -= (write_pos.last_pos - write_pos.cur_idx);
    }
  }

  // Decrement active_inserts for our children and flush if necessary
  for (size_t i = 0; i < fanout; i++) {
    if (positions[i].gutter != nullptr) {
      positions[i].gutter->active_inserts--;
      positions[i].gutter->flush_if_can(*this);
    }
  }
}

void CacheGuttering::InsertThread::place_upds_in_leaves(const node_id_t *srcs,
                                                        const node_id_t *dsts,
                                                        size_t parent_index, size_t num_updates,
                                                        size_t update_level) {
  // std::cerr << "Placing updates in leaves" << std::endl;
  FanoutMetaData meta = CGsystem.get_level_update_metadata(update_level);

  size_t fanout = 1 << meta.fanout_bits;
  size_t child_bits = meta.bit_position;
  node_id_t child_mask = meta.fanout_mask;
  size_t first_child = parent_index * fanout;
  auto bounds = buffer_bounds(parent_index, child_bits + meta.fanout_bits);


  // std::cerr << "First leaf: " << first_child << std::endl;
  // std::cerr << "Upds: " << num_updates << std::endl;
  // std::cerr << "Child bits: " << child_bits << std::endl;
  // std::cerr << "Child mask: " << child_mask << std::endl;

  // compute the number of updates bound to each of the leaves
  node_id_t child_hist[fanout];
  {
    node_id_t child_hist2[fanout]; // temp histogram for fast computation
    for (size_t i = 0; i < fanout; i++) {
      child_hist[i] = 0;
      child_hist2[i] = 0;
    }

    for (size_t i = 0; i < num_updates - 1; i += 2) {
      node_id_t child_idx1 = extract_left_bits(srcs[i], child_bits) & child_mask;
      node_id_t child_idx2 = extract_left_bits(srcs[i + 1], child_bits) & child_mask;
      assert(child_idx1 < fanout && child_idx2 < fanout);
      ++child_hist[child_idx1];
      ++child_hist2[child_idx2];
    }

    // sum histograms and handle last update if num_updates % 2 == 1
    for (size_t i = 0; i < fanout; i++) {
      child_hist[i] += child_hist2[i];
    }
    if ((num_updates & 0x1) == 1) {
      ++child_hist[extract_left_bits(srcs[num_updates - 1], child_bits) & child_mask];
    }
  }
  

  // Reserve the positions in the children
  LeafWritePos positions[fanout];
  for (size_t i = 0; i < fanout; i++) {
    if (child_hist[i] > 0) {
      positions[i] = CGsystem.leaf_reserve(first_child + i, child_hist[i]);

      // number of updates that don't go into first leaf
      child_hist[i] -= (positions[i].last_pos - positions[i].cur_idx);
    }
  }

  // Write updates to children
  for (size_t i = 0; i < num_updates; i++) {
    node_id_t src = srcs[i];
    node_id_t dst = dsts[i];
    // std::cerr << i << ": " << src << "," << dst;
    node_id_t child_idx = extract_left_bits(src, child_bits) & child_mask;

    assert(src >= bounds.first && src < bounds.second);

    auto &write_pos = positions[child_idx];
    assert(write_pos.cur_idx < write_pos.last_pos);
    // std::cerr << " -> (" << child_idx << ", "<< write_pos.cur_idx << ")" << std::endl;
    write_pos.gutter->data[write_pos.cur_idx] = dst;
    write_pos.cur_idx++;

    if (write_pos.cur_idx == write_pos.last_pos && child_hist[child_idx] > 0) {
      write_pos.gutter->active_inserts--;
      write_pos.gutter->flush_if_can(*this);

      write_pos = CGsystem.leaf_reserve(first_child + child_idx, child_hist[child_idx]);

      // number of updates that don't go into next gutter
      assert(child_hist[child_idx] >= write_pos.last_pos - write_pos.cur_idx);
      child_hist[child_idx] -= (write_pos.last_pos - write_pos.cur_idx);
    }
  }

  // Decrement active_inserts for our children and flush if necessary
  for (size_t i = 0; i < fanout; i++) {
    if (positions[i].gutter != nullptr) {
      positions[i].gutter->active_inserts--;
      positions[i].gutter->flush_if_can(*this);
    }
  }
}

void CacheGuttering::SharedGutter::flush(InsertThread &thr) {
  // std::cerr << "SharedGutter " << index << " flush()" << std::endl;
  assert(active_inserts == 0);

  size_t num_updates = std::min(capacity, insert_pos.load());

  if (level == 3 && CGsystem.level4_gutters != nullptr) {
#ifdef EXIT_LEVEL3
    insert_pos = 0;
#else
    // inserting to level4 gutter
    thr.place_upds_in_gutters(srcs, dsts, index, num_updates, level + 1);
#endif
  } else {
#if defined(EXIT_LEVEL3) || defined(EXIT_LEVEL4)
    insert_pos = 0;
#else
    // inserting to leaf gutter
    thr.place_upds_in_leaves(srcs, dsts, index, num_updates, level + 1);
#endif
  }
}

void CacheGuttering::LeafGutter::flush(InsertThread &thr) {
  // std::cerr << "LeafGutter " << index << " flush()" << std::endl;
  assert(active_inserts == 0);

  size_t we_think_size_is = std::min(insert_pos.load(), capacity);
  insert_pos = we_think_size_is;
  thr.wq_push_helper(index, *this, we_think_size_is);
}

CacheGuttering::SharedWritePos CacheGuttering::shared_reserve(SharedGutter **gutters,
                                                              const size_t buf_idx,
                                                              size_t num_updates) {
  assert(num_updates > 0);
  while (true) {
    SharedGutter *gutter = gutters[buf_idx];
    gutter->active_inserts++;
    SharedWritePos write_pos = gutter->reserve_positions(gutters, num_updates);

    if (write_pos.last_pos > 1000000) {
      std::cerr << "ERROR: This is odd" << std::endl;
      throw std::runtime_error("ahhhhh");
    }

    if (write_pos.last_pos > 0)
      return write_pos;
    else
      gutter->active_inserts--; // we couldn't get a reservation. Try again!
  }
}

CacheGuttering::LeafWritePos CacheGuttering::leaf_reserve(const size_t buf_idx,
                                                          size_t num_updates) {
  assert(num_updates > 0);
  while (true) {
    LeafGutter *gutter = leaf_gutters[buf_idx];
    gutter->active_inserts++;
    LeafWritePos write_pos = gutter->reserve_positions(num_updates);
    
    if (write_pos.last_pos > 0)
      return write_pos;
    else
      gutter->active_inserts--; // we couldn't get a reservation. Try again!
  }
}

CacheGuttering::SharedWritePos CacheGuttering::SharedGutter::reserve_positions(
    SharedGutter **gutters, size_t num_updates) {
  // std::cerr << "SharedGutter " << index << " batch_insert(" << num_updates << ") " << std::endl;
  SharedWritePos ret;

  if (insert_pos >= capacity) {
    return ret;
  }

  size_t pos = insert_pos.fetch_add(num_updates);

  if (pos >= capacity) {
    return ret;
  }

  if (pos > 1000000) {
    throw std::runtime_error("Ahhhh");
  }

  ret.gutter = this;
  ret.cur_idx = pos;
  ret.last_pos = std::min(capacity, pos + num_updates);

  if (pos < capacity && pos + num_updates >= capacity) {
    size_t fanout = level == 3 ? local_fanout : global_fanout;
    size_t extra_idx =
        CGsystem.extra_gutters_idxs[level - 3]->fetch_add(1) % (CGsystem.inserters * fanout);

    // swap an extra gutter with the gutter we are flushing
    // after this step completes, other threads can start populating the extra buffer
    SharedGutter *&extra_buf = CGsystem.extra_gutters[level - 3][extra_idx];
    extra_buf->insert_pos = 0;
    extra_buf->index = index;
    assert(extra_buf->active_inserts == 0);
    std::swap(gutters[index], extra_buf);
  }

  return ret;
}

CacheGuttering::LeafWritePos CacheGuttering::LeafGutter::reserve_positions(size_t num_updates) {
  // std::cerr << "LeafGutter " << index << " reserve_positions(" << num_updates << ") " << std::endl;
  LeafWritePos ret;

  if (insert_pos >= capacity) {
    return ret;
  }

  size_t pos = insert_pos.fetch_add(num_updates);

  if (pos >= capacity) {
    return ret;
  }

  ret.gutter = this;
  ret.cur_idx = pos;
  ret.last_pos = std::min(capacity, pos + num_updates);
  
  if (pos < capacity && pos + num_updates >= capacity) {
    size_t extra_idx =
        CGsystem.leaf_extra_buf_idx.fetch_add(1) % (CGsystem.inserters * CGsystem.global_fanout);

    // swap our extra gutter with the gutter we are flushing
    // after this step completes, other threads can start populating our extra buffer
    LeafGutter *&extra_buf = CGsystem.extra_leaves[extra_idx];
    extra_buf->insert_pos = 0;
    extra_buf->index = index;
    assert(extra_buf->active_inserts == 0);
    std::swap(CGsystem.leaf_gutters[index], extra_buf);
  }

  return ret;
}

void CacheGuttering::InsertThread::wq_push_helper(node_id_t node_idx, LeafGutter &leaf, size_t exp_size) {
  // std::cerr << "Flushing LeafGutter " << leaf.index << " (" << leaf.insert_pos << ", " << leaf.capacity << ")" << std::endl;

  if (leaf.insert_pos > leaf.capacity) {
    std::cerr << "ERROR: LeafGutter is too big!" << std::endl;
    std::cerr << "Flushing LeafGutter " << leaf.index << " (" << leaf.insert_pos << ", " << leaf.capacity << ")" << std::endl;
    std::cerr << "Expected size = " << exp_size << std::endl;
    exit(EXIT_FAILURE);
  }

  leaf.data.resize(leaf.insert_pos); // no memory reallocation because insert_pos <= capacity
  local_wq_buffer.batches[local_wq_buffer.size].node_idx = node_idx + CGsystem.relabelling_offset;
  std::swap(leaf.data, local_wq_buffer.batches[local_wq_buffer.size].upd_vec);
  leaf.data.resize(leaf.capacity); // make room for new updates

  ++local_wq_buffer.size;
  if (local_wq_buffer.size >= CGsystem.wq_batch_per_elm)
    flush_wq_buf();
  leaf.insert_pos = 0;
}

void CacheGuttering::InsertThread::flush_wq_buf() {
  // if nothing to flush then don't
  if (local_wq_buffer.size == 0) return;

  // std::cerr << "Flushing wq buffer: " << local_wq_buffer.size << std::endl;
#ifndef EXIT_LEAVES
  // if wq buffer size is less than expected
  if (local_wq_buffer.size < CGsystem.wq_batch_per_elm) {
    // clear the batches beyond wq buffer size
    for (size_t i = local_wq_buffer.size; i < CGsystem.wq_batch_per_elm; i++)
      local_wq_buffer.batches[i].upd_vec.clear();
  }

  // perform the flush
  CGsystem.wq.push(local_wq_buffer.batches);
#endif
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
#if !defined(EXIT_LEVEL1) && !defined(EXIT_LEVEL2)
    if (level3_gutters != nullptr) {
      for (size_t i = min; i < max; i++) {
        level3_gutters[i]->flush(insert_threads[thr]);
      }
      min <<= fanout_bits[2]; // multiply by L3 fanout
      max <<= fanout_bits[2];
    }
#ifndef EXIT_LEVEL3
    if (level4_gutters != nullptr) {
      for (size_t i = min; i < max; i++) {
        level4_gutters[i]->flush(insert_threads[thr]);
      }
      min <<= fanout_bits[3]; // multiply by L4 fanout
      max <<= fanout_bits[3];
    }
#ifndef EXIT_LEVEL4
    for (size_t i = min; i < max && i < num_nodes; i++) {
      leaf_gutters[i]->flush(insert_threads[thr]);
    }
#endif
#endif
#endif
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
