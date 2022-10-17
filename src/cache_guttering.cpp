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
  std::cout << "->(L3)" << extract_left_bits(src, level3_pos);
  if (level4_gutters)
    std::cout << "->(RM)" << extract_left_bits(src, level4_pos);
  std::cout << std::endl;
}

CacheGuttering::CacheGuttering(node_id_t num_nodes, uint32_t workers, uint32_t inserters, 
 const GutteringConfiguration &conf) : GutteringSystem(num_nodes, workers, conf), inserters(inserters), 
 num_nodes(num_nodes), level1_pos(ceil(log2(num_nodes)) - level1_bits),
 level2_pos(std::max((int)ceil(log2(num_nodes)) - level2_bits, 0)), 
 level3_pos(std::max((int)ceil(log2(num_nodes)) - level3_bits, 0)),
 level4_pos(std::max((int)ceil(log2(num_nodes)) - level4_bits, 0)) {
  
  // initialize storage for inserter threads
  insert_threads.reserve(inserters);
  for (uint32_t t = 0; t < inserters; t++) 
    insert_threads.emplace_back(*this);

  // initialize level4_gutters if necessary
  if (max_level4_bufs < num_nodes) {

    level4_fanout = num_nodes / max_level4_bufs;
    level4_fanout += num_nodes % max_level4_bufs == 0 ? 0 : 1;

    level4_elms_per_buf = level4_fanout * block_size / sizeof(update_t);

    std::cout << "Using level 4 buffer" << std::endl;
    std::cout << "level 4 fanout    = " << level4_fanout << std::endl;
    std::cout << "level 4 elems/buf = " << level4_elms_per_buf << std::endl;

    level4_gutters = new RAM_Gutter[max_level4_bufs];
    for (node_id_t i = 0; i < max_level4_bufs; ++i)
      level4_gutters[i].reserve(level4_elms_per_buf);
  }

  // initialize leaf gutters
  leaf_gutters = new Leaf_Gutter[num_nodes];
  for (node_id_t i = 0; i < num_nodes; ++i)
    leaf_gutters[i].reserve(leaf_gutter_size);

  // initialize l3 flush locks
  level3_flush_locks = new std::mutex[level3_bufs];
  
  std::cout << "Total InsertThreads bytes: " << sizeof(InsertThread) * inserters << std::endl;
  // for debugging -- print out root to leaf paths for every id
  // for (node_id_t i = 0; i < num_nodes; i++)
  //  print_r_to_l(i);
}

CacheGuttering::~CacheGuttering() {
  delete[] leaf_gutters;
  delete[] level4_gutters;
  delete[] level3_flush_locks;
}

void CacheGuttering::InsertThread::insert(update_t upd) {
  upd.first -= CGsystem.relabelling_offset;
  node_id_t l1_idx = extract_left_bits(upd.first, CGsystem.level1_pos);
  auto &gutter = level1_gutters[l1_idx];
  gutter.data[gutter.num_elms++] = upd;

  // std::cout << "Handling update " << upd.first << ", " << upd.second << std::endl;
  // std::cout << "Placing in L1 buffer " << l1_idx << ", num_elms = " << gutter.num_elms << std::endl;

  if (gutter.num_elms >= gutter.max_elms) {
    // std::cout << "Flushing L1 gutter" << std::endl;
    flush_buf_l1(l1_idx);
  }
}

void CacheGuttering::InsertThread::flush_buf_l1(const node_id_t idx) {
  auto &l1_gutter = level1_gutters[idx];
  for (size_t i = 0; i < l1_gutter.num_elms; i++) {
    update_t upd = l1_gutter.data[i];
    node_id_t l2_idx = extract_left_bits(upd.first, CGsystem.level2_pos);
    auto &l2_gutter = level2_gutters[l2_idx];
    l2_gutter.data[l2_gutter.num_elms++] = upd;
    if (l2_gutter.num_elms >= l2_gutter.max_elms)
      flush_buf_l2(l2_idx);
  }
  l1_gutter.num_elms = 0;
  l1_gutter.max_elms = level1_elms_per_buf;
}

void CacheGuttering::InsertThread::flush_buf_l2(const node_id_t idx) {
  auto &l2_gutter = level2_gutters[idx];
  for (size_t i = 0; i < l2_gutter.num_elms; i++) {
    update_t upd = l2_gutter.data[i];
    node_id_t l3_idx = extract_left_bits(upd.first, CGsystem.level3_pos);
    assert(l3_idx >> (CGsystem.level2_pos - CGsystem.level3_pos) == idx);
    //   std::cout << "l2pos=" << CGsystem.level2_pos << " l3pos=" << CGsystem.level3_pos << std::endl;
    //   std::cout << ((l3_idx >> (CGsystem.level2_pos - CGsystem.level3_pos)) ^ idx) << std::endl;
    //   std::cout << "idx=" << idx << " l3_idx=" << l3_idx << std::endl;
    //   exit(1);
    // }
    auto &l3_gutter = level3_gutters[l3_idx];
    l3_gutter.data[l3_gutter.num_elms++] = upd;
    if (l3_gutter.num_elms >= l3_gutter.max_elms)
      flush_buf_l3(l3_idx);
  }
  l2_gutter.num_elms = 0;
  l2_gutter.max_elms = level2_elms_per_buf;
}

void CacheGuttering::InsertThread::flush_buf_l3(const node_id_t idx) {
  // lock associated mutex for this level3 gutter
  CGsystem.level3_flush_locks[idx].lock();

  auto &l3_gutter = level3_gutters[idx];
  if (CGsystem.level4_gutters == nullptr) {
    // flush directly to leaves
    for (size_t i = 0; i < l3_gutter.num_elms; i++) {
      update_t upd = l3_gutter.data[i];
      Leaf_Gutter &leaf = CGsystem.leaf_gutters[upd.first];
      // std::cout << "L3 Handling update " << upd.first << ", " << upd.second << std::endl;
      leaf.push_back(upd.second);
      if (leaf.size() >= CGsystem.leaf_gutter_size) {
        assert(leaf.size() == CGsystem.leaf_gutter_size);
        wq_push_helper(upd.first, leaf);
      }
    }
  } else {
    // flush to level 4 gutters
    for (size_t i = 0; i < l3_gutter.num_elms; i++) {
      update_t upd = l3_gutter.data[i];
      node_id_t l4_idx = extract_left_bits(upd.first, CGsystem.level4_pos);
      RAM_Gutter &gutter = CGsystem.level4_gutters[l4_idx];
      gutter.push_back(upd);
      if (gutter.size() >= CGsystem.level4_elms_per_buf) {
        assert(gutter.size() == CGsystem.level4_elms_per_buf);
        flush_buf_l4(l4_idx);
      }
    }
  }
  l3_gutter.num_elms = 0;
  l3_gutter.max_elms = level3_elms_per_buf;
  // unlock
  CGsystem.level3_flush_locks[idx].unlock();
}

void CacheGuttering::InsertThread::flush_buf_l4(const node_id_t idx) {
  RAM_Gutter &gutter = CGsystem.level4_gutters[idx];
  for (update_t upd : gutter) {
    Leaf_Gutter &leaf = CGsystem.leaf_gutters[upd.first];
    leaf.push_back(upd.second);
    if (leaf.size() >= CGsystem.leaf_gutter_size) {
      assert(leaf.size() == CGsystem.leaf_gutter_size);
      wq_push_helper(upd.first, leaf);
    }
  }
  gutter.clear();
}

void CacheGuttering::InsertThread::wq_push_helper(node_id_t node_idx, Leaf_Gutter &leaf) {
  local_wq_buffer.batches[local_wq_buffer.size].node_idx = node_idx + CGsystem.relabelling_offset;
  std::swap(local_wq_buffer.batches[local_wq_buffer.size].upd_vec, leaf);
  ++local_wq_buffer.size;
  if (local_wq_buffer.size >= CGsystem.wq_batch_per_elm)
    flush_wq_buf();
  leaf.clear();
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
    flush_buf_l1(i);
  for (size_t i = 0; i < level2_bufs; i++)
    flush_buf_l2(i);
  for (size_t i = 0; i < level3_bufs; i++)
    flush_buf_l3(i);
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
  
  // flush level4 gutters if necessary
  if (level4_gutters != nullptr) {
    for (size_t i = 0; i < max_level4_bufs; i++)
      insert_threads[0].flush_buf_l4(i);
  }

  for (node_id_t i = 0; i < num_nodes; i++) {
    if (leaf_gutters[i].size() > 0) {
      // std::cout << "flushing leaf gutter " << i << " with " << leaf_gutters[i].size() << " updates" << std::endl;
      assert(leaf_gutters[i].size() <= leaf_gutter_size);
      insert_threads[0].wq_push_helper(i, leaf_gutters[i]);
      leaf_gutters[i].clear();
    }
  }

  // flush the local work queue buffer for each InsertThread
  for (size_t i = 0; i < inserters; i++)
    insert_threads[i].flush_wq_buf();
}
