#include "cache_guttering.h"

#include <iostream>

inline static node_id_t extract_left_bits(node_id_t number, int pos) {
  number >>= pos;
  return number;
}

void CacheGuttering::print_r_to_l(node_id_t src) {
  std::cout << "src: " << src;
  std::cout << "->" << extract_left_bits(src, l1_pos);
  std::cout << "->" << extract_left_bits(src, l2_pos);
  std::cout << "->" << extract_left_bits(src, l3_pos);
  std::cout << "->" << extract_left_bits(src, RAM1_pos);
  std::cout << std::endl;
}

CacheGuttering::CacheGuttering(node_id_t num_nodes, uint32_t workers, uint32_t inserters)
 : GutteringSystem(num_nodes, workers), inserters(inserters), num_nodes(num_nodes), 
   l1_pos(ceil(log2(num_nodes)) - l1_bits),
   l2_pos(std::max((int)ceil(log2(num_nodes)) - l2_bits, 0)), 
   l3_pos(std::max((int)ceil(log2(num_nodes)) - l3_bits, 0)),
   RAM1_pos(std::max((int)ceil(log2(num_nodes)) - RAM1_bits, 0)) {
  
  // initialize storage for inserter threads
  insert_threads.reserve(inserters);
  for (uint32_t t = 0; t < inserters; t++) 
    insert_threads.emplace_back(*this);

  // initialize RAM1_gutters if necessary
  if (max_RAM1_bufs < num_nodes) {

    RAM1_fanout = num_nodes / max_RAM1_bufs;
    RAM1_fanout += num_nodes % max_RAM1_bufs == 0 ? 0 : 1;

    RAM1_buf_elms = RAM1_fanout * RAM_bytes_per_child / sizeof(update_t);

    std::cout << "Using RAM1 buffer" << std::endl;
    std::cout << "RAM1 fanout   = " << RAM1_fanout << std::endl;
    std::cout << "RAM1 elements = " << RAM1_buf_elms << std::endl;

    RAM1_gutters = new RAM_Gutter[max_RAM1_bufs];
    for (node_id_t i = 0; i < max_RAM1_bufs; ++i)
      RAM1_gutters[i].reserve(RAM1_buf_elms);
  }

  // initialize leaf gutters
  leaf_gutters = new Leaf_Gutter[num_nodes];
  for (node_id_t i = 0; i < num_nodes; ++i)
    leaf_gutters[i].reserve(leaf_gutter_size);

  // initialize l2 locks
  L2_flush_locks = new std::mutex[num_l2_bufs];

  // for debugging -- print out root to leaf paths for every id
  // for (node_id_t i = 0; i < num_nodes; i++)
  //   print_r_to_l(i);
}

CacheGuttering::~CacheGuttering() {
  delete[] leaf_gutters;
  delete[] RAM1_gutters;
  delete[] L2_flush_locks;
}

void CacheGuttering::InsertThread::insert(const update_t &upd) {
  node_id_t l1_idx = extract_left_bits(upd.first, CGsystem.l1_pos);
  auto &gutter = l1_gutters[l1_idx];
  gutter.data[gutter.num_elms++] = upd;

  // std::cout << "Handling update " << upd.first << ", " << upd.second << std::endl;
  // std::cout << "Placing in L1 buffer " << l1_idx << ", num_elms = " << gutter.num_elms << std::endl;

  if (gutter.num_elms >= l1l2buffer_elms) {
    // std::cout << "Flushing L1 gutter" << std::endl;
    flush_buf_l1(l1_idx);
  }
}

void CacheGuttering::InsertThread::flush_buf_l1(const node_id_t idx) {
  auto &l1_gutter = l1_gutters[idx];
  for (size_t i = 0; i < l1_gutter.num_elms; i++) {
    update_t upd = l1_gutter.data[i];
    node_id_t l2_idx = extract_left_bits(upd.first, CGsystem.l2_pos);
    auto &l2_gutter = l2_gutters[l2_idx];
    l2_gutter.data[l2_gutter.num_elms++] = upd;
    if (l2_gutter.num_elms >= l1l2buffer_elms)
      flush_buf_l2(l2_idx);
  }
  l1_gutter.num_elms = 0;
}

void CacheGuttering::InsertThread::flush_buf_l2(const node_id_t idx) {
  // lock associated mutex for this l2 gutter
  CGsystem.L2_flush_locks[idx].lock();

  auto &l2_gutter = l2_gutters[idx];
  for (size_t i = 0; i < l2_gutter.num_elms; i++) {
    update_t upd = l2_gutter.data[i];
    node_id_t l3_idx = extract_left_bits(upd.first, CGsystem.l3_pos);
    assert(l3_idx >> (CGsystem.l2_pos - CGsystem.l3_pos) == idx); 
    //   std::cout << "l2pos=" << CGsystem.l2_pos << " l3pos=" << CGsystem.l3_pos << std::endl;
    //   std::cout << ((l3_idx >> (CGsystem.l2_pos - CGsystem.l3_pos)) ^ idx) << std::endl;
    //   std::cout << "idx=" << idx << " l3_idx=" << l3_idx << std::endl;
    //   exit(1);
    // }
    auto &l3_gutter = CGsystem.l3_gutters[l3_idx];
    l3_gutter.data[l3_gutter.num_elms++] = upd;
    if (l3_gutter.num_elms >= l3buffer_elms)
      CGsystem.flush_buf_l3(l3_idx);
  }

  // unlock
  CGsystem.L2_flush_locks[idx].unlock();

  l2_gutter.num_elms = 0;
}

void CacheGuttering::flush_buf_l3(const node_id_t idx) {
  auto &l3_gutter = l3_gutters[idx];
  if (RAM1_gutters == nullptr) {
    // flush directly to leaves
    for (size_t i = 0; i < l3_gutter.num_elms; i++) {
      update_t upd = l3_gutter.data[i];
      Leaf_Gutter &leaf = leaf_gutters[upd.first];
      // std::cout << "L3 Handling update " << upd.first << ", " << upd.second << std::endl;
      leaf.push_back(upd.second);
      if (leaf.size() >= leaf_gutter_size) {
        assert(leaf.size() == leaf_gutter_size);
        wq.push(upd.first, leaf);
        leaf.clear();
      }
    }
  } else {
    // flush to RAM1 gutters
    for (size_t i = 0; i < l3_gutter.num_elms; i++) {
      update_t upd = l3_gutter.data[i];
      node_id_t RAM1_idx = extract_left_bits(upd.first, RAM1_pos);
      RAM_Gutter &gutter = RAM1_gutters[RAM1_idx];
      gutter.push_back(upd);
      if (gutter.size() >= RAM1_buf_elms) {
        assert(gutter.size() == RAM1_buf_elms);
        flush_RAM_l1(RAM1_idx);
      }
    }
  }
  l3_gutter.num_elms = 0;
}

void CacheGuttering::flush_RAM_l1(const node_id_t idx) {
  RAM_Gutter &gutter = RAM1_gutters[idx];
  for (update_t upd : gutter) {
    Leaf_Gutter &leaf = leaf_gutters[upd.first];
    leaf.push_back(upd.second);
    if (leaf.size() >= leaf_gutter_size) {
      assert(leaf.size() == leaf_gutter_size);
      wq.push(upd.first, leaf);
      leaf.clear();
    }
  }
  gutter.clear();
}

void CacheGuttering::force_flush() {
  for (auto &thr : insert_threads) {
    for (size_t i = 0; i < num_l1_bufs; i++)
      thr.flush_buf_l1(i);
    for (size_t i = 0; i < num_l2_bufs; i++)
      thr.flush_buf_l2(i);
  }

  for (size_t i = 0; i < num_l3_bufs; i++)
    flush_buf_l3(i);

  if (RAM1_gutters != nullptr) {
    for (size_t i = 0; i < max_RAM1_bufs; i++)
      flush_RAM_l1(i);
  }

  for (node_id_t i = 0; i < num_nodes; i++) {
    if (leaf_gutters[i].size() > 0) {
      // std::cout << "flushing leaf gutter " << i << " with " << leaf_gutters[i].size() << " updates" << std::endl;
      assert(leaf_gutters[i].size() <= leaf_gutter_size);
      wq.push(i, leaf_gutters[i]);
      leaf_gutters[i].clear();
    }
  }
}
