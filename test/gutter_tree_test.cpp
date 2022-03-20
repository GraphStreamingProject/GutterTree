#include <gtest/gtest.h>
#include <math.h>
#include <thread>
#include <atomic>
#include <fstream>

#include "../include/gutter_tree.h"

#define KB (1 << 10)
#define MB (1 << 20)
#define GB (1 << 30)

static bool shutdown = false;
static std::atomic<uint32_t> upd_processed;

// queries the guttering system and verifies that the data
// returned makes sense
// Should be run in a seperate thread
static void querier(GutterTree *tree, int nodes) {
  WorkQueue::DataNode *data;
  while(true) {
    bool valid = tree->get_data(data);
    if (valid) {
      node_id_t key = data->get_node_idx();
      std::vector<node_id_t> updates = data->get_data_vec();
      // verify that the updates are all between the correct nodes
      for (auto upd : updates) {
        // printf("edge to %d\n", upd.first);
        ASSERT_EQ(nodes - (key + 1), upd) << "key " << key;
        upd_processed += 1;
      }
      tree->get_data_callback(data);
    }
    else if(shutdown)
      return;
  }
}

static void batch_querier(GutterTree *tree, int nodes, int batch_size) {
  std::vector<WorkQueue::DataNode *> data_vec;
  while(true) {
    bool valid = tree->get_data_batched(data_vec, batch_size);
    if (valid) {
      // printf("Got batched data vector of size %lu\n", data_vec.size());
      for (auto data : data_vec) {
        node_id_t key = data->get_node_idx();
        std::vector<node_id_t> updates = data->get_data_vec();
        // verify that the updates are all between the correct nodes
        for (auto upd : updates) {
          // printf("edge to %d\n", upd.first);
          ASSERT_EQ(nodes - (key + 1), upd) << "key " << key;
          upd_processed += 1;
        }
        tree->get_data_callback(data);
      }
    }
    else if(shutdown)
      return;
  }
}

static void write_configuration(uint32_t buffer_exp, uint32_t fanout, int queue_factor, 
                        int page_factor, int num_threads) {
  std::ofstream conf("./buffering.conf");
  conf << "buffer_exp=" << buffer_exp << std::endl;
  conf << "branch=" << fanout << std::endl;
  conf << "queue_factor=" << queue_factor << std::endl;
  conf << "page_factor=" << page_factor << std::endl;
  conf << "num_threads=" << num_threads << std::endl;
}

// helper function to run a basic test of the buffer tree with
// various parameters
// this test only works if the depth of the tree does not exceed 1
// and no work is claimed off of the work queue
// to work correctly num_updates must be a multiple of nodes
static void run_test(const uint32_t nodes, const uint32_t num_updates, const int buffer_exp, const int branch_factor) {
  printf("Running Test: nodes=%i num_updates=%i buffer_exp 2^%i branch_factor %i\n",
         nodes, num_updates, buffer_exp, branch_factor);

  write_configuration(buffer_exp, branch_factor, 8, 1, 1); // 8 is queue_factor, 1 is page_factor, 1 is num_threads
  GutterTree *gt = new GutterTree("./test_", nodes, 1, true); // 1 is number of workers
  shutdown = false;
  upd_processed = 0;
  std::thread qworker(querier, gt, nodes);

  for (unsigned i = 0; i < num_updates; i++) {
    update_t upd;
    upd.first = i % nodes;
    upd.second = (nodes - 1) - (i % nodes);
    gt->insert(upd);
  }
  printf("force flush\n");
  gt->force_flush();
  shutdown = true;
  gt->set_non_block(true); // switch to non-blocking calls in an effort to exit
  qworker.join();
  ASSERT_EQ(num_updates, upd_processed);
  delete gt;
}

TEST(GutterTreeTests, Small) {
  const int nodes       = 10;
  const int num_updates = 400;
  const int buf_exp     = 12;
  const int branch      = 2;

  run_test(nodes, num_updates, buf_exp, branch);
}

TEST(GutterTreeTests, Medium) {
  const int nodes       = 100;
  const int num_updates = 360000;
  const int buf_exp     = 20;
  const int branch      = 8;

  run_test(nodes, num_updates, buf_exp, branch);
}

TEST(GutterTreeTests, ManyInserts) {
  const int nodes       = 32;
  const int num_updates = 1000000;
  const int buf_exp     = 20;
  const int branch      = 2;

  run_test(nodes, num_updates, buf_exp, branch);
}

// test designed to trigger recursive flushes
// Insert full root buffers which are 95% node 0 and 5% a node
// which will make 95% split from 5% at different levels of 
// the tree. We do this process from bottom to top. When this
// is done. We insert a full buffer of 0 updates.
//
// For exampele 0 and 2, then 0 and 4, etc. 
TEST(GutterTreeTests, EvilInsertions) {
  int full_root = MB/GutterTree::serial_update_size;
  const int nodes       = 32;
  const int num_updates = 4 * full_root;
  const int buf_exp     = 20;
  const int branch      = 2;

  write_configuration(buf_exp, branch, 8, 5, 1); // 8=queue_factor, 5=page_factor, 1=threads
  GutterTree *gt = new GutterTree("./test_", nodes, 1, true); //1=num_workers
  shutdown = false;
  upd_processed = 0;
  std::thread qworker(querier, gt, nodes);

  for (int l = 1; l <= 3; l++) {
    for (int i = 0; i < full_root; i++) {
      update_t upd;
      if (i < .95 * full_root) {
        upd.first  = 0;
        upd.second = (nodes - 1) - (0 % nodes);
      } else {
        upd.first  = 1 << l;
        upd.second = (nodes - 1) - (upd.first % nodes);
      }
      gt->insert(upd);
    }
  }
  
  for (int n = 0; n < full_root; n++) {
    update_t upd;
    upd.first = 0;
    upd.second = (nodes - 1) - (0 % nodes);
    gt->insert(upd);
  }
  gt->force_flush();
  shutdown = true;
  gt->set_non_block(true); // switch to non-blocking calls in an effort to exit

  qworker.join();
  ASSERT_EQ(num_updates, upd_processed);
  delete gt;
}


TEST(GutterTreeTests, ParallelInsert) {
  // fairly large number of updates and small buffers
  // to create a large number of flushes from root buffers
  const int nodes       = 1024;
  const int num_updates = 400000;
  const int buf_exp     = 17;
  const int branch      = 8;

  // here we limit the number of slots in the circular queue to 
  // create contention between the threads. 
  // (we say num_workers=5 and queue factor=1 instead of 20,8)
  write_configuration(buf_exp, branch, 1, 1, 8); // also 8=num_threads=branch
  GutterTree *gt = new GutterTree("./test_", nodes, 5, true);
  shutdown = false;
  upd_processed = 0;
  std::thread query_threads[20];
  for (int t = 0; t < 20; t++) {
    query_threads[t] = std::thread(querier, gt, nodes);
  }
  
  for (int i = 0; i < num_updates; i++) {
    update_t upd;
    upd.first = i % nodes;
    upd.second = (nodes - 1) - (i % nodes);
    gt->insert(upd);
  }
  printf("force flush\n");
  gt->force_flush();
  shutdown = true;
  gt->set_non_block(true); // switch to non-blocking calls in an effort to exit

  for (int t = 0; t < 20; t++) {
    query_threads[t].join();
  }
  ASSERT_EQ(num_updates, upd_processed);
  delete gt;
}

TEST(GutterTreeTests, ManyQueryThreads) {
  const int nodes       = 1024;
  const int num_updates = 5206;
  const int buf_exp     = 17;
  const int branch      = 8;

  // here we limit the number of slots in the circular queue to 
  // create contention between the threads. (we pass 5 threads and queue factor =1 instead of 20,8)
  write_configuration(buf_exp, branch, 1, 1, 1); // 1 is queue_factor

  GutterTree *gt = new GutterTree("./test_", nodes, 5, true); // 5 is the number of workers
  shutdown = false;
  upd_processed = 0;
  std::thread query_threads[20];
  for (int t = 0; t < 20; t++) {
    query_threads[t] = std::thread(querier, gt, nodes);
  }
  
  for (int i = 0; i < num_updates; i++) {
    update_t upd;
    upd.first = i % nodes;
    upd.second = (nodes - 1) - (i % nodes);
    gt->insert(upd);
  }
  gt->force_flush();
  shutdown = true;
  gt->set_non_block(true); // switch to non-blocking calls in an effort to exit

  for (int t = 0; t < 20; t++) {
    query_threads[t].join();
  }
  ASSERT_EQ(num_updates, upd_processed);
  delete gt;
}
