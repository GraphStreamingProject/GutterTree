#include <gtest/gtest.h>
#include <thread>
#include <atomic>
#include <fstream>
#include <math.h>
#include "standalone_gutters.h"
#include "gutter_tree.h"

#define KB (1 << 10)
#define MB (1 << 20)
#define GB (1 << 30)

static bool shutdown = false;
static std::atomic<uint32_t> upd_processed;

enum SystemEnum {
  GUTTREE,
  STANDALONE
};

struct GutterConfig {
  int buffer_exp = 20;
  int branch = 64;
  int queue_factor = 2;
  int page_factor = 1;
  int num_flushers = 1;
  int gutter_factor = 1;

  void write() {
    std::ofstream conf("./buffering.conf");
    conf << "# configuration created by unit test" << std::endl;
    conf << "buffer_exp=" << buffer_exp << std::endl;
    conf << "branch=" << branch << std::endl;
    conf << "queue_factor=" << queue_factor << std::endl;
    conf << "page_factor=" << page_factor << std::endl;
    conf << "num_threads=" << num_flushers << std::endl;
    conf << "gutter_factor=" << gutter_factor << std::endl;
  }
};

// queries the buffer tree and verifies that the data
// returned makes sense
// Should be run in a seperate thread
static void querier(GutteringSystem *gts, int nodes) {
  WorkQueue::DataNode *data;
  while(true) {
    bool valid = gts->get_data(data);
    if (valid) {
      node_id_t key = data->get_node_idx();
      std::vector<node_id_t> updates = data->get_data_vec();
      // verify that the updates are all between the correct nodes
      for (auto upd : updates) {
        // printf("edge to %d\n", upd.first);
        ASSERT_EQ(nodes - (key + 1), upd) << "key " << key;
        upd_processed += 1;
      }
      gts->get_data_callback(data);
    }
    else if(shutdown)
      return;
  }
}

static void batch_querier(GutteringSystem *gts, int nodes, int batch_size) {
  std::vector<WorkQueue::DataNode *> data_vec;
  while(true) {
    bool valid = gts->get_data_batched(data_vec, batch_size);
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
        gts->get_data_callback(data);
      }
    }
    else if(shutdown)
      return;
  }
}

class GuttersTest : public testing::TestWithParam<SystemEnum> {};
INSTANTIATE_TEST_SUITE_P(GutteringTestSuite, GuttersTest, testing::Values(GUTTREE, STANDALONE));

// helper function to run a basic test of the buffer tree with
// various parameters
// this test only works if the depth of the tree does not exceed 1
// and no work is claimed off of the work queue
// to work correctly num_updates must be a multiple of nodes
static void run_test(const int nodes, const int num_updates, const int data_workers, 
 const SystemEnum gts_enum) {
  GutteringSystem *gts;
  std::string system_str;
  if (gts_enum == GUTTREE) {
    system_str = "GutterTree";
    gts = new GutterTree("./test_", nodes, data_workers, true);
  }
  else if (gts_enum == STANDALONE) {
    system_str = "StandAloneGutters";
    gts = new StandAloneGutters(nodes, data_workers);
  }
  else {
    printf("Did not recognize gts_enum!\n");
    exit(EXIT_FAILURE);
  }
  printf("Running Test: system=%s, nodes=%i, num_updates=%i\n",
    system_str.c_str(), nodes, num_updates);

  shutdown = false;
  upd_processed = 0;

  std::thread query_threads[data_workers];
  for (int t = 0; t < data_workers; t++)
    query_threads[t] = std::thread(querier, gts, nodes);

  for (int i = 0; i < num_updates; i++) {
    update_t upd;
    upd.first = i % nodes;
    upd.second = (nodes - 1) - (i % nodes);
    gts->insert(upd);
  }
  printf("force flush\n");
  gts->force_flush();
  shutdown = true;
  gts->set_non_block(true); // switch to non-blocking calls in an effort to exit
  
  for (int t = 0; t < data_workers; t++)
    query_threads[t].join();

  ASSERT_EQ(num_updates, upd_processed);
  delete gts;
}

TEST_P(GuttersTest, Small) {
  const int nodes = 10;
  const int num_updates = 400;
  const int data_workers = 1;
  
  // Guttering System configuration
  GutterConfig conf;
  conf.buffer_exp = 12;
  conf.branch = 2;
  conf.write();

  run_test(nodes, num_updates, data_workers, GetParam());
}

TEST_P(GuttersTest, Medium) {
  const int nodes = 100;
  const int num_updates = 360000;
  const int data_workers = 1;

  // Guttering System configuration
  GutterConfig conf;
  conf.buffer_exp = 20;
  conf.branch = 8;
  conf.write();

  run_test(nodes, num_updates, data_workers, GetParam());
}

TEST_P(GuttersTest, ManyInserts) {
  const int nodes = 32;
  const int num_updates = 1000000;
  const int data_workers = 4;

  // Guttering System configuration
  GutterConfig conf;
  conf.buffer_exp = 20;
  conf.branch = 2;
  conf.write();

  run_test(nodes, num_updates, data_workers, GetParam());
}

TEST_P(GuttersTest, TinyGutters) {
  const int nodes = 128;
  const int num_updates = 40000;
  const int data_workers = 4;

  // gutter factor to make buffers size 1
  GutterConfig conf;
  conf.buffer_exp = 16;
  conf.branch = 16;
  conf.gutter_factor = -1 * GutteringSystem::upds_per_sketch(nodes);
  conf.queue_factor = 1;
  conf.write();

  run_test(nodes, num_updates, data_workers, GetParam());
}

TEST_P(GuttersTest, FlushAndInsertAgain) {
  const int nodes       = 1024;
  const int num_updates = 10000;
  const int num_flushes = 5;
  const int data_workers = 20;

  // gutter factor to make buffers size 1
  GutterConfig conf;
  conf.gutter_factor = 2;
  conf.write();

  SystemEnum gts_enum = GetParam();
  GutteringSystem *gts;
  std::string system_str;
  if (gts_enum == GUTTREE) {
    system_str = "GutterTree";
    gts = new GutterTree("./test_", nodes, data_workers, true);
  }
  else if (gts_enum == STANDALONE) {
    system_str = "StandAloneGutters";
    gts = new StandAloneGutters(nodes, data_workers);
  }
  else {
    printf("Did not recognize gts_enum!\n");
    exit(EXIT_FAILURE);
  }
  printf("Running Test: system=%s, nodes=%i, num_updates=%i\n",
    system_str.c_str(), nodes, num_updates);

  shutdown = false;
  upd_processed = 0;

  std::thread query_threads[data_workers];
  for (int t = 0; t < data_workers; t++)
    query_threads[t] = std::thread(querier, gts, nodes);

  for (int f = 0; f < num_flushes; f++) {
    for (int i = 0; i < num_updates; i++) {
      update_t upd;
      upd.first = i % nodes;
      upd.second = (nodes - 1) - (i % nodes);
      gts->insert(upd);
    }
    gts->force_flush();
  }

  // flush again to ensure that doesn't cause problems
  gts->force_flush();
  shutdown = true;
  gts->set_non_block(true); // switch to non-blocking calls in an effort to exit
  
  for (int t = 0; t < data_workers; t++)
    query_threads[t].join();

  ASSERT_EQ(num_updates * num_flushes, upd_processed);
  delete gts;
}

TEST_P(GuttersTest, GetDataBatched) {
  const int nodes = 2048;
  const int num_updates = 100000;
  const int data_batch_size = 8;
  const int data_workers = 10;

  // gutter factor to make buffers size 1
  GutterConfig conf;
  conf.queue_factor = 20;
  conf.write();

  SystemEnum gts_enum = GetParam();
  GutteringSystem *gts;
  std::string system_str;
  if (gts_enum == GUTTREE) {
    system_str = "GutterTree";
    gts = new GutterTree("./test_", nodes, data_workers, true);
  }
  else if (gts_enum == STANDALONE) {
    system_str = "StandAloneGutters";
    gts = new StandAloneGutters(nodes, data_workers);
  }
  else {
    printf("Did not recognize gts_enum!\n");
    exit(EXIT_FAILURE);
  }
  printf("Running Test: system=%s, nodes=%i, num_updates=%i\n",
    system_str.c_str(), nodes, num_updates);

  shutdown = false;
  upd_processed = 0;

  std::thread query_threads[data_workers];
  for (int t = 0; t < data_workers; t++)
    query_threads[t] = std::thread(batch_querier, gts, nodes, data_batch_size);

  for (int i = 0; i < num_updates; i++) {
    update_t upd;
    upd.first = i % nodes;
    upd.second = (nodes - 1) - (i % nodes);
    gts->insert(upd);
  }

  gts->force_flush();
  shutdown = true;
  gts->set_non_block(true); // switch to non-blocking calls in an effort to exit
  
  for (int t = 0; t < data_workers; t++)
    query_threads[t].join();

  ASSERT_EQ(num_updates, upd_processed);
  delete gts;
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

  GutterConfig conf;
  conf.buffer_exp = 20;
  conf.branch = 2;
  conf.write();

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

  GutterConfig conf;
  conf.buffer_exp = 17;
  conf.branch = 8;
  conf.num_flushers = 8;
  conf.queue_factor = 1;
  conf.write();

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

