#include <gtest/gtest.h>
#include <algorithm>
#include <thread>
#include <atomic>
#include <fstream>
#include <math.h>
#include "standalone_gutters.h"
#include "gutter_tree.h"
#include "cache_guttering.h"

#define KB (1 << 10)
#define MB (1 << 20)
#define GB (1 << 30)

static bool shutdown = false;
static std::atomic<uint32_t> upd_processed;

enum SystemEnum {
  GUTTREE,
  STANDALONE,
  CACHETREE
};

// queries the buffer tree and verifies that the data
// returned makes sense
// Should be run in a seperate thread
static void querier(GutteringSystem *gts, int nodes) {
  WorkQueue::DataNode *data;
  while(true) {
    bool valid = gts->get_data(data);
    if (valid) {
      std::vector<update_batch> batches = data->get_batches();
      for (auto batch : batches) {
        node_id_t key = batch.node_idx;
        std::vector<node_id_t> upd_vec = batch.upd_vec;
        // verify that the updates are all between the correct nodes
        for (auto upd : upd_vec) {
          // printf("edge from %u to %u\n", key, upd);
          ASSERT_EQ(nodes - (key + 1), upd) << "key " << key;
          upd_processed += 1;
        }
      }
      gts->get_data_callback(data);
    }
    else if(shutdown)
      return;
  }
}

class GuttersTest : public testing::TestWithParam<SystemEnum> {};
INSTANTIATE_TEST_SUITE_P(GutteringTestSuite, GuttersTest, testing::Values(GUTTREE, STANDALONE, CACHETREE));

// helper function to run a basic test of the buffer tree with
// various parameters
// this test only works if the depth of the tree does not exceed 1
// and no work is claimed off of the work queue
// to work correctly num_updates must be a multiple of nodes
static void run_test(const int nodes, const int num_updates, const int data_workers,
 const SystemEnum gts_enum, const GutteringConfiguration &conf, const int nthreads=1) {
  GutteringSystem *gts;
  std::string system_str;
  if (gts_enum == GUTTREE) {
    system_str = "GutterTree";
    gts = new GutterTree("./test_", nodes, data_workers, conf, true);
  }
  else if (gts_enum == STANDALONE) {
    system_str = "StandAloneGutters";
    gts = new StandAloneGutters(nodes, data_workers, nthreads, conf);
  }
  else if (gts_enum == CACHETREE) {
    system_str = "CacheGuttering";
    gts = new CacheGuttering(nodes, data_workers, nthreads, conf);
  }
  else {
    printf("Did not recognize gts_enum!\n");
    ASSERT_EQ(1, 0);
    exit(EXIT_FAILURE);
  }
  printf("Running Test: system=%s, nodes=%i, num_updates=%i\n",
    system_str.c_str(), nodes, num_updates);

  shutdown = false;
  upd_processed = 0;

  std::thread query_threads[data_workers];
  for (int t = 0; t < data_workers; t++)
    query_threads[t] = std::thread(querier, gts, nodes);

  // In case there are multiple threads
  std::vector<std::thread> threads;
  threads.reserve(nthreads);
  // This is the work to do per thread (rounded up)
  const int work_per = (num_updates+nthreads-1) / nthreads;

  auto task = [&](const int j){
    for (int i = j * work_per; i < (j+1) * work_per && i < num_updates; i++) {
      update_t upd;
      upd.first = i % nodes;
      upd.second = (nodes - 1) - (i % nodes);
      gts->insert(upd, j);
    }
  };

  //Spin up then join threads
  for (int j = 0; j < nthreads; j++)
    threads.emplace_back(task, j);
  for (int j = 0; j < nthreads; j++)
    threads[j].join();


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
  auto conf = GutteringConfiguration()
              .buffer_exp(15)
              .fanout(2);

  run_test(nodes, num_updates, data_workers, GetParam(), conf);
}

TEST_P(GuttersTest, Medium) {
  const int nodes = 100;
  const int num_updates = 360000;
  const int data_workers = 1;

  // Guttering System configuration
  auto conf = GutteringConfiguration()
              .buffer_exp(20)
              .fanout(8);

  run_test(nodes, num_updates, data_workers, GetParam(), conf);
}

TEST_P(GuttersTest, ManyInserts) {
  const int nodes = 32;
  const int num_updates = 1000000;
  const int data_workers = 4;

  // Guttering System configuration
  auto conf = GutteringConfiguration()
              .buffer_exp(20)
              .fanout(2);

  run_test(nodes, num_updates, data_workers, GetParam(), conf);
}

TEST(GuttersTest, ManyInsertsParallel) {
  const int nodes = 32;
  const int num_updates = 1000000;
  const int data_workers = 4;

  // Guttering System configuration
  auto conf = GutteringConfiguration()
              .buffer_exp(20)
              .fanout(2);

  run_test(nodes, num_updates, data_workers, STANDALONE, conf, 10);
}

TEST_P(GuttersTest, TinyGutters) {
  const int nodes = 128;
  const int num_updates = 40000;
  const int data_workers = 4;

  // gutter factor to make buffers size 1
  auto conf = GutteringConfiguration()
              .buffer_exp(16)
              .fanout(16)
              .gutter_factor(-1 * GutteringSystem::upds_per_sketch(nodes))
              .queue_factor(1);

  run_test(nodes, num_updates, data_workers, GetParam(), conf);
}

TEST_P(GuttersTest, FlushAndInsertAgain) {
  const int nodes       = 1024;
  const int num_updates = 10000;
  const int num_flushes = 3;
  const int data_workers = 20;

  // gutter factor to make buffers size 1
  auto conf = GutteringConfiguration()
              .gutter_factor(1);

  SystemEnum gts_enum = GetParam();
  GutteringSystem *gts;
  std::string system_str;
  if (gts_enum == GUTTREE) {
    system_str = "GutterTree";
    gts = new GutterTree("./test_", nodes, data_workers, conf, true);
  }
  else if (gts_enum == STANDALONE) {
    system_str = "StandAloneGutters";
    gts = new StandAloneGutters(nodes, data_workers, 1, conf);
  }
  else if (gts_enum == CACHETREE) {
    system_str = "CacheGuttering";
    gts = new CacheGuttering(nodes, data_workers, 1, conf);
  }
  else {
    printf("Did not recognize gts_enum!\n");
    ASSERT_EQ(1, 0);
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
  auto conf = GutteringConfiguration()
              .queue_factor(20)
              .wq_batch_per_elm(data_batch_size);

  SystemEnum gts_enum = GetParam();
  GutteringSystem *gts;
  std::string system_str;
  if (gts_enum == GUTTREE) {
    system_str = "GutterTree";
    gts = new GutterTree("./test_", nodes, data_workers, conf, true);
  }
  else if (gts_enum == STANDALONE) {
    system_str = "StandAloneGutters";
    gts = new StandAloneGutters(nodes, data_workers, 1, conf);
  }
  else if (gts_enum == CACHETREE) {
    system_str = "CacheGuttering";
    gts = new CacheGuttering(nodes, data_workers, 1, conf);
  }
  else {
    printf("Did not recognize gts_enum!\n");
    ASSERT_EQ(1, 0);
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

  auto conf = GutteringConfiguration()
              .buffer_exp(20)
              .fanout(2);

  GutterTree *gt = new GutterTree("./test_", nodes, 1, conf, true); //1=num_workers
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

  auto conf = GutteringConfiguration()
              .buffer_exp(17)
              .fanout(8)
              .num_flushers(8)
              .queue_factor(1);

  GutterTree *gt = new GutterTree("./test_", nodes, 5, conf, true);
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


TEST(StandaloneTest, ParallelInserts) {
  const int nodes = 32;
  const int num_updates = 1000000;
  const int data_workers = 4;
  const int nthreads = 10;

  GutteringConfiguration conf;

  run_test(nodes, num_updates, data_workers, STANDALONE, conf, nthreads);
}

TEST(CacheGutteringTest, ParallelInserts) {
  const int nodes = 32;
  const int num_updates = 5000000;
  const int data_workers = 4;
  const int nthreads = 10;

  GutteringConfiguration conf;

  run_test(nodes, num_updates, data_workers, CACHETREE, conf, nthreads);
}

TEST(CacheGutteringTest, RelabellingOffset) {
  const int nodes = 1024;
  const int relabelling_offset = 1024;
  const int num_updates = 5000000;
  const int data_workers = 4;
  const int nthreads = 10;

  GutteringConfiguration conf;

  // ==== shameless copy of run_test ====
  // helper function to run a basic test of the buffer tree with
  // various parameters
  // this test only works if the depth of the tree does not exceed 1
  // and no work is claimed off of the work queue
  // to work correctly num_updates must be a multiple of nodes
  auto gts = new CacheGuttering(nodes, data_workers, nthreads, conf);
  gts->set_offset(relabelling_offset);
  printf("Running Test: system=%s, nodes=%i, num_updates=%i\n",
    "CacheGuttering", nodes, num_updates);

  shutdown = false;
  upd_processed = 0;

  // In case there are multiple threads
  std::vector<std::thread> threads;
  threads.reserve(nthreads);
  std::vector<std::vector<update_t>> recorded_insertions(nthreads, std::vector<update_t>());
  std::vector<std::vector<update_t>> retrieved_insertions(data_workers, std::vector<update_t>());
  // This is the work to do per thread (rounded up)
  const int work_per = (num_updates+nthreads-1) / nthreads;

  auto task = [&](const int j){
    for (int i = j * work_per; i < (j+1) * work_per && i < num_updates; i++) {
      update_t upd;
      upd.first = i % nodes + relabelling_offset;
      upd.second = (nodes - 1) - (i % nodes);
      gts->insert(upd, j);
      recorded_insertions[j].push_back(upd);
    }
  };
  auto query_task = [&](const int j) {
    WorkQueue::DataNode *data;
    while(true) {
      bool valid = gts->get_data(data);
      if (valid) {
        std::vector<update_batch> batches = data->get_batches();
        for (auto batch : batches) {
          node_id_t key = batch.node_idx;
          std::vector<node_id_t> upd_vec = batch.upd_vec;
          for (auto upd : upd_vec) {
            retrieved_insertions[j].push_back({key, upd});
          }
        }
        gts->get_data_callback(data);
      }
      else if(shutdown)
        return;
    }
  };

  std::thread query_threads[data_workers];
  for (int t = 0; t < data_workers; t++) {
    query_threads[t] = std::thread(query_task, t);
  }

  // Spin up then join threads
  for (int j = 0; j < nthreads; j++)
    threads.emplace_back(task, j);
  for (int j = 0; j < nthreads; j++)
    threads[j].join();


  printf("force flush\n");
  gts->force_flush();
  shutdown = true;
  gts->set_non_block(true); // switch to non-blocking calls in an effort to exit
  
  for (int t = 0; t < data_workers; t++)
    query_threads[t].join();

  std::vector<update_t> catted_recorded;
  std::vector<update_t> catted_retrieved;
  catted_recorded.reserve(num_updates);
  catted_retrieved.reserve(num_updates);
  for (auto vec : recorded_insertions) {
    catted_recorded.insert(catted_recorded.end(), vec.begin(), vec.end());
  }
  for (auto vec : retrieved_insertions) {
    catted_retrieved.insert(catted_retrieved.end(), vec.begin(), vec.end());
  }
  std::sort(catted_recorded.begin(), catted_recorded.end());
  std::sort(catted_retrieved.begin(), catted_retrieved.end());

  ASSERT_EQ(catted_recorded, catted_retrieved);

  delete gts;
}