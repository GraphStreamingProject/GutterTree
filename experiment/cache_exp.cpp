#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <fstream>
#include "../include/cache_guttering.h"

static bool shutdown = false;
static constexpr uint32_t prime = 100000007;
static std::atomic<size_t> num_updates_processed;

// queries the guttering system
// Should be run in a seperate thread
static void querier(GutteringSystem *gts) {
  WorkQueue::DataNode *data;
  while(true) {
    bool valid = gts->get_data(data);
    if (valid) {
      size_t updates = 0;
      for (auto batch : data->get_batches())
        updates += batch.upd_vec.size();
      num_updates_processed += updates;
    }
    else if(shutdown)
      return;
    gts->get_data_callback(data);
  }
}

static void run_randomized(const int nodes, const unsigned long updates, const unsigned int nthreads=1) {
  num_updates_processed = 0;
  shutdown = false;
  size_t num_workers  = 20;
  size_t page_factor  = 1;
  size_t buffer_exp   = 20;
  size_t fanout       = 64;
  size_t queue_factor = 8;
  size_t num_flushers = 2;
  float gutter_factor = 1;
  size_t wq_batch     = 8;

  auto conf = GutteringConfiguration()
              .page_factor(page_factor)
              .buffer_exp(buffer_exp)
              .fanout(fanout)
              .queue_factor(queue_factor)
              .num_flushers(num_flushers)
              .gutter_factor(gutter_factor)
              .wq_batch_per_elm(wq_batch);

  CacheGuttering *gutters = new CacheGuttering(nodes, num_workers, nthreads, conf);

  // create queriers
  std::thread query_threads[num_workers];
  for (size_t t = 0; t < num_workers; t++) {
    query_threads[t] = std::thread(querier, gutters);
  }

  std::vector<std::thread> threads;
  threads.reserve(nthreads);
  const unsigned long work_per = (updates + (nthreads-1)) / nthreads;
  printf("work per thread: %lu\n", work_per);

  auto task = [&](const unsigned int j){
    for (unsigned long i = j * work_per; i < (j+1) * work_per && i < updates; i++) {
      if(i % 1000000000 == 0)
        printf("processed so far: %lu\n", i);
      update_t upd;
      upd.first = (i * prime) % nodes;
      upd.second = (nodes - 1) - upd.first;
      gutters->insert(upd, j);
      std::swap(upd.first, upd.second);
      gutters->insert(upd, j);
    }
  };

  auto start = std::chrono::steady_clock::now();
  //Spin up then join threads
  for (unsigned int j = 0; j < nthreads; j++) {
    threads.emplace_back(task, j);
#ifdef LINUX_FALLOCATE
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(j, &cpuset);
    int rc = pthread_setaffinity_np(threads[j].native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      std::cerr << "Error calling pthread_setaffinity_np for thread " << j << ": " << rc << "\n";
    }
#endif
  }
  for (unsigned int j = 0; j < nthreads; j++)
    threads[j].join();

  gutters->force_flush();
  shutdown = true;
  gutters->set_non_block(true); // switch to non-blocking calls in an effort to exit

  std::chrono::duration<double> delta = std::chrono::steady_clock::now() - start;
  printf("Insertions took %f seconds: average rate = %f\n", delta.count(), updates/delta.count());

  for (size_t t = 0; t < num_workers; t++)
    query_threads[t].join();

  std::cout << "Number of updates processed according to queriers = " << num_updates_processed << std::endl;

  delete gutters;
}

static void run_test(const int nodes, const unsigned long updates, const unsigned int nthreads=1) {
  num_updates_processed = 0;
  shutdown = false;
  size_t num_workers = 20;

  auto conf = GutteringConfiguration()
              .page_factor(1)
              .buffer_exp(20)
              .fanout(64)
              .queue_factor(8)
              .num_flushers(2)
              .gutter_factor(1)
              .wq_batch_per_elm(8);
  CacheGuttering *gutters = new CacheGuttering(nodes, num_workers, nthreads, conf);

  // create queriers
  std::thread query_threads[num_workers];
  for (size_t t = 0; t < num_workers; t++) {
    query_threads[t] = std::thread(querier, gutters);
  }

  std::vector<std::thread> threads;
  threads.reserve(nthreads);
  const unsigned long work_per = (updates + (nthreads-1)) / nthreads;
  printf("work per thread: %lu\n", work_per);

  auto task = [&](const unsigned int j){
    for (unsigned long i = j * work_per; i < (j+1) * work_per && i < updates; i++) {
      if(i % 1000000000 == 0)
        printf("processed so far: %lu\n", i);
      update_t upd;
      upd.first = i % nodes;
      upd.second = (nodes - 1) - (i % nodes);
      gutters->insert(upd, j);
      std::swap(upd.first, upd.second);
      gutters->insert(upd, j);
    }
  };

  auto start = std::chrono::steady_clock::now();
  //Spin up then join threads
  for (unsigned int j = 0; j < nthreads; j++) {
    threads.emplace_back(task, j);
#ifdef LINUX_FALLOCATE
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(j, &cpuset);
    int rc = pthread_setaffinity_np(threads[j].native_handle(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      std::cerr << "Error calling pthread_setaffinity_np for thread " << j << ": " << rc << "\n";
    }
#endif
  }
  for (unsigned int j = 0; j < nthreads; j++)
    threads[j].join();

  gutters->force_flush();
  shutdown = true;
  gutters->set_non_block(true); // switch to non-blocking calls in an effort to exit
  
  std::chrono::duration<double> delta = std::chrono::steady_clock::now() - start;
  printf("Insertions took %f seconds: average rate = %f\n", delta.count(), updates/delta.count());

  for (size_t t = 0; t < num_workers; t++)
    query_threads[t].join();

  std::cout << "Number of updates processed according to queriers = " << num_updates_processed << std::endl;
  delete gutters;
}

TEST(CG_Throughput, kron15_10threads) {
  run_test(32768, 280025434, 10);
  ASSERT_EQ(num_updates_processed, 280025434 * 2);
}
TEST(CG_Throughput, kron15_20threads) {
  run_test(32768, 280025434, 20);
  ASSERT_EQ(num_updates_processed, 280025434 * 2);
}

TEST(CG_Throughput, kron17_10threads) {
  run_test(131072, 4474931789, 10);
  ASSERT_EQ(num_updates_processed, 4474931789 * 2);
}
TEST(CG_Throughput, kron17_20threads) {
  run_test(131072, 4474931789, 20);
  ASSERT_EQ(num_updates_processed, 4474931789 * 2);
}

TEST(CG_Throughput, EpsilonOver_kron17_10threads) {
  run_test(131073, 4474931789, 10);
  ASSERT_EQ(num_updates_processed, 4474931789 * 2);
}
TEST(CG_Throughput, EpsilonOver_kron17_20threads) {
  run_test(131073, 4474931789, 20);
  ASSERT_EQ(num_updates_processed, 4474931789 * 2);
}

TEST(CG_Throughput, kron18_10threads) {
  run_test(262144, 17891985703, 10);
  ASSERT_EQ(num_updates_processed, 17891985703 * 2);
}
TEST(CG_Throughput, kron18_20threads) {
  run_test(262144, 17891985703, 20);
  ASSERT_EQ(num_updates_processed, 17891985703 * 2);
}
TEST(CG_Throughput, kron18_24threads) {
  run_test(262144, 17891985703, 24);
  ASSERT_EQ(num_updates_processed, 17891985703 * 2);
}
TEST(CG_Throughput, kron18_48threads) {
  run_test(262144, 17891985703, 48);
  ASSERT_EQ(num_updates_processed, 17891985703 * 2);
}

TEST(CG_Throughput_Rand, kron15_10threads) {
  run_randomized(32768, 280025434, 10);
  ASSERT_EQ(num_updates_processed, 280025434 * 2);
}
TEST(CG_Throughput_Rand, kron15_20threads) {
  run_randomized(32768, 280025434, 20);
  ASSERT_EQ(num_updates_processed, 280025434 * 2);
}

TEST(CG_Throughput_Rand, kron17_10threads) {
  run_randomized(131072, 4474931789, 10);
  ASSERT_EQ(num_updates_processed, 4474931789 * 2);
}
TEST(CG_Throughput_Rand, kron17_20threads) {
  run_randomized(131072, 4474931789, 20);
  ASSERT_EQ(num_updates_processed, 4474931789 * 2);
}

TEST(CG_Throughput_Rand, EpsilonOver_kron17_10threads) {
  run_randomized(131073, 4474931789, 10);
  ASSERT_EQ(num_updates_processed, 4474931789 * 2);
}
TEST(CG_Throughput_Rand, EpsilonOver_kron17_20threads) {
  run_randomized(131073, 4474931789, 20);
  ASSERT_EQ(num_updates_processed, 4474931789 * 2);
}

TEST(CG_Throughput_Rand, kron18_10threads) {
  run_randomized(262144, 17891985703, 10);
  ASSERT_EQ(num_updates_processed, 17891985703 * 2);
}
TEST(CG_Throughput_Rand, kron18_20threads) {
  run_randomized(262144, 17891985703, 20);
  ASSERT_EQ(num_updates_processed, 17891985703 * 2);
}
TEST(CG_Throughput_Rand, kron18_24threads) {
  run_randomized(262144, 17891985703, 24);
  ASSERT_EQ(num_updates_processed, 17891985703 * 2);
}
TEST(CG_Throughput_Rand, kron18_48threads) {
  run_randomized(262144, 17891985703, 48);
  ASSERT_EQ(num_updates_processed, 17891985703 * 2);
}
