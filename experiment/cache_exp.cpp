#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <fstream>
#include "../include/cache_guttering.h"

static bool shutdown = false;
static constexpr uint32_t prime = 100000007;

// queries the guttering system
// Should be run in a seperate thread
static void querier(GutteringSystem *gts) {
  WorkQueue::DataNode *data;
  while(true) {
    bool valid = gts->get_data(data);
    if(!valid && shutdown)
      return;
    gts->get_data_callback(data);
  }
}

static void run_randomized(const int nodes, const unsigned long updates, const unsigned int nthreads=1) {
  shutdown = false;
  size_t num_workers = 20;

  GutteringConfiguration conf(1, 20, 64, 8, 2, 1, 8);
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
      std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
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
  delete gutters;
}

static void run_test(const int nodes, const unsigned long updates, const unsigned int nthreads=1) {
  shutdown = false;
  size_t num_workers = 20;

  GutteringConfiguration conf(1, 20, 64, 8, 2, 1, 8);
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
      std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
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
  delete gutters;
}

TEST(CG_Throughput, kron15_10threads) {
  run_test(32768, 280025434, 10);
}
TEST(CG_Throughput, kron15_20threads) {
  run_test(32768, 280025434, 20);
}

TEST(CG_Throughput, kron17_10threads) {
  run_test(131072, 4474931789, 10);
}
TEST(CG_Throughput, kron17_20threads) {
  run_test(131072, 4474931789, 20);
}

TEST(CG_Throughput, EpsilonOver_kron17_10threads) {
  run_test(131073, 4474931789, 10);
}
TEST(CG_Throughput, EpsilonOver_kron17_20threads) {
  run_test(131073, 4474931789, 20);
}

TEST(CG_Throughput, kron18_10threads) {
  run_test(262144, 17891985703, 10);
}
TEST(CG_Throughput, kron18_20threads) {
  run_test(262144, 17891985703, 20);
}
TEST(CG_Throughput, kron18_24threads) {
  run_test(262144, 17891985703, 24);
}
TEST(CG_Throughput, kron18_48threads) {
  run_test(262144, 17891985703, 48);
}

TEST(CG_Throughput_Rand, kron15_10threads) {
  run_randomized(32768, 280025434, 10);
}
TEST(CG_Throughput_Rand, kron15_20threads) {
  run_randomized(32768, 280025434, 20);
}

TEST(CG_Throughput_Rand, kron17_10threads) {
  run_randomized(131072, 4474931789, 10);
}
TEST(CG_Throughput_Rand, kron17_20threads) {
  run_randomized(131072, 4474931789, 20);
}

TEST(CG_Throughput_Rand, EpsilonOver_kron17_10threads) {
  run_randomized(131073, 4474931789, 10);
}
TEST(CG_Throughput_Rand, EpsilonOver_kron17_20threads) {
  run_randomized(131073, 4474931789, 20);
}

TEST(CG_Throughput_Rand, kron18_10threads) {
  run_randomized(262144, 17891985703, 10);
}
TEST(CG_Throughput_Rand, kron18_20threads) {
  run_randomized(262144, 17891985703, 20);
}
TEST(CG_Throughput_Rand, kron18_24threads) {
  run_randomized(262144, 17891985703, 24);
}
TEST(CG_Throughput_Rand, kron18_48threads) {
  run_randomized(262144, 17891985703, 48);
}
