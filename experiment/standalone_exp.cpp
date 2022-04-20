#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <fstream>
#include "../include/standalone_gutters.h"

static bool shutdown = false;
static constexpr uint32_t prime = 100000007;

// queries the guttering system
// Should be run in a seperate thread
void querier(GutteringSystem *gts) {
  WorkQueue::DataNode *data;
  while(true) {
    bool valid = gts->get_data(data);
    if(!valid && shutdown)
      return;
    gts->get_data_callback(data);
  }
}

void write_configuration(int queue_factor, int gutter_factor) {
  std::ofstream conf("./buffering.conf");
  conf << "queue_factor=" << queue_factor << std::endl;
  conf << "gutter_factor=" << gutter_factor << std::endl;
}

void run_randomized(const int nodes, const unsigned long updates, const unsigned int nthreads=1) {
  shutdown = false;
  write_configuration(2, 1); // 2 is queue_factor, 1 is gutter_factor
  StandAloneGutters *gutters = new StandAloneGutters(nodes, 40, nthreads); // 40 is num workers

  // create queriers
  std::thread query_threads[40];
  for (int t = 0; t < 40; t++) {
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
  for (unsigned int j = 0; j < nthreads; j++)
    threads.emplace_back(task, j);
  for (unsigned int j = 0; j < nthreads; j++)
    threads[j].join();

  gutters->force_flush();
  shutdown = true;
  gutters->set_non_block(true); // switch to non-blocking calls in an effort to exit
  
  std::chrono::duration<double> delta = std::chrono::steady_clock::now() - start;
  printf("Insertions took %f seconds: average rate = %f\n", delta.count(), updates/delta.count());

  for (int t = 0; t < 40; t++)
    query_threads[t].join();
  delete gutters;
}

void run_test(const int nodes, const unsigned long updates, const unsigned int nthreads=1) {
  shutdown = false;
  write_configuration(2, 1); // 2 is queue_factor, 1 is gutter_factor
  StandAloneGutters *gutters = new StandAloneGutters(nodes, 40, nthreads); // 40 is num workers

  // create queriers
  std::thread query_threads[40];
  for (int t = 0; t < 40; t++) {
    query_threads[t] = std::thread(querier, gutters);
  }

  std::vector<std::thread> threads;
  threads.reserve(nthreads);
  const unsigned long work_per = (updates + (nthreads-1)) / nthreads;
  printf("work per thread: %lu\n", work_per);

  auto task = [&](const unsigned int j){
    for (unsigned long i = j * work_per; i < (j+1) * work_per && i < updates; i++) 
    {
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
  for (unsigned int j = 0; j < nthreads; j++)
    threads.emplace_back(task, j);
  for (unsigned int j = 0; j < nthreads; j++)
    threads[j].join();

  gutters->force_flush();
  shutdown = true;
  gutters->set_non_block(true); // switch to non-blocking calls in an effort to exit
  
  std::chrono::duration<double> delta = std::chrono::steady_clock::now() - start;
  printf("Insertions took %f seconds: average rate = %f\n", delta.count(), updates/delta.count());

  for (int t = 0; t < 40; t++)
    query_threads[t].join();
  delete gutters;
}

TEST(SA_Throughput, kron15_10threads) {
  run_test(32768, 280025434, 10);
}
TEST(SA_Throughput, kron15_20threads) {
  run_test(32768, 280025434, 20);
}

TEST(SA_Throughput, kron17_10threads) {
  run_test(131072, 4474931789, 10);
}
TEST(SA_Throughput, kron17_20threads) {
  run_test(131072, 4474931789, 20);
}

TEST(SA_Throughput, kron18_10threads) {
  run_test(262144, 17891985703, 10);
}
TEST(SA_Throughput, kron18_20threads) {
  run_test(262144, 17891985703, 20);
}

TEST(SA_Throughput_Rand, kron15_10threads) {
  run_randomized(32768, 280025434, 10);
}
TEST(SA_Throughput_Rand, kron15_20threads) {
  run_randomized(32768, 280025434, 20);
}

TEST(SA_Throughput_Rand, kron17_10threads) {
  run_randomized(131072, 4474931789, 10);
}
TEST(SA_Throughput_Rand, kron17_20threads) {
  run_randomized(131072, 4474931789, 20);
}

TEST(SA_Throughput_Rand, kron18_10threads) {
  run_randomized(262144, 17891985703, 10);
}
TEST(SA_Throughput_Rand, kron18_20threads) {
  run_randomized(262144, 17891985703, 20);
}
