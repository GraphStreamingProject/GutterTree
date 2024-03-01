#include <gtest/gtest.h>
#include <algorithm>
#include <thread>
#include <atomic>
#include <fstream>
#include <math.h>
#include <random>
#include "vertex_group.h"


static size_t get_time_nano()
{
  auto now = std::chrono::high_resolution_clock::now();
  return std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
}

std::mt19937 gen(6); // 6 chosen by fair dice roll

void set_seed(uint64_t seed)
{
  gen.seed(seed);
}

uint64_t get_rand(int a, int b)
{
  std::uniform_int_distribution dist(a, b-1);
  return dist(gen);
}



class VertexGroupTest : public testing::Test {};
// INSTANTIATE_TEST_SUITE_P(VertexGroupTestSuite, VertexGroupTest);

TEST(VertexGroupTest, PackedArrayStream) {
  // stream through the array and garauntee that all entries store the correct value
  auto seed = get_time_nano();
  set_seed(seed);
  std::cout << "seed: " << seed << std::endl;
  constexpr size_t num_elems = 3000;
  constexpr size_t bits_per_entry = 4;

  PackedIntArray<bits_per_entry, num_elems> packed_array;
  std::array<uint64_t, num_elems> true_values; 
  
  for (size_t i=0; i < num_elems; i++) {
    uint64_t rand_value = get_rand(0, 1 << bits_per_entry);

    std::cout << "num: " << rand_value <<std::endl;
    true_values[i] = rand_value;
    packed_array.set(i, rand_value);
  }

  for (size_t i=0; i < num_elems; i++) {
    size_t predicted = packed_array[i];
    size_t actual = true_values[i];
    std::cout << "ooga " << predicted << " booga " << actual << std::endl;
    ASSERT_EQ(true_values[i], packed_array[i]);
  }

}