#pragma once

#include <iostream>
#include <string>
#include <unistd.h> //sysconf

// forward declaration
class GutteringSystem;

class GutteringConfiguration {
private:
  // write granularity
  uint32_t _page_size = 8192;
  
  // size of an internal node buffer
  uint32_t _buffer_size = 1 << 23;
  
  // maximum number of children per node
  uint32_t _fanout = 64;
  
  // total number of batches in queue is this factor * num_workers
  uint32_t _queue_factor = 8;
  
  // the number of flush threads
  uint32_t _num_flushers = 2;
  
  // factor which increases/decreases the leaf gutter size
  float _gutter_factor = 1;
  
  // number of batches placed into or removed from the queue in one push or peek operation
  size_t _wq_batch_per_elm = 1;

  friend class GutteringSystem;

public:
  GutteringConfiguration() {
    _page_size = sysconf(_SC_PAGE_SIZE); // works on POSIX systems (alternative is boost)
    // Windows may need https://docs.microsoft.com/en-us/windows/win32/api/sysinfoapi/nf-sysinfoapi-getnativesysteminfo?redirectedfrom=MSDN
  }
  
  // setters
  auto& page_factor(int page_factor) {
    if (page_factor > 50 || page_factor < 1) {
      printf("WARNING: page_factor out of bounds [1,50] using default(1)\n");
      page_factor = 1;
    }
    _page_size = page_factor * sysconf(_SC_PAGE_SIZE);
    return *this;
  }
  
  auto& buffer_exp(int buffer_exp) {
    if (buffer_exp > 30 || buffer_exp < 10) {
      printf("WARNING: buffer_exp out of bounds [10,30] using default(20)\n");
      buffer_exp = 20;
    }
    _buffer_size = 1 << buffer_exp;
    return *this;
  }
  
  auto& fanout(uint32_t fanout) {
    _fanout = fanout;
    if (_fanout > 2048 || _fanout < 2) {
      printf("WARNING: fanout out of bounds [2,2048] using default(64)\n");
      _fanout = 64;
    }
    return *this;
  }
  
  auto& queue_factor(uint32_t queue_factor) {
    _queue_factor = queue_factor;
    if (_queue_factor > 1024 || _queue_factor < 1) {
      printf("WARNING: queue_factor out of bounds [1,1024] using default(8)\n");
      _queue_factor = 2;
    }
    return *this;
  }
  
  auto& num_flushers(uint32_t num_flushers) {
    _num_flushers = num_flushers;
    if (_num_flushers > 20 || _num_flushers < 1) {
      printf("WARNING: num_flushers out of bounds [1,20] using default(1)\n");
      _num_flushers = 1;
    }
    return *this;
  }
  
  auto& gutter_factor(float gutter_factor) {
    _gutter_factor = gutter_factor;
    if (_gutter_factor < 1 && _gutter_factor > -1) {
      printf("WARNING: gutter_factor must be outside of range -1 < x < 1 using default(1)\n");
      _gutter_factor = 1;
    }
    if (_gutter_factor < 0)
      _gutter_factor = 1 / (-1 * _gutter_factor); // gutter factor reduces size if negative

    return *this;
  }
  
  auto& wq_batch_per_elm(size_t wq_batch_per_elm) {
    _wq_batch_per_elm = wq_batch_per_elm;
    return *this;
  }

  void print() const {
    std::cout << "GutteringSystem Configuration:" << std::endl;
    std::cout << " Background threads = " << _num_flushers << std::endl;
    std::cout << " Leaf gutter factor = " << _gutter_factor << std::endl;
    std::cout << " WQ elements factor = " << _queue_factor << std::endl;
    std::cout << " WQ batches per elm = " << _wq_batch_per_elm << std::endl;
    std::cout << " GutterTree params:"    << std::endl;
    std::cout << "  Write granularity = " << _page_size << std::endl;
    std::cout << "  Buffer size       = " << _buffer_size << std::endl;
    std::cout << "  Fanout            = " << _fanout << std::endl;
  }

  // no use of equal operator
  GutteringConfiguration &operator=(const GutteringConfiguration &) = delete;

  // moving and copying allowed
  GutteringConfiguration(const GutteringConfiguration &) = default;
  GutteringConfiguration (GutteringConfiguration &&) = default;
};
