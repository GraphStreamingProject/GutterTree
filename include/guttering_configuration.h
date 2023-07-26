#pragma once
#include <unistd.h>
#include <iostream>
#include <string>

// forward declaration
class GutteringSystem;

class GutteringConfiguration {
private:
  // write granularity
  size_t _page_size = uninit_param;
  
  // size of an internal node buffer
  size_t _buffer_size = uninit_param;
  
  // maximum number of children per node
  size_t _fanout = uninit_param;
  
  // total number of batches in queue is this factor * num_workers
  size_t _queue_factor = uninit_param;
  
  // the number of flush threads
  size_t _num_flushers = uninit_param;
  
  // the size of each leaf gutter in bytes
  size_t _gutter_bytes = uninit_param;
  
  // number of batches placed into or removed from the queue in one push or peek operation
  size_t _wq_batch_per_elm = uninit_param;

  friend class GutteringSystem;

public:
  GutteringConfiguration() = default;
  GutteringConfiguration& set_defaults();
  
  // setters
  GutteringConfiguration& page_factor(size_t page_factor);
  GutteringConfiguration& buffer_exp(size_t buffer_exp);
  GutteringConfiguration& fanout(size_t fanout);
  GutteringConfiguration& queue_factor(size_t queue_factor);
  GutteringConfiguration& num_flushers(size_t num_flushers);
  GutteringConfiguration& gutter_bytes(size_t gutter_bytes);
  GutteringConfiguration& wq_batch_per_elm(size_t wq_batch_per_elm);

  // getters
  size_t get_page_size()        { return _page_size; }
  size_t get_buffer_size()      { return _buffer_size; }
  size_t get_fanout()           { return _fanout; }
  size_t get_queue_factor()     { return _queue_factor; }
  size_t get_num_flushers()     { return _num_flushers; }
  size_t get_gutter_bytes()     { return _gutter_bytes; }
  size_t get_wq_batch_per_elm() { return _wq_batch_per_elm; }

  friend std::ostream& operator<<(std::ostream& out, GutteringConfiguration conf);

  // no use of equal operator
  GutteringConfiguration &operator=(const GutteringConfiguration &) = delete;

  // moving and copying allowed
  GutteringConfiguration(const GutteringConfiguration &) = default;
  GutteringConfiguration (GutteringConfiguration &&) = default;

  static constexpr size_t uninit_param = (size_t) -1;
};
