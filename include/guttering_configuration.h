#pragma once

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
  GutteringConfiguration();
  
  // setters
  GutteringConfiguration& page_factor(int page_factor);
  
  GutteringConfiguration& buffer_exp(int buffer_exp);
  
  GutteringConfiguration& fanout(uint32_t fanout);
  
  GutteringConfiguration& queue_factor(uint32_t queue_factor);
  
  GutteringConfiguration& num_flushers(uint32_t num_flushers);
  
  GutteringConfiguration& gutter_factor(float gutter_factor);
  
  GutteringConfiguration& wq_batch_per_elm(size_t wq_batch_per_elm);

  friend std::ostream& operator<<(std::ostream& out, const GutteringConfiguration& dt);

  // no use of equal operator
  GutteringConfiguration &operator=(const GutteringConfiguration &) = delete;

  // moving and copying allowed
  GutteringConfiguration(const GutteringConfiguration &) = default;
  GutteringConfiguration (GutteringConfiguration &&) = default;
};
