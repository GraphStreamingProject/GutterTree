#include "../include/guttering_configuration.h"

#include "types.h"

// set any uninitialized parameters to the default values
GutteringConfiguration& GutteringConfiguration::set_defaults() {
  if (_page_size == uninit_param)        _page_size        = sysconf(_SC_PAGE_SIZE);
  if (_buffer_size == uninit_param)      _buffer_size      = 1 << 23;
  if (_fanout == uninit_param)           _fanout           = 64;
  if (_queue_factor == uninit_param)     _queue_factor     = 8;
  if (_num_flushers == uninit_param)     _num_flushers     = 2;
  if (_gutter_bytes == uninit_param)     _gutter_bytes     = 32 * 1024;
  if (_wq_batch_per_elm == uninit_param) _wq_batch_per_elm = 1;

  return *this;
}

// setters
GutteringConfiguration& GutteringConfiguration::page_factor(size_t page_factor) {
  if (page_factor > 50 || page_factor < 1) {
    printf("WARNING: page_factor out of bounds [1,50] using default(1)\n");
    page_factor = 1;
  }
  // sysconf works on POSIX systems. Windows may need something else
  _page_size = page_factor * sysconf(_SC_PAGE_SIZE);
  return *this;
}

GutteringConfiguration& GutteringConfiguration::buffer_exp(size_t buffer_exp) {
  if (buffer_exp > 30 || buffer_exp < 10) {
    printf("WARNING: buffer_exp out of bounds [10,30] using default(20)\n");
    buffer_exp = 20;
  }
  _buffer_size = 1 << buffer_exp;
  return *this;
}

GutteringConfiguration& GutteringConfiguration::fanout(size_t fanout) {
  _fanout = fanout;
  if (_fanout > 2048 || _fanout < 2) {
    printf("WARNING: fanout out of bounds [2,2048] using default(64)\n");
    _fanout = 64;
  }
  return *this;
}

GutteringConfiguration& GutteringConfiguration::queue_factor(size_t queue_factor) {
  _queue_factor = queue_factor;
  if (_queue_factor > 1024 || _queue_factor < 1) {
    printf("WARNING: queue_factor out of bounds [1,1024] using default(8)\n");
    _queue_factor = 2;
  }
  return *this;
}

GutteringConfiguration& GutteringConfiguration::num_flushers(size_t num_flushers) {
  _num_flushers = num_flushers;
  if (_num_flushers > 20 || _num_flushers < 1) {
    printf("WARNING: num_flushers out of bounds [1,20] using default(1)\n");
    _num_flushers = 1;
  }
  return *this;
}

GutteringConfiguration& GutteringConfiguration::gutter_bytes(size_t gutter_bytes) {
  _gutter_bytes = gutter_bytes;
  if (_gutter_bytes < 1) {
    printf("WARNING: gutter_bytes must be at least 1, using default(32 KiB)\n");
    _gutter_bytes = 32 * 1024;
  }

  return *this;
}

GutteringConfiguration& GutteringConfiguration::wq_batch_per_elm(size_t wq_batch_per_elm) {
  _wq_batch_per_elm = wq_batch_per_elm;
  return *this;
}

std::ostream& operator<<(std::ostream& out, GutteringConfiguration conf) {
  conf.set_defaults();

  out << "GutteringSystem Configuration:" << std::endl;
  out << " Background threads = " << conf._num_flushers << std::endl;
  out << " Updates per batch  = " << conf._gutter_bytes / sizeof(node_id_t) << std::endl;
  out << " WQ elements factor = " << conf._queue_factor << std::endl;
  out << " WQ batches per elm = " << conf._wq_batch_per_elm << std::endl;
  out << " GutterTree params:"    << std::endl;
  out << "  Write granularity = " << conf._page_size << std::endl;
  out << "  Buffer size (KiB) = " << conf._buffer_size / 1024 << std::endl;
  out << "  Fanout            = " << conf._fanout;
  return out;
}
