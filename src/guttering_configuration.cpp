#include <iostream>
#include <string>
#include <unistd.h> //sysconf

#include "../include/guttering_configuration.h"

GutteringConfiguration::GutteringConfiguration() {
  _page_size = sysconf(_SC_PAGE_SIZE); // works on POSIX systems (alternative is boost)
  // Windows may need https://docs.microsoft.com/en-us/windows/win32/api/sysinfoapi/nf-sysinfoapi-getnativesysteminfo?redirectedfrom=MSDN
}

// setters
GutteringConfiguration& GutteringConfiguration::page_factor(int page_factor) {
  if (page_factor > 50 || page_factor < 1) {
    printf("WARNING: page_factor out of bounds [1,50] using default(1)\n");
    page_factor = 1;
  }
  _page_size = page_factor * sysconf(_SC_PAGE_SIZE);
  return *this;
}

GutteringConfiguration& GutteringConfiguration::buffer_exp(int buffer_exp) {
  if (buffer_exp > 30 || buffer_exp < 10) {
    printf("WARNING: buffer_exp out of bounds [10,30] using default(20)\n");
    buffer_exp = 20;
  }
  _buffer_size = 1 << buffer_exp;
  return *this;
}

GutteringConfiguration& GutteringConfiguration::fanout(uint32_t fanout) {
  _fanout = fanout;
  if (_fanout > 2048 || _fanout < 2) {
    printf("WARNING: fanout out of bounds [2,2048] using default(64)\n");
    _fanout = 64;
  }
  return *this;
}

GutteringConfiguration& GutteringConfiguration::queue_factor(uint32_t queue_factor) {
  _queue_factor = queue_factor;
  if (_queue_factor > 1024 || _queue_factor < 1) {
    printf("WARNING: queue_factor out of bounds [1,1024] using default(8)\n");
    _queue_factor = 2;
  }
  return *this;
}

GutteringConfiguration& GutteringConfiguration::num_flushers(uint32_t num_flushers) {
  _num_flushers = num_flushers;
  if (_num_flushers > 20 || _num_flushers < 1) {
    printf("WARNING: num_flushers out of bounds [1,20] using default(1)\n");
    _num_flushers = 1;
  }
  return *this;
}

GutteringConfiguration& GutteringConfiguration::gutter_factor(float gutter_factor) {
  _gutter_factor = gutter_factor;
  if (_gutter_factor < 1 && _gutter_factor > -1) {
    printf("WARNING: gutter_factor must be outside of range -1 < x < 1 using default(1)\n");
    _gutter_factor = 1;
  }
  if (_gutter_factor < 0)
    _gutter_factor = 1 / (-1 * _gutter_factor); // gutter factor reduces size if negative

  return *this;
}

GutteringConfiguration& GutteringConfiguration::wq_batch_per_elm(size_t wq_batch_per_elm) {
  _wq_batch_per_elm = wq_batch_per_elm;
  return *this;
}

std::ostream& operator<<(std::ostream& out, const GutteringConfiguration& conf) {
  out << "GutteringSystem Configuration:" << std::endl;
  out << " Background threads = " << conf._num_flushers << std::endl;
  out << " Leaf gutter factor = " << conf._gutter_factor << std::endl;
  out << " WQ elements factor = " << conf._queue_factor << std::endl;
  out << " WQ batches per elm = " << conf._wq_batch_per_elm << std::endl;
  out << " GutterTree params:"    << std::endl;
  out << "  Write granularity = " << conf._page_size << std::endl;
  out << "  Buffer size       = " << conf._buffer_size << std::endl;
  out << "  Fanout            = " << conf._fanout;
  return out;
}
