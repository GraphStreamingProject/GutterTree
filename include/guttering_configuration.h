#include <iostream>
#include <fstream>
#include <string>
#include <unistd.h> //sysconf
static constexpr char config_loc[] = "buffering.conf";

class GutteringConfiguration {
public:
  // write granularity
  uint32_t page_size = 8192;
  
  // size of an internal node buffer
  uint32_t buffer_size = 1 << 23;
  
  // maximum number of children per node
  uint32_t fanout = 64;
  
  // total number of batches in queue is this factor * num_workers
  uint32_t queue_factor = 8;
  
  // the number of flush threads
  uint32_t num_flushers = 2;
  
  // factor which increases/decreases the leaf gutter size
  float gutter_factor = 1;
  
  // number of batches placed into or removed from the queue in one push or peek operation
  size_t wq_batch_per_elm = 1;

  void print() const {
    std::cout << "GutteringSystem Configuration:" << std::endl;
    std::cout << " Background threads = " << num_flushers << std::endl;
    std::cout << " Leaf gutter factor = " << gutter_factor << std::endl;
    std::cout << " WQ elements factor = " << queue_factor << std::endl;
    std::cout << " WQ batches per elm = " << wq_batch_per_elm << std::endl;
    std::cout << " GutterTree params:"    << std::endl;
    std::cout << "  Write granularity = " << page_size << std::endl;
    std::cout << "  Buffer size       = " << buffer_size << std::endl;
    std::cout << "  Fanout            = " << fanout << std::endl;
  }

  // Constructor that sets values
  GutteringConfiguration(uint32_t page_factor, uint32_t buffer_exp, uint32_t fanout, 
    uint32_t queue_factor, uint32_t flush_threads, float gutter_f, size_t wq_batch_per_elm) : 
    page_size(page_factor * sysconf(_SC_PAGE_SIZE)), buffer_size(1 << buffer_exp), fanout(fanout), 
    queue_factor(queue_factor), num_flushers(flush_threads), gutter_factor(gutter_f), 
    wq_batch_per_elm(wq_batch_per_elm) {}

  // Constructor that reads from file
  GutteringConfiguration() {
    // parse the configuration file
    std::string line;
    std::ifstream conf(config_loc);
    if (conf.is_open()) {
      while(getline(conf, line)) {
        if (line[0] == '#' || line[0] == '\n') continue;
        else if(line.substr(0, line.find('=')) == "buffer_exp") {
          int buffer_exp  = std::stoi(line.substr(line.find('=') + 1));
          if (buffer_exp > 30 || buffer_exp < 10) {
            printf("WARNING: buffer_exp out of bounds [10,30] using default(20)\n");
            buffer_exp = 20;
          }
          buffer_size = 1 << buffer_exp;
        }
        else if(line.substr(0, line.find('=')) == "branch") {
          fanout = std::stoi(line.substr(line.find('=') + 1));
          if (fanout > 2048 || fanout < 2) {
            printf("WARNING: branch out of bounds [2,2048] using default(64)\n");
            fanout = 64;
          }
        }
        else if(line.substr(0, line.find('=')) == "queue_factor") {
          queue_factor = std::stoi(line.substr(line.find('=') + 1));
          if (queue_factor > 1024 || queue_factor < 1) {
            printf("WARNING: queue_factor out of bounds [1,1024] using default(8)\n");
            queue_factor = 2;
          }
        }
        else if(line.substr(0, line.find('=')) == "page_factor") {
          int page_factor = std::stoi(line.substr(line.find('=') + 1));
          if (page_factor > 50 || page_factor < 1) {
            printf("WARNING: page_factor out of bounds [1,50] using default(1)\n");
            page_factor = 1;
          }
          page_size = page_factor * sysconf(_SC_PAGE_SIZE); // works on POSIX systems (alternative is boost)
          // Windows may need https://docs.microsoft.com/en-us/windows/win32/api/sysinfoapi/nf-sysinfoapi-getnativesysteminfo?redirectedfrom=MSDN
        }
        else if(line.substr(0, line.find('=')) == "num_threads") {
          num_flushers = std::stoi(line.substr(line.find('=') + 1));
          if (num_flushers > 20 || num_flushers < 1) {
            printf("WARNING: num_threads out of bounds [1,20] using default(1)\n");
            num_flushers = 1;
          }
        }
        else if(line.substr(0, line.find('=')) == "gutter_factor") {
          gutter_factor = std::stof(line.substr(line.find('=') + 1));
          if (gutter_factor < 1 && gutter_factor > -1) {
            printf("WARNING: gutter_factor must be outside of range -1 < x < 1 using default(1)\n");
            gutter_factor = 1;
          }
          if (gutter_factor < 0)
            gutter_factor = 1 / (-1 * gutter_factor); // gutter factor reduces size if negative
        }
      }
    } else {
      printf("WARNING: Could not open buffering configuration file! Using default settings.\n");
    }
  }

  // no copying for you
  GutteringConfiguration(const GutteringConfiguration &) = delete;
  GutteringConfiguration &operator=(const GutteringConfiguration &) = delete;

  // moving is allowed
  GutteringConfiguration (GutteringConfiguration &&) = default;
};
