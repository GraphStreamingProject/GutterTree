#include "../include/work_queue.h"
#include "../include/types.h"

#include <string.h>
#include <chrono>

WorkQueue::WorkQueue(int num_elements, int size_of_elm): 
  len(num_elements), elm_size(size_of_elm) {
  head = 0;
  tail = 0;
  no_block = false;
  queue_size = 0;

  // malloc the memory for the work queue
  queue_array = (queue_elm *) malloc(sizeof(queue_elm) * len);
  data_array = (char *) malloc(elm_size * len * sizeof(char));
  for (int i = 0; i < len; i++) {
    queue_array[i].data    = data_array + (elm_size * i);
    queue_array[i].dirty   = false;
    queue_array[i].touched = false;
    queue_array[i].size    = 0;
  }

  printf("WQ: created work queue with %li elements each of size %li\n", len, elm_size);
}

WorkQueue::~WorkQueue() {
  // free the queue
  free(data_array);
  free(queue_array);
}

void WorkQueue::push(char *elm, int size) {
  if(size > elm_size) {
    printf("WQ: write of size %i bytes greater than max of %li\n", size, elm_size);
    throw WriteTooBig();
  }

  inserts++;
  if (empty())
    empty_inserts++;

  while(true) {
    std::unique_lock<std::mutex> lk(write_lock);
    // printf("WQ: push: wait on not-full. full() = %s\n", (full())? "true" : "false");
    cirq_full.wait_for(lk, std::chrono::seconds(2), [this]{return !full();});
    if(!full()) {
      queue_size++;
      if (queue_size > max_queue_size)
        max_queue_size = queue_size;
      memcpy(queue_array[head].data, elm, size);
      queue_array[head].dirty = true;
      queue_array[head].size = size;
      head = incr(head);
      lk.unlock();
      cirq_empty.notify_one();
      break;
    }
    lk.unlock();
  }
}

bool WorkQueue::peek(std::pair<int, queue_elm> &ret) {
  do {
    std::unique_lock<std::mutex> lk(read_lock);
    cirq_empty.wait_for(lk, std::chrono::seconds(2), [this]{return (!empty() || no_block);});
    if(!empty()) {
      int temp = tail;
      queue_array[tail].touched = true;
      tail = incr(tail);
      queue_size--;
      lk.unlock();

      ret.first = temp;
      ret.second = queue_array[temp];
      return true;
    }
    lk.unlock();
  }while(!no_block);
  return false;
}

void WorkQueue::pop(int i) {
  write_lock.lock();
  queue_array[i].dirty   = false; // this data has been processed and this slot may now be overwritten
  queue_array[i].touched = false; // may read this slot
  write_lock.unlock();
  cirq_full.notify_one();
}

void WorkQueue::print() {
  printf("WQ: head=%i, tail=%i, is_full=%s, is_empty=%s\n", 
    head, tail, full()? "true" : "false", empty()? "true" : "false");
}
