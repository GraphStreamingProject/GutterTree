#include "../include/work_queue.h"
#include "../include/types.h"

#include <string.h>
#include <chrono>
#include <cassert>

WorkQueue::WorkQueue(int q_len, int max_elm_size) : len(q_len), max_elm_size(max_elm_size) {
  non_block = false;

  // place all nodes of linked list in the producer queue and reserve
  // memory for the vectors
  for (int i = 0; i < len; i++) {
    DataNode *node = new DataNode(max_elm_size); // create and reserve space for updates
    node->next = producer_list; // next of node is head
    producer_list = node; // set head to new node
  }
  consumer_list_size = 0;

  printf("WQ: created work queue with %i elements each of size %i\n", len, max_elm_size);
}

WorkQueue::~WorkQueue() {
  // free data from the queues
  // grab locks to ensure that list variables aren't old due to cpu caching
  producer_list_lock.lock();
  consumer_list_lock.lock();
  while (producer_list != nullptr) {
    DataNode *temp = producer_list;
    producer_list = producer_list->next;
    delete temp;
  }
  while (consumer_list != nullptr) {
    DataNode *temp = consumer_list;
    consumer_list = consumer_list->next;
    delete temp;
  }
  producer_list_lock.unlock();
  consumer_list_lock.unlock();
}

void WorkQueue::push(node_id_t node_idx, std::vector<node_id_t> &upd_vec) {
  if(upd_vec.size() > max_elm_size) {
    throw WriteTooBig(upd_vec.size(), max_elm_size);
  }

  std::unique_lock<std::mutex> lk(producer_list_lock);
  producer_condition.wait(lk, [this]{return !full();});

  // printf("WQ: Push:\n");
  // print();

  // remove head from produce_list
  DataNode *node = producer_list;
  producer_list = producer_list->next;
  lk.unlock();

  // set node id and set node's data vector equal to data_vec
  node->node_idx = node_idx;
  std::swap(node->data_vec, upd_vec);

  // add this block to the consumer queue for processing
  consumer_list_lock.lock();
  node->next = consumer_list;
  consumer_list = node;
  ++consumer_list_size;
  consumer_list_lock.unlock();
  consumer_condition.notify_one();
}

bool WorkQueue::peek(DataNode *&data) {
  // wait while queue is empty
  // printf("waiting to peek\n");
  std::unique_lock<std::mutex> lk(consumer_list_lock);
  consumer_condition.wait(lk, [this]{return !empty() || non_block;});

  // printf("WQ: Peek\n");
  // print();

  // if non_block and queue is empty then there is no data to get
  // so inform the caller of this
  if (empty()) {
    lk.unlock();
    return false;
  }

  // remove head from consumer_list and release lock
  DataNode *node = consumer_list;
  consumer_list = consumer_list->next;
  --consumer_list_size;
  lk.unlock();

  data = node;
  return true;
}

bool WorkQueue::peek_batch(std::vector<DataNode *> &node_vec, int batch_size) {
  assert(batch_size <= len); // cannot request a batch bigger than the work queue

  node_vec.clear(); // clear out any old data
  node_vec.reserve(batch_size);

  // wait until consumer queue is large enough
  std::unique_lock<std::mutex> lk(consumer_list_lock);
  consumer_condition.wait(lk, 
    [this, batch_size]{return consumer_list_size >= batch_size || non_block;});

  // printf("WQ: Peek-batch\n");
  // print();

  if (empty()) {
    lk.unlock();
    return false;
  }

  // pull data from head of consumer_list
  for(int i = 0; i < batch_size; i++) {
    if (consumer_list == nullptr) break; // if non_block is true may not be able to get full batch

    node_vec.push_back(consumer_list);
    consumer_list = consumer_list->next;
    --consumer_list_size;
  }
  lk.unlock();
  return true;
}

void WorkQueue::peek_callback(DataNode *node) {
  producer_list_lock.lock();
  // printf("WQ: Callback\n");
  // print();
  node->next = producer_list;
  producer_list = node;
  producer_list_lock.unlock();
  producer_condition.notify_one();
  // printf("WQ: Callback done\n");
}

void WorkQueue::set_non_block(bool _block) {
  consumer_list_lock.lock();
  non_block = _block;
  consumer_list_lock.unlock();
  consumer_condition.notify_all();
}

void WorkQueue::print() {
  std::string to_print = "";

  int p_size = 0;
  DataNode *temp = producer_list;
  while (temp != nullptr) {
    to_print += std::to_string(p_size) + ": " + std::to_string((uint64_t)temp) + "\n";
    temp = temp->next;
    ++p_size;
  }
  int c_size = 0;
  temp = consumer_list;
  while (temp != nullptr) {
    to_print += std::to_string(c_size) + ": " + std::to_string((uint64_t)temp) + "\n";
    temp = temp->next;
    ++c_size;
  }
  printf("WQ: producer_queue size = %i consumer_queue size = %i\n%s", p_size, c_size, to_print.c_str());
}
