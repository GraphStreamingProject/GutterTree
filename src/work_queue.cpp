#include "../include/work_queue.h"
#include "../include/types.h"

#include <string.h>
#include <chrono>
#include <cassert>

WorkQueue::WorkQueue(size_t total_batches, size_t batch_size, size_t bpe) : 
 len(total_batches / bpe + total_batches % bpe), max_batch_size(batch_size), batch_per_elm(bpe) {
  non_block = false;

  // place all nodes of linked list in the producer queue and reserve
  // memory for the vectors
  for (size_t i = 0; i < len; i++) {
    DataNode *node = new DataNode(batch_per_elm, max_batch_size); // create and reserve space for updates
    node->next = producer_list; // next of node is head
    producer_list = node; // set head to new node
  }

  printf("WQ: created work queue with %lu elements, each of %lu batch(es), containing %lu updates\n", len, batch_per_elm, max_batch_size);
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

void WorkQueue::push(std::vector<update_batch> &upd_vec_batch) {
  if (upd_vec_batch.size() > batch_per_elm) {
    throw WriteTooBig("WQ: Too many batches in call to push " + 
      std::to_string(upd_vec_batch.size()) + " > " + std::to_string(batch_per_elm));
  }
  for (auto &batch : upd_vec_batch) {
    // ensure the write size is valid
    std::vector<node_id_t> &upd_vec = batch.upd_vec;
    if(upd_vec.size() > max_batch_size) {
      throw WriteTooBig("WQ: Batch is too big " + std::to_string(upd_vec.size()) 
        + " > " + std::to_string(max_batch_size));
    }
  }
  std::unique_lock<std::mutex> lk(producer_list_lock);
  producer_condition.wait(lk, [this]{return !full();});

  // printf("WQ: Push:\n");
  // print();

  // remove head from produce_list
  DataNode *node = producer_list;
  producer_list = producer_list->next;
  lk.unlock();

  // swap the batch vectors to perform the update
  std::swap(node->batches, upd_vec_batch);

  // add this block to the consumer queue for processing
  consumer_list_lock.lock();
  node->next = consumer_list;
  consumer_list = node;
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
  lk.unlock();

  data = node;
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
