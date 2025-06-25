#pragma once
#include <condition_variable>
#include <mutex>
#include <utility>
#include <atomic>
#include <vector>
#include <exception>
#include "types.h"

/**
 * WorkQueue is templatized by data type we're storing.
 * This data-type must be: 1. default constructable, 2. able to use operator=
 * Ideally it should also have fast std::swap() performance (e.g. a std::vector just swaps
 * metadata/pointers)
 */
template<class T> 
class WorkQueue {
 public:
  class DataNode {
   private:
    // LL next pointer
    DataNode *next = nullptr;
    T data;

    friend class WorkQueue;
   public:
    const T& get_data() { return data; }
  };

  /**
   * Construct a work queue
   * @param num_queue_elements   the rough number of data elements to have in the queue
   */
  WorkQueue(size_t num_queue_elements)
      : len(num_queue_elements) {
    non_block = false;

    // place all nodes of linked list in the producer queue and reserve
    // memory for the vectors
    for (size_t i = 0; i < len; i++) {
      // create and reserve space for queue elements
      DataNode *node = new DataNode();
      node->next = producer_list;  // next of node is head
      producer_list = node;        // set head to new node
    }
  }
  ~WorkQueue() {
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

  /**
   * TODO: Rewrite this description
   * Initialize the queue pointers to point at actual data instead of nullptrs
   * If this function is called, IT MUST be called before performing any operations with the queue
   * The queue can also work without initializing pointers, so long as the pointers returned from
   * push being null is acceptable. (i.e. user initializes after push or does not need the returned
   * pointer)
   * @param new_data   a vector of data that will start in the queue but is swapped with
   *                       data that is pushed into the queue.
   */
  void populate_queue(const std::vector<T> &new_data) {
    if (new_data.size() != len) {
      throw std::invalid_argument("WQ: Error number of initialized data batches incorrect");
    }
    DataNode *data = producer_list; // head of producer list
    for (size_t i = 0; i < len; i++) {
      data->data = new_data[i];
      data = data->next;
    }
  }

  /**
   * Adds a data element to the queue
   * @param push_data   the data the user wants to add to the queue. When this function returns,
   *                    this reference will hold the data that was in the "empty" queue node it replaced
   */
  void push(T &push_data) {
    std::unique_lock<std::mutex> lk(producer_list_lock);
    producer_condition.wait(lk, [this]{return !full();});

    // printf("WQ: Push:\n");
    // print();

    // remove head from produce_list
    DataNode *node = producer_list;
    producer_list = producer_list->next;
    lk.unlock();

    // swap the batch vectors to perform the update
    std::swap(node->data, push_data);

    // add this block to the consumer queue for processing
    consumer_list_lock.lock();
    node->next = consumer_list;
    consumer_list = node;
    consumer_list_lock.unlock();
    consumer_condition.notify_one();
  }

  /**
   * Get data from the queue for processing
   * @param data   where to place the Data
   * @return  true if we were able to get good data, false otherwise
   */
  bool pop(DataNode *&data) {
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

  /**
   * After processing data taken from the work queue call this function
   * to mark the node as ready to be overwritten
   * @param data   the LL node that we have finished processing
   */
  void pop_callback(DataNode *node) {
    producer_list_lock.lock();
    // printf("WQ: Callback\n");
    // print();
    node->next = producer_list;
    producer_list = node;
    producer_list_lock.unlock();
    producer_condition.notify_one();
    // printf("WQ: Callback done\n");
  }

  void set_non_block(bool _block) {
    consumer_list_lock.lock();
    non_block = _block;
    consumer_list_lock.unlock();
    consumer_condition.notify_all();
  }

  /**
   * Function which prints the work queue
   * Used for debugging
   */
  void print() {
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

  // functions for checking if the queue is empty or full
  inline bool full()    {return producer_list == nullptr;} // if producer queue empty, wq full
  inline bool empty()   {return consumer_list == nullptr;} // if consumer queue empty, wq empty

private:
  DataNode *producer_list = nullptr; // list of nodes ready to be written to
  DataNode *consumer_list = nullptr; // list of nodes with data for reading

  const size_t len;            // number of elments in queue

  // locks and condition variables for producer list
  std::condition_variable producer_condition;
  std::mutex producer_list_lock;

  // locks and condition variables for consumer list
  std::condition_variable consumer_condition;
  std::mutex consumer_list_lock;

  // should WorkQueue peeks wait until they can succeed(false)
  // or return false on failure (true)
  bool non_block;
};
