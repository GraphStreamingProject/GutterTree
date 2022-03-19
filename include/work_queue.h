#pragma once
#include <condition_variable>
#include <mutex>
#include <utility>
#include <atomic>
#include <vector>
#include "types.h"

class WorkQueue {
 public:
  class DataNode {
   private:
    // LL next pointer
    DataNode *next = nullptr;
    node_id_t node_idx = 0;
    std::vector<node_id_t> *data_vec;

    DataNode(const size_t vec_size) {
      data_vec = new std::vector<node_id_t>();
      data_vec->reserve(vec_size);
    }

    friend class WorkQueue;
   public:
    node_id_t get_node_idx() { return node_idx; }
    std::vector<node_id_t> get_data_vec() { return *data_vec; }
  };

  /*
   * Construct a work queue
   * @param num_elements  the number of queue slots
   * @param max_elm_size  the maximum size of a data element
   */
  WorkQueue(int num_elements, int max_elm_size);
  ~WorkQueue();

  /* 
   * Add a data element to the queue
   * @param data    reference to the data to be placed into the queue
   * @param size    the number items in the vector
   */
  void push(node_id_t node_idx, std::vector<node_id_t> *&data_vec);

  /* 
   * Get data from the queue for processing
   * @param data   where to place the Data
   * @return  true if we were able to get good data, false otherwise
   */
  bool peek(DataNode *&data);

  /*
   * Wait until the work queue has enough items in it to satisfy the request and then
   * return batch_size number of work units
   */
  bool peek_batch(std::vector<DataNode *> &data_vec, int batch_size);
  
  /* 
   * After processing data taken from the work queue call this function
   * to mark the node as ready to be overwritten
   * @param data   the LL node that we have finished processing
   */
  void peek_callback(DataNode *data);

  void set_non_block(bool _block);

  /*
   * Function which prints the work queue
   * Used for debugging
   */
  void print();

  // functions for checking if the queue is empty or full
  inline bool full()    {return producer_list == nullptr;} // if producer queue empty, wq full
  inline bool empty()   {return consumer_list == nullptr;} // if consumer queue empty, wq empty
  inline int get_size() {return q_size;}

private:
  DataNode *producer_list = nullptr; // list of nodes ready to be written to
  DataNode *consumer_list = nullptr; // list of nodes with data for reading

  const int len;
  const int max_elm_size;
  std::atomic<int> q_size;

  // locks and condition variables for producer list
  std::condition_variable producer_condition;
  std::mutex producer_list_lock;

  // locks and condition variables for consumer list
  std::condition_variable consumer_condition;
  std::mutex consumer_list_lock;
  size_t consumer_list_size; // size of consumer list for peek_batch

  // should WorkQueue peeks wait until they can succeed(false)
  // or return false on failure (true)
  bool non_block;
};

class WriteTooBig : public std::exception {
private:
  const int elm_size;
  const int max_size;

public:
  WriteTooBig(int elm_size, int max_size) : elm_size(elm_size), max_size(max_size) {}

  virtual const char *what() const throw() {
    return ("WQ: Write is too big " + std::to_string(elm_size) + " > " + 
      std::to_string(max_size)).c_str();
  }
};
