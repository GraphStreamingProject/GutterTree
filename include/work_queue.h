#pragma once
#include <condition_variable>
#include <mutex>
#include <utility>
#include <atomic>
#include <vector>
#include "types.h"

struct update_batch {
  node_id_t node_idx;
  std::vector<node_id_t> upd_vec;
};

class WorkQueue {
 public:
  class DataNode {
   private:
    // LL next pointer
    DataNode *next = nullptr;
    std::vector<update_batch> batches;

    DataNode(const size_t batch_per_elm, const size_t vec_size) {
      batches.resize(batch_per_elm);
      for (size_t i = 0; i < batch_per_elm; i++) {
        batches[i].upd_vec.reserve(vec_size);
      }
    }
    friend class WorkQueue;
   public:
    const std::vector<update_batch>& get_batches() { return batches; }
  };

  /*
   * Construct a work queue
   * The number of elements in the queue is num_batches / batch_per_elm
   * As a consequence num_batches is rounded up to the nearest multiple of batch_per_elm
   * @param num_batches     the rough number of batches to have in the queue
   * @param max_batch_size  the maximum size of a batch
   * @param batch_per_elm   number of batches per queue element.
   */
  WorkQueue(size_t num_batches, size_t max_batch_size, size_t batch_per_elm);
  ~WorkQueue();

  /* 
   * Add a data element to the queue
   * @param upd_vec_batch  vector of graph node id the associated updates
   */
  void push(std::vector<update_batch> &upd_vec_batch);

  /* 
   * Get data from the queue for processing
   * @param data   where to place the Data
   * @return  true if we were able to get good data, false otherwise
   */
  bool peek(DataNode *&data);

  /*
   * Wait until the work queue has enough items in it to satisfy the request and then
   * @param node_vec     where to place the batch of Data
   * @param batch_size   the amount of Data requested
   * return true if able to get good data, false otherwise
   */
  bool peek_batch(std::vector<DataNode *> &node_vec, size_t batch_size);
  
  /* 
   * After processing data taken from the work queue call this function
   * to mark the node as ready to be overwritten
   * @param data   the LL node that we have finished processing
   */
  void peek_callback(DataNode *data);

  /*
   * A batched version of peek_callback that avoids locking on every DataNode
   */
  void peek_batch_callback(const std::vector<DataNode *> &node_vec);

  void set_non_block(bool _block);

  /*
   * Function which prints the work queue
   * Used for debugging
   */
  void print();

  // functions for checking if the queue is empty or full
  inline bool full()    {return producer_list == nullptr;} // if producer queue empty, wq full
  inline bool empty()   {return consumer_list == nullptr;} // if consumer queue empty, wq empty

private:
  DataNode *producer_list = nullptr; // list of nodes ready to be written to
  DataNode *consumer_list = nullptr; // list of nodes with data for reading

  const size_t len;            // number of elments in queue
  const size_t max_batch_size; // maximum batch size
  const size_t batch_per_elm;  // number of batches per work queue element

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

class WriteTooBig : public std::exception {
private:
  const std::string message;

public:
  WriteTooBig(std::string message) : 
    message(message) {}

  virtual const char *what() const throw() {
    return message.c_str();
  }
};
