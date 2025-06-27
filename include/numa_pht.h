
#include "guttering_system.h"
#include "pht.h"
/**
 * The idea of NumaPHT is to limit the amount of tree space that each thread touches without
 * giving up on performance. If updates are uniformly distributed then this is easy, simply divide
 * the gutter space among the threads evenly and allow each to operate independently.
 * 
 * However, if updates are not distributed uniformly, or arrive in a non-uniform order, then this
 * strategy will reduce performance as only some of the threads will be performing significant
 * work.
 * 
 * TODO: Solve this problem at some point I guess
 */
class NumaPHT : public PipelineHyperTree {
 private:
  const node_id_t num_gutters;

  const size_t num_inserters;
  const size_t num_trees;
  const size_t tree_bits;
  const size_t vert_bits;

  constexpr static size_t thread_buffer_size = 2048;

  struct ThreadBuffer {
    size_t size = 0;
    size_t capacity;
    update_t *buffer = nullptr;

    ThreadBuffer() : capacity(thread_buffer_size), buffer(new update_t[capacity]) {}
    ThreadBuffer(ThreadBuffer&& oth) : size(oth.size), capacity(oth.capacity), buffer(oth.buffer) {
      oth.size = 0;
      oth.capacity = 0;
      oth.buffer = nullptr;
    }
    ThreadBuffer& operator=(ThreadBuffer&& oth) {
      size = oth.size;
      capacity = oth.capacity;
      buffer = oth.buffer;

      oth.size = 0;
      oth.capacity = 0;
      oth.buffer = nullptr;

      return *this;
    }

    ~ThreadBuffer() { if (buffer != nullptr) delete[] buffer; }
  };

  // thread buffers for its roots and updates it wants to route to other trees
  ThreadBuffer *thread_buffers;

  WorkQueue<ThreadBuffer> **tree_queues;
 public:
  /**
   * NumaPHT constructor
   * 
   * @param num_gutters     number of gutters to construct
   * @param num_consumers   number of threads that will be removing leaf gutters from the tree
   * @param num_inserters   number of threads that will be performing updates
   * @param num_trees       number of independent trees to divide the gutters into
   */
  NumaPHT(node_id_t num_gutters, size_t num_consumers, size_t num_inserters, size_t num_trees,
          GutteringConfiguration conf);

  ~NumaPHT();

  insert_ret_t insert(const update_t &upd, size_t thr_id);

  insert_ret_t batch_insert(const update_t *batch, size_t num_updates, size_t thr_id);

  insert_ret_t process_stream_upd_batch(const GraphStreamUpdate *batch, size_t num_updates,
                                        size_t thr_id);

  insert_ret_t force_flush();

  insert_ret_t insert(const update_t &upd) {
    insert(upd, 0);
  }

  thread_local static size_t thr_insert_num;
};
