#include "../include/circular_queue.h"
#include "../include/update.h"
#include "../include/buffer_tree.h"

#include <string.h>
#include <chrono>

CircularQueue::CircularQueue(int num_elements, int size_of_elm): 
  len(num_elements), elm_size(size_of_elm) {
	head = 0;
	tail = 0;

	// malloc the memory for the circular queue
	queue_array = (queue_elm *) malloc(sizeof(queue_elm) * len);
	data_array = (char *) malloc(elm_size * len * sizeof(char));
	for (int i = 0; i < len; i++) {
		queue_array[i].data  = data_array + (elm_size * i);
		queue_array[i].dirty = false;
		queue_array[i].size  = 0;
	}

	printf("CQ: created circular queue with %i elements each of size %i\n", len, elm_size);
}

CircularQueue::~CircularQueue() {
	// free the queue
	for (int i = 0; i < len; i++) {
		free(queue_array[i].data);
	}
	free(queue_array);
}

void CircularQueue::push(char *elm, int size) {
	std::unique_lock<std::mutex> lk(write_lock);
	while(true) {
		// printf("CQ: push: wait on not-full. full() = %s\n", (full())? "true" : "false");
		cirq_full.wait(lk, [this]{return !full();});
		if(!full()) {
			// printf("CQ: push: got not-full");
			memcpy(queue_array[head].data, elm, size);
			queue_array[head].dirty = true;
			queue_array[head].size = size;
			head = incr(head);
			lk.unlock();
			// printf(" new head is %i\n", head);
			cirq_empty.notify_one();
			break;
		}
		lk.unlock();
	}
}

bool CircularQueue::peek(std::pair<int, queue_elm> &ret) {
	do {
		std::unique_lock<std::mutex> lk(read_lock);
		cirq_empty.wait(lk, [this]{return (!empty() || no_block);});
		if(!empty()) {
			// printf("CQ: peek: got non-empty");
			int temp = tail;
			tail = incr(tail);
			lk.unlock();
			// printf(" new tail is %i\n", tail);
			ret.first = temp;
			ret.second = queue_array[temp];
			return true;
		}
		lk.unlock();
	}while(!no_block);
	// printf("CQ: peek: EXITING without data due to no_block\n");
	return false;
}

void CircularQueue::pop(int i) {
	read_lock.lock();
	queue_array[i].dirty = false; // this data has been processed and this slot may now be overwritten
	cirq_full.notify_one();
	read_lock.unlock();
	// printf("CQ: pop: popped item %i\n", i);
}