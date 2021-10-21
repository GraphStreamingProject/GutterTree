#include "../include/buffer_tree.h"

#include <utility>
#include <unistd.h> //sysconf
#include <string.h> //memcpy
#include <stdexcept> // to throw exception on Windows TODO: remove
#include <fcntl.h>  //posix_fallocate
#include <errno.h>
#include <fstream>

/*
 * Static "global" BufferTree variables
 */
uint32_t BufferTree::num_nodes;
uint32_t BufferTree::page_size;
uint8_t  BufferTree::max_level;
uint32_t BufferTree::buffer_size;
uint32_t BufferTree::fanout;
uint64_t BufferTree::backing_EOF;
uint64_t BufferTree::leaf_size;
uint32_t BufferTree::queue_factor;
int      BufferTree::backing_store;
char *   BufferTree::cache;

/*
 * Constructor
 * Sets up the buffer tree given the storage directory, buffer size, number of children
 * and the number of nodes we will insert(N)
 * We assume that node indices begin at 0 and increase to N-1
 */
BufferTree::BufferTree(std::string dir, Node
nodes, int workers, bool reset=false) : dir(dir) {
	int file_flags = O_RDWR | O_CREAT;
	if (reset) {
		file_flags |= O_TRUNC;
	}
	num_nodes = nodes;
	configure_tree(); // read the configuration file

	page_size = (page_size % serial_update_size == 0)? page_size : page_size + serial_update_size - page_size % serial_update_size;
	if (buffer_size < page_size) {
		printf("WARNING: requested buffer size smaller than page_size. Set to page_size.\n");
		buffer_size = page_size;
	}
	
	// setup static variables
	num_nodes       = nodes;
	max_level       = ceil(log(num_nodes) / log(fanout));
	backing_EOF     = 0;
	leaf_size       = floor(24 * pow(log2(num_nodes), 3)); // size of leaf proportional to size of sketch
	leaf_size       = (leaf_size % serial_update_size == 0)? leaf_size : leaf_size + serial_update_size - leaf_size % serial_update_size;

	// malloc the memory for the root node
	root_node = (char *) malloc(buffer_size);
	root_position = 0;

	// malloc the memory used when flushing
	flush_buffers   = (char ***) malloc(sizeof(char **) * max_level);
	flush_positions = (char ***) malloc(sizeof(char **) * max_level);
	read_buffers    = (char **)  malloc(sizeof(char *)  * max_level);
	for (int l = 0; l < max_level; l++) {
		flush_buffers[l]   = (char **) malloc(sizeof(char *) * fanout);
		flush_positions[l] = (char **) malloc(sizeof(char *) * fanout);
		if(l != 0) read_buffers[l] = (char *) malloc(sizeof(char) * (buffer_size + page_size));
		for (uint32_t i = 0; i < fanout; i++) {
			flush_buffers[l][i] = (char *) calloc(page_size, sizeof(char));
		}
	}
	cache = (char *) malloc(fanout * ((uint64_t)buffer_size + page_size));

	// open the file which will be our backing store for the non-root nodes
	// create it if it does not already exist
	std::string file_name = dir + "buffer_tree_v0.2.data";
	printf("opening file %s\n", file_name.c_str());
	backing_store = open(file_name.c_str(), file_flags, S_IRUSR | S_IWUSR);
	if (backing_store == -1) {
		fprintf(stderr, "Failed to open backing storage file! error=%s\n", strerror(errno));
		exit(1);
	}

	setup_tree(); // setup the buffer tree

	// create the work queue in which we will place ripe fruit (full leaves)
	wq = new WorkQueue(queue_factor*workers, leaf_size + page_size);
	
	// will want to use mmap instead? - how much is in RAM after allocation (none?)
	// can't use mmap instead might use it as well. (Still need to create the file to be a given size)
	// will want to use pwrite/read so that the IOs are thread safe and all threads can share a single file descriptor
	// if we don't use mmap that is

	// printf("Successfully created buffer tree\n");
}

BufferTree::~BufferTree() {
	printf("Closing BufferTree\n");
	wq->get_stats();
	// force_flush(); // flush everything to leaves (could just flush to files in higher levels)

	// free malloc'd memory
	for(int l = 0; l < max_level; l++) {
		free(flush_positions[l]);
		if (l != 0) free(read_buffers[l]);
		for (uint16_t i = 0; i < fanout; i++) {
			free(flush_buffers[l][i]);
		}
		free(flush_buffers[l]);
	}
	free(flush_buffers);
	free(flush_positions);
	free(read_buffers);
	free(cache);
	free(root_node);
	for(uint32_t i = 0; i < buffers.size(); i++) {
		if (buffers[i] != nullptr)
			delete buffers[i];
	}
	delete wq;
	close(backing_store);
}

// Read the configuration file to determine a variety of BufferTree params
void BufferTree::configure_tree() {
	uint32_t buffer_exp  = 20;
	uint16_t branch      = 64;
	int queue_f          = 2;
	int page_factor      = 1;

	std::string line;
	std::ifstream conf("./buffering.conf");
	if (conf.is_open()) {
		while(getline(conf, line)) {
			if (line[0] == '#' || line[0] == '\n') continue;
			if(line.substr(0, line.find('=')) == "buffer_exp") {
				buffer_exp  = std::stoi(line.substr(line.find('=') + 1));
				if (buffer_exp > 30 || buffer_exp < 10) {
					printf("WARNING: buffer_exp out of bounds [10,30] using default(20)\n");
					buffer_exp = 20;
				}
			}
			if(line.substr(0, line.find('=')) == "branch") {
				branch = std::stoi(line.substr(line.find('=') + 1));
				if (branch > 2048 || branch < 2) {
					printf("WARNING: branch out of bounds [2,2048] using default(64)\n");
					branch = 64;
				}
			}
			if(line.substr(0, line.find('=')) == "queue_factor") {
				queue_f = std::stoi(line.substr(line.find('=') + 1));
				if (queue_f > 16 || queue_f < 1) {
					printf("WARNING: queue_factor out of bounds [1,16] using default(2)\n");
					queue_f = 2;
				}
			}
			if(line.substr(0, line.find('=')) == "page_factor") {
				page_factor = std::stoi(line.substr(line.find('=') + 1));
				if (page_factor > 50 || page_factor < 1) {
					printf("WARNING: page_factor out of bounds [1,50] using default(1)\n");
					page_factor = 1;
				}
			}
		}
	} else {
		printf("WARNING: Could not open buffering configuration file! Using default setttings.\n");
	}
	buffer_size = 1 << buffer_exp;
	fanout = branch;
	queue_factor = queue_f;
	page_size = page_factor * sysconf(_SC_PAGE_SIZE); // works on POSIX systems (alternative is boost)
}


void print_tree(std::vector<BufferControlBlock *>bcb_list) {
	for(uint32_t i = 0; i < bcb_list.size(); i++) {
		if (bcb_list[i] != nullptr) 
			bcb_list[i]->print();
	}
}

// TODO: clean up this function
void BufferTree::setup_tree() {
	printf("Creating a tree of depth %i\n", max_level);
	File_Pointer size = 0;

	// create the BufferControlBlocks
	for (uint32_t l = 1; l <= max_level; l++) { // loop through all levels
		if(l == 2) size = 0; // reset the size because the first level is held in cache

		uint32_t level_size    = pow(fanout, l); // number of blocks in this level
		uint32_t plevel_size   = pow(fanout, l-1);
		uint32_t start         = buffers.size();
		node_id_t key          = 0;
		double parent_keys     = num_nodes;
		uint32_t options       = fanout;
		bool skip          = false;
		uint32_t parent        = 0;
		File_Pointer index = 0;

		buffers.reserve(start + level_size);
		for (uint32_t i = 0; i < level_size; i++) { // loop through all blocks in the level
			// get the parent of this node if not level 1 and if we have a new parent
			if (l > 1 && (i-start) % fanout == 0) {
				parent      = start + i/fanout - plevel_size; // this logic should check out because only the last level is ever not full
				parent_keys = buffers[parent]->max_key - buffers[parent]->min_key + 1;
				key         = buffers[parent]->min_key;
				options     = fanout;
				skip        = (parent_keys == 1)? true : false; // if parent leaf then skip
			}
			if (skip || parent_keys == 0) {
				continue;
			}

			BufferControlBlock *bcb = new BufferControlBlock(start + index, size, l);
	 		bcb->min_key     = key;
	 		key              += ceil(parent_keys/options);
			bcb->max_key     = key - 1;

			if (l != 1)
				buffers[parent]->add_child(start + index);
			
			parent_keys -= ceil(parent_keys/options);
			options--;
			buffers.push_back(bcb);
			index++; // seperate variable because sometimes we skip stuff
			if(bcb->is_leaf())
				size += leaf_size + page_size;
			else 
				size += buffer_size + page_size;
		}
	}

	// allocate file space for all the nodes to prevent fragmentation
	#ifdef LINUX_FALLOCATE
	fallocate(backing_store, 0, 0, size); // linux only but fast
  #endif
  #ifdef WINDOWS_FILEAPI
  // https://stackoverflow.com/questions/455297/creating-big-file-on-windows/455302#455302
  // implement the above
  throw std::runtime_error("Windows is not currently supported by FastBufferTree");
  #endif
  #ifdef POSIX_FCNTL
    // taken from https://github.com/trbs/fallocate
    fstore_t store_options = {
        F_ALLOCATECONTIG,
        F_PEOFPOSMODE,
        0,
        static_cast<off_t>(size),
        0
    };
    fcntl(backing_store, F_PREALLOCATE, &store_options);
  #endif
	backing_EOF = size;
	// print_tree(buffers);
}

// serialize an update to a data location (should only be used for root I think)
inline void BufferTree::serialize_update(char *dst, update_t src) {
	node_id_t node1 = src.first;
	node_id_t node2 = src.second;

	memcpy(dst, &node1, sizeof(node_id_t));
	memcpy(dst + sizeof(node_id_t), &node2, sizeof(node_id_t));
}

inline update_t BufferTree::deserialize_update(char *src) {
	update_t dst;
	memcpy(&dst.first, src, sizeof(node_id_t));
	memcpy(&dst.second, src + sizeof(node_id_t), sizeof(node_id_t));

	return dst;
}

// copy two serailized updates between two locations
inline void BufferTree::copy_serial(char *src, char *dst) {
	memcpy(dst, src, serial_update_size);
}

/*
 * Load a key from a given location
 */
inline node_id_t BufferTree::load_key(char *location) {
	node_id_t key;
	memcpy(&key, location, sizeof(node_id_t));
	return key;
}

/*
 * Perform an insertion to the buffer-tree
 * Insertions always go to the root
 */
insert_ret_t BufferTree::insert(update_t upd) {
	// printf("inserting to buffer tree . . . ");
	// root_lock.lock();
	if (root_position + serial_update_size > buffer_size) {
		flush_root();
	}

	serialize_update(root_node + root_position, upd);
	root_position += serial_update_size;
	// root_lock.unlock();
	// printf("done insert\n");
}

/*
 * Helper function which determines which child we should flush to
 */
inline uint32_t which_child(node_id_t key, node_id_t min_key, node_id_t max_key, uint16_t options) {
	node_id_t total = max_key - min_key + 1;
	double div = (double)total / options;

	uint32_t larger_kids = total % options;
	uint32_t larger_count = larger_kids * ceil(div);
	node_id_t idx = key - min_key;

	if (idx >= larger_count)
		return ((idx - larger_count) / (int)div) + larger_kids;
	else
		return idx / ceil(div);
}

/*
 * Function for perfoming a flush anywhere in the tree agnostic to position.
 * this function should perform correctly so long as the parameters are correct.
 *
 * IMPORTANT: after perfoming the flush it is the caller's responsibility to reset
 * the number of elements in the buffer and associated pointers.
 *
 * IMPORTANT: Unless we add more flush_buffers only a single flush at each level may occur 
 * at once otherwise the data will clash
 */
flush_ret_t BufferTree::do_flush(char *data, uint32_t data_size, uint32_t begin, 
	node_id_t min_key, node_id_t max_key, uint16_t options, uint8_t level) {
	// setup
	uint32_t full_flush = page_size - (page_size % serial_update_size);

	char **flush_pos = flush_positions[level];
	char **flush_buf = flush_buffers[level];

	char *data_start = data;
	for (uint32_t i = 0; i < fanout; i++) {
		flush_pos[i] = flush_buf[i];
	}

	while (data - data_start < data_size) {
		node_id_t key = load_key(data);
		uint32_t child  = which_child(key, min_key, max_key, options);
		if (child > fanout - 1) {
			printf("ERROR: incorrect child %u abandoning insert key=%u min=%u max=%u\n", child, key, min_key, max_key);
			printf("first child = %u\n", buffers[begin]->get_id());
			printf("data pointer = %lu data_start=%lu data_size=%u\n", (uint64_t) data, (uint64_t) data_start, data_size);
			throw KeyIncorrectError();
		}
		if (buffers[child+begin]->min_key > key || buffers[child+begin]->max_key < key) {
			printf("ERROR: bad key %u for child %u, child min = %u, max = %u\n", 
				key, child, buffers[child+begin]->min_key, buffers[child+begin]->max_key);
			throw KeyIncorrectError();
		}
 
		copy_serial(data, flush_pos[child]);
		flush_pos[child] += serial_update_size;

		if (flush_pos[child] - flush_buf[child] >= full_flush) {
			// write to our child, return value indicates if it needs to be flushed
			uint32_t size = flush_pos[child] - flush_buf[child];
			if(buffers[begin+child]->write(flush_buf[child], size)) {
				flush_control_block(buffers[begin+child]);
			}

			flush_pos[child] = flush_buf[child]; // reset the flush_position
		}
		data += serial_update_size; // go to next thing to flush
	}

	// loop through the flush buffers and write out any non-empty ones
	for (uint32_t i = 0; i < fanout; i++) {
		if (flush_pos[i] - flush_buf[i] > 0) {
			// write to child i, return value indicates if it needs to be flushed
			uint32_t size = flush_pos[i] - flush_buf[i];
			if(buffers[begin+i]->write(flush_buf[i], size)) {
				flush_control_block(buffers[begin+i]);
			}
		}
	}
}

flush_ret_t inline BufferTree::flush_root() {
	// printf("Flushing root\n");
	// root_lock.lock(); // TODO - we can probably reduce this locking to only the last page
	do_flush(root_node, root_position, 0, 0, num_nodes-1, fanout, 0);
	root_position = 0;
	// root_lock.unlock();
}

flush_ret_t inline BufferTree::flush_control_block(BufferControlBlock *bcb) {
	// printf("flushing "); bcb->print();
	if(bcb->size() == 0) {
		return; // don't flush empty control blocks
	}

	if (bcb->is_leaf()) {
		return flush_leaf_node(bcb);
	}
	return flush_internal_node(bcb);
}

flush_ret_t inline BufferTree::flush_internal_node(BufferControlBlock *bcb) {
	// flushing a control block is the only time read_buffers are used
	// and we call this on the bottom level of the tree (max_level) so
	// level-1 for the read_buffers is important.
	uint8_t level = bcb->level;
	if (level == 1) { // we have this in cache
		read_buffers[level-1] = cache + bcb->offset();
	} else {
		uint32_t data_to_read = bcb->size();
		uint32_t offset = 0;
		while(data_to_read > 0) {
			int len = pread(backing_store, read_buffers[level-1] + offset, data_to_read, bcb->offset() + offset);
			if (len == -1) {
				printf("ERROR flush failed to read from buffer %i, %s\n", bcb->get_id(), strerror(errno));
				exit(EXIT_FAILURE);
			}
			data_to_read -= len;
			offset += len;
		}
	}

	// printf("read %lu bytes\n", len);

	do_flush(read_buffers[level-1], bcb->size(), bcb->first_child, bcb->min_key, bcb->max_key, bcb->children_num, bcb->level);
	bcb->reset();
}

flush_ret_t inline BufferTree::flush_leaf_node(BufferControlBlock *bcb) { 
	uint8_t level = bcb->level;
	if (level == 1) {
		read_buffers[level-1] = cache + bcb->offset();
	} else {
		uint32_t data_to_read = bcb->size();
		uint32_t offset = 0;
		while(data_to_read > 0) {
			int len = pread(backing_store, read_buffers[level-1] + offset, data_to_read, bcb->offset() + offset);
			if (len == -1) {
				printf("ERROR flush failed to read from buffer %i, %s\n", bcb->get_id(), strerror(errno));
				exit(EXIT_FAILURE);
			}
			data_to_read -= len;
			offset += len;
		}
	}
	wq->push(read_buffers[level-1], bcb->size());
	bcb->reset();
}

// ask the buffer tree for data
// this function may sleep until data is available
bool BufferTree::get_data(data_ret_t &data) {
	File_Pointer idx = 0;

	// make a request to the circular buffer for data
	std::pair<int, queue_elm> queue_data;
	bool got_data = wq->peek(queue_data);

	if (!got_data)
		return false; // we got no data so return not valid

	int i         = queue_data.first;
	queue_elm elm = queue_data.second;
	char *serial_data = elm.data;
	uint32_t len      = elm.size;

	if (len == 0) {
		wq->pop(i);
		return false; // we got no data so return not valid
        }

	data.second.clear(); // remove any old data from the vector
	uint32_t vec_len  = len / serial_update_size;
	data.second.reserve(vec_len); // reserve space for our updates

	// assume the first key is correct so extract it
	node_id_t key = load_key(serial_data);
	data.first = key;

	while(idx < (uint64_t) len) {
		update_t upd = deserialize_update(serial_data + idx);
		// printf("got update: %lu %lu\n", upd.first, upd.second);
		if (upd.first == 0 && upd.second == 0) {
			break; // got a null entry so done
		}

		if (upd.first != key) {
			// error to handle some weird unlikely buffer tree shenanigans
			printf("ERROR: source node %u and key %u do not match in get_data()\n", upd.first, key);
			throw KeyIncorrectError();
		}

		// printf("query to node %lu got edge to node %lu\n", key, upd.second);
		data.second.push_back(upd.second);
		idx += serial_update_size;
	}

	wq->pop(i); // mark the wq entry as clean
	return true;
}

flush_ret_t BufferTree::force_flush() {
	// printf("Force flush\n");
	flush_root();
	// loop through each of the bufferControlBlocks and flush it
	// looping from 0 on should force a top to bottom flush (if we do this right)
	
	for (BufferControlBlock *bcb : buffers) {
		if (bcb != nullptr) {
			flush_control_block(bcb);
		}
	}
}

void BufferTree::set_non_block(bool block) {
	if (block) {
		wq->no_block = true; // circular queue operations should no longer block
		wq->cirq_empty.notify_all();
		wq->cirq_full.notify_all();
	}
	else {
		wq->no_block = false; // set circular queue to block if necessary
	}
}
