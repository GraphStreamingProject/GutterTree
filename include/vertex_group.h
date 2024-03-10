#pragma once
#include "guttering_system.h"
#include <cassert>
#include <array>

struct SerializedGroupBuffer {
  // thing to update 
  std::vector<update_batch> batches;
  size_t size = 0;
};

template<size_t bits_per_entry, size_t max_entries> 
class PackedIntArray {
  using word_t = size_t;
  private:
    static constexpr size_t word_size = sizeof(word_t);
    static constexpr size_t bits_per_word = 8 * word_size;
    static constexpr size_t num_words = (bits_per_entry * max_entries + (bits_per_word-1)) / bits_per_word;
    std::array<word_t, num_words> data = {0}; 
    size_t _size = 0;
    // returns bits from [start_idx, end_idx)
    // TODO - rename to mention word in the name
    word_t get_bit_range(word_t word, uint8_t start_idx, uint8_t end_idx ) const {
      // std::cout << "get: start " << (size_t) start_idx << " end " << (size_t) end_idx << std::endl;
      assert(end_idx <= bits_per_word);
      // first, zero out any bits that are to the left of end_idx
      word_t mask = ~(~((word_t) 0) << (end_idx - start_idx));
      mask = mask << start_idx;

      // then, right-shift by start_idx to both truncate any bits outside of the range and to get the right value
      return (word & mask) >> start_idx;
    }


    // put the contents of to_put inside of word, shifted to start_index
    word_t set_bit_range(word_t word, word_t to_put, uint8_t start_idx, uint8_t end_idx) {
      //std::cerr << "set: start " << (size_t) start_idx << " end " << (size_t) end_idx << std::endl;
      // first, create a mask for the range [start_idx, end_idx)
      // this will be 1 ONLY in the bits that we'd like to set
      assert(start_idx <= end_idx);
      assert(end_idx <= bits_per_word);
      assert(to_put <= 1 << (end_idx - start_idx));
      to_put = to_put << start_idx;
      word_t mask = ~((~((word_t) 0)) << (end_idx-start_idx)); // zero out bits that are to the left of end_idx, including end_idx
      // zero out the bits up until start index
      // mask = mask & (~((word_t) 0) << start_idx);
      // right shift by start_idx to truncate 
      mask <<= start_idx;
      word = (word & ~mask) | (to_put & mask);
      return word;
    }
  
  public:
    node_id_t get(size_t idx) const {
      assert(idx < max_entries);

      size_t bit_position = idx * bits_per_entry;
      size_t word_idx = bit_position / bits_per_word;
      size_t left_offset = bit_position % bits_per_word;
      size_t right_num_bits = std::max(bits_per_word, left_offset + bits_per_entry) - bits_per_word;
      size_t left_num_bits = bits_per_entry - right_num_bits;

      node_id_t left_result = get_bit_range(
        data[word_idx], 
        left_offset, 
        std::min(left_offset + bits_per_entry, bits_per_word)
      );
      
      if ((word_idx >= num_words)) {
        //very infrequent branch
        return left_offset;
      }
      else {
        node_id_t right_result = get_bit_range(
          data[word_idx+1],
          0,
          right_num_bits
        );
        // NOTE - we will assume that the right_result contains the more significant bits!
        return (right_result << left_num_bits) | left_result;
      }
    };

    void set(size_t idx, word_t val) {
      assert(val <= (1 << bits_per_entry));
      assert(idx < max_entries);
      
      size_t bit_position = idx * bits_per_entry;
      size_t word_idx = bit_position / bits_per_word;
      size_t left_offset = bit_position % bits_per_word;

      size_t right_num_bits = std::max(bits_per_word, left_offset + bits_per_entry) - bits_per_word;
      size_t left_num_bits = bits_per_entry - right_num_bits;

      word_t left_put = get_bit_range(val, 0, left_num_bits);
      //clear the necessary left bits first:
      // data[word_idx] |= left_put;
      data[word_idx] = set_bit_range(
        data[word_idx], left_put, left_offset, 
        std::min(left_offset + bits_per_entry, bits_per_word)
      );

      if (word_idx >= num_words) {
        return;
      } 
      else {
        word_t right_put = get_bit_range(val, left_num_bits, bits_per_entry);
        data[word_idx+1] = set_bit_range(data[word_idx+1], right_put, 0, right_num_bits);
      }      
    };

    // TODO - these arent needed, probably.
    size_t size() {return size;};

    void put(node_id_t val) {
      set(_size++, val);
    }

    void clear() {
      _size = 0;
    }

    public:
      node_id_t operator [](size_t i) const {return get(i);}
};


class VertexGroupGutter {
  public:
    static constexpr size_t num_bits = 7;
    static constexpr size_t group_size = 1 << num_bits;
    static constexpr size_t buffer_size = 4096;  // number oftarget nodes the buffer should be able to store
  private:
    node_id_t start_node;
    size_t _size = 0;
    std::array<node_id_t, buffer_size> dests;  // target nodes
    PackedIntArray<num_bits, buffer_size> sources;
    // struct WQ_Buffer {
    //   // TODO - make this global
    //   std::vector<update_batch> batches;
    //   size_t size = 0;
    // };

  public:
    size_t size() {return _size;};
    bool full() {return _size >= buffer_size;};
    void set(update_t update, size_t idx) {
      assert(idx < buffer_size);
      node_id_t src = update.first;
      node_id_t dst = update.second;
      assert(src >= start_node && src <= start_node + buffer_size); // TODO - make this robust to overflow
      dests[idx] = dst;
      sources.set(idx, src - start_node);
    }
    update_t get(size_t idx) {
      node_id_t src = start_node + sources[idx];
      node_id_t dst = dests[idx];
      return {src, dst};
    }
    void put(update_t update) {
      set(update, _size++);
    }
    void reset(node_id_t new_start_node) {
      clear();
      start_node = new_start_node;
    }

    void clear() {
      _size = 0;
    }
    SerializedGroupBuffer serialize() {
      // TODO - update the interface of how this is used
      // TODO - these can probably be global
      std::vector<node_id_t> sorted_targets(buffer_size);
      std::array<size_t, group_size> targets_per_source;
      std::array<size_t, group_size> start_idxs;
      std::array<size_t, group_size> current_idxs; 
      // TODO - can probably do this on a single array
      for (size_t i=0; i < size(); i++) {
        targets_per_source[sources[i]]++;
      }
      size_t running_sum = 0;
      for (size_t src_idx=0; src_idx < group_size; src_idx++) {
        start_idxs[src_idx] = running_sum;
        current_idxs[src_idx] = running_sum;
        running_sum += targets_per_source[src_idx];
      }
      for (size_t i=0; i < size(); i++) {
        auto source = sources[i];
        auto target = dests[i];
        sorted_targets[current_idxs[source]++] = target;
      }
      // TODO - DO THIS WITHOUT NEW MEMORY ALLOCATIONS
      std::vector<update_batch> updates(VertexGroupGutter::group_size);
      // doing it this way for portability reasons
      size_t updated_srcs=0;
      for (size_t src_idx=0; src_idx < group_size; src_idx++) {
        node_id_t source_full_id = start_node + src_idx;
        // edge case - can be zero, meaning current and start
        // weren't even set properly
        size_t size = current_idxs[src_idx]-start_idxs[src_idx];
        if (size != 0) {
          updated_srcs++;
          std::vector<node_id_t>::const_iterator start = sorted_targets.begin() + start_idxs[src_idx];
          std::vector<node_id_t>::const_iterator end = sorted_targets.begin() + current_idxs[src_idx];
          std::vector<node_id_t> targets(start, end);
          updates.push_back({source_full_id, targets });
        }
      }
      return {updates, 0};
    } 
};