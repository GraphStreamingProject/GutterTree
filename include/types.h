#pragma once
#include <utility>
#include <vector>
typedef uint32_t node_id_t;
typedef uint64_t Node;
typedef std::pair<node_id_t, node_id_t> update_t;
typedef void insert_ret_t;
typedef void flush_ret_t;
typedef std::pair<Node, std::vector<Node>> data_ret_t;
