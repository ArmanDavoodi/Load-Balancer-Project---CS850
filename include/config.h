#ifndef CONFIG_H_H
#define CONFIG_H_H
#define PRINTER_LOCK // if defined, pauses the system for printing
#define PRINT_NODE_INFO // if defined, prints node info
#define PRINT_SHARD_INFO // if defined, prints shard info
#define PRINT_INPUT // if defined, prints input arguments
#define PRINT_SHARD_PER_NODE // if defined, prints shards owned by nodes
#define PRINT_UPDATE_INFO // if defined, prints update info whenever there is an update
#define DEBUG // if defined, does additional assetions and prints
#define ANALYZE // if defined, prints analysis info
#define PRINT_COLORED // if defined, prints colored output

#include "testlog.h"

inline static constexpr size_t write_buffer_size = 3000000; // size of the buffer
#endif
