
/*
 * bwtree.h - This file implements the BwTree
 */

#pragma once
#ifndef _BWTREE_H
#define _BWTREE_H

#include "common.h"
#include <atomic>

/*
 * class DefaultMappingTable - This class implements the minimal mapping table
 *                             which supports the allocation and CAS of node IDs
 * 
 * 1. Release of node ID is not supported. Always allocate from the counter
 * 2. The mapping table is fixed sized. No bounds checking is performed under
 *    release mode. Under debug mode an error will be raised
 */
template <typedef BaseType, uint64_t TABLE_SIZE>
class DefaultMappingTable {
 public:
  

 private:
  // Fixed sized mapping table with atomic type as elements
  atomic<BaseType> mapping_table[TABLE_SIZE];
  uint64_t next_slot;
};

} // namespace index_building_block
} // namespace wangziqi2013

#endif