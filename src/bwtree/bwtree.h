
/*
 * bwtree.h - This file implements the BwTree
 */

#pragma once
#ifndef _BWTREE_H
#define _BWTREE_H

#include "common.h"
#include <atomic>

namespace wangziqi2013 {
namespace index_building_block {

/*
 * class DefaultMappingTable - This class implements the minimal mapping table
 *                             which supports the allocation and CAS of node IDs
 * 
 * 1. Release of node ID is not supported. Always allocate from the counter
 * 2. The mapping table is fixed sized. No bounds checking is performed under
 *    release mode. Under debug mode an error will be raised
 * 
 * It accepts two template parameters: One to specify the element type. The atomic
 * type to the pointer of the element type is stored. Another to specify the 
 * size of the mapping table, which is the number of elements
 */
template <typename BaseNodeType, size_t TABLE_SIZE>
class DefaultMappingTable {
 public:
  friend void MappingTableTest();

  // External class should def this
  using NodeIDType = uint64_t;

 private:
  /*
   * DefaultMappingTable() - Constructor
   * 
   * The constructor is private to avoid allocating a mapping table on the stack
   * or directly putting it as a memory, as the table can be potentially large
   */
  DefaultMappingTable() : 
    next_slot{NodeIDType{0}} {
    return;
  }

 public: 

  /*
   * ~DefaultMappingTable() - Destructor
   */
  ~DefaultMappingTable() {}

  /*
   * Get() - Allocate an instance of the mapping table
   * 
   * Do not use new() to instanciate a mapping table.
   */
  static DefaultMappingTable *Get() {
    return new DefaultMappingTable{};
  }

  /*
   * Destroy() - The destructor of the mapping table instance
   * 
   * Do not use delete directly
   */
  static void Destroy(DefaultMappingTable *mapping_table_p) {
    delete mapping_table_p;
  }

  /*
   * AllocateNodeID() - Allocate a slot and put the given node_p into it
   * 
   * If allocation fails because of table overflow, an error is raised under 
   * debug mode. The slot is always returned even if node_p is given
   */
  inline NodeIDType AllocateNodeID(BaseNodeType *node_p) {
    // Use atomic instruction to allocate slots
    NodeIDType slot = next_slot.fetch_add(1);
    // Only do this after the atomic inc
    assert(slot < TABLE_SIZE);
    mapping_table[slot] = node_p;

    return slot;
  }

  /*
   * ReleaseNodeID() - Release the node ID
   * 
   * The minimal implementation does not support this. Just leak the node ID.
   * Subclasses could extend this class by overriding this function to provide
   * node ID release capabilities.
   */
  inline void ReleaseNodeID(NodeIDType node_id) {
    assert(node_id < TABLE_SIZE);
    (void)node_id; 
    return;
  }

  /*
   * CAS() - Performs compare and swap on a table element
   */
  inline bool CAS(NodeIDType node_id, 
                  BaseNodeType *old_value, 
                  BaseNodeType *new_value) {
    assert(node_id < TABLE_SIZE);
    return mapping_table[node_id].compare_exchange_strong(old_value, new_value);
  }

  /*
   * At() - Returns the content on a given index
   */
  inline BaseNodeType *At(NodeIDType node_id) {
    assert(node_id < TABLE_SIZE);
    return mapping_table[node_id].load();
  }

  /*
   * Reset() - Clear the content as well as the index
   */
  void Reset() {
    memset(mapping_table, 0x00, sizeof(mapping_table));
    next_slot = NodeIDType{0};
    return;
  }

 private:
  // Fixed sized mapping table with atomic type as elements
  std::atomic<BaseNodeType *> mapping_table[TABLE_SIZE];
  std::atomic<NodeIDType> next_slot;
};

/*
 * class DefaultDeltaChain - This class defines the storage of the delta chain
 */
class DefaultDeltaChain {
 public:
  /*
   * enum class NodeType - Defines the enum of node type
   */
  enum class NodeType : uint16_t {
    InnerBase = 0,
    InnerInsert,
    InnerDelete,
    InnerSplit,
    InnerRemove,
    InnerMerge,

    LeafBase,
    LeafInsert,
    LeafDelete,
    LeafSplit,
    LeafRemove,
    LeafMerge,
  };

  /*
   * DefaultDeltaChain() - Constructor
   */
  DefaultDeltaChain() {
    IF_DEBUG(mem_usage.store(0UL));
    return;
  }

  template<typename DeltaType, typename ...Args>
  inline void AllocateDelta() {
    IF_DEBUG(mem_usage.fetch_add(sizeof(DeltaType)));
    return new DeltaType{Args...};
  }

 private:
  IF_DEBUG(std::atomic<size_t> mem_usage);
};

/*
 * class DefaultNode - This class defines the way key and values are stored
 *                     in the base node
 * 
 * 1. Delta allocation is not defined
 * 2. Node consolidation is not defined
 */
template <KeyType, ValueType, DeltaChainType>
class DefaultNode {
 private:
  DeltaChainType delta_chain;
  KeyType low_key;
};

} // namespace index_building_block
} // namespace wangziqi2013

#endif