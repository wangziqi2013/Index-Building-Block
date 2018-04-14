
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
namespace bwtree {

template <typename KeyType,
          typename ValueType,
          typename _KeyLess = std::less<KeyType>,
          typename _KeyEq = std::equal_to<KeyType>,
          typename _ValueEq = std::equal_to<ValueType>>
class DefaultOperator {
 public:
  _KeyEq _key_eq; 
  _KeyLess _key_less;
  _ValueEq _value_eq;
  
  // * KeyLess()
  inline bool KeyLess(const KeyType &k1, const KeyType &k2) 
  { return _key_kess(k1, k2); }
  // * KeyGreater()
  inline bool KeyGreater(const KeyType &k1, const KeyType &k2) 
  { return _key_kess(k2, k1); }
  // * KeyEq()
  inline bool KeyEq(const KeyType &k1, const KeyType &k2) 
  { return _key_eq(k1, k2); }
  // * KeyLessEq()
  inline bool KeyLessEq(const KeyType &k1, const KeyType &k2) 
  { return !KeyGreater(k1, k2); }
  // * KeyGreaterEq()
  inline bool KeyGreaterEq(const KeyType &k1, const KeyType &k2) 
  { return !KeyLess(k1, k2); }
  // * ValueEq()
  inline bool ValueEq(const ValueType &v1, const ValueType &v2) 
  { return _value_eq(v1, v2); }
};

/*
 * class KeyValuePair - Operators involving key comparison
 */
template <typename KeyType, typename ValueType>
class KeyValuePair {
 public:
  KeyType key;
  ValueType value;

  // Operators for checking magnitude with either a key, or another key 
  // value pair
  // * operator<
  inline bool operator<(const KeyValuePair &kvp) const { return key < kvp.key; }
  // * operator<
  inline bool operator<(const KeyType &k) const { return key < k; }
  // * operator>
  inline bool operator>(const KeyValuePair &kvp) const { return key < kvp.key; }
  // * operator>
  inline bool operator>(const KeyType &k) const { return key > k; }
  // * operator==
  inline bool operator==(const KeyValuePair &kvp) const { return key == kvp.key; }
  // * operator==
  inline bool operator==(const KeyType &k) const { return key == k; }
  // * operator>=
  inline bool operator>=(const KeyValuePair &kvp) const { return key >= kvp.key; }
  // * operator>=
  inline bool operator>=(const KeyType &k) const { return key >= k; }
  // * operator<=
  inline bool operator<=(const KeyValuePair &kvp) const { return key <= kvp.key; }
  // * operator<=
  inline bool operator<=(const KeyType &k) const { return key <= k; }
};

/*
  * enum class NodeType - Defines the enum of node type
  */
enum class NodeType : uint16_t {
  InnerBase = 1,
  InnerInsert,
  InnerDelete,
  InnerSplit,
  InnerRemove,
  InnerMerge,

  LeafBase = 10,
  LeafInsert,
  LeafDelete,
  LeafSplit,
  LeafRemove,
  LeafMerge,
};

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
   * DefaultMappingTable() - Private Constructor
   * 
   * The constructor is private to avoid allocating a mapping table on the stack
   * or directly putting it as a memory, as the table can be potentially large
   */
  DefaultMappingTable() : 
    next_slot{NodeIDType{0}} {
    return;
  }

  /*
   * ~DefaultMappingTable() - Private Destructor
   */
  ~DefaultMappingTable() {}

 public: 
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
   * DefaultDeltaChain() - Constructor
   */
  DefaultDeltaChain() {
    IF_DEBUG(mem_usage.store(0UL));
    return;
  }

  template<typename DeltaType, typename ...Args>
  inline void AllocateDelta(Args &&...args) {
    IF_DEBUG(mem_usage.fetch_add(sizeof(DeltaType)));
    return new DeltaType{args...};
  }

 private:
  IF_DEBUG(std::atomic<size_t> mem_usage);
};

using NodeSizeType = uint32_t;
using NodeHeightType = uint16_t;

/*
 * class NodeBase - Base class of base node and delta node types
 * 
 * Virtual node abstraction is defined in this class
 */
template <typename KeyType, typename ValueType>
class NodeBase {
 public:
  using KeyValuePairType = KeyValuePair<KeyType, ValueType>;

 protected:
  /*
   * NodeBase() - Constructor
   */
  NodeBase(NodeType ptype, NodeHeightType pheight, NodeSizeType psize,
           KeyValuePairType *plow_key_p, KeyValuePairType *phigh_key_p) :
    type{ptype}, height{pheight}, size{psize},
    low_key_p{plow_key_p}, high_key_p{phigh_key_p} {}

 public:
  // * GetSize() - Returns the size
  inline NodeSizeType GetSize() const { return size; }
  // * GetDepth() - Returns the depth
  inline NodeHeightType GetHeight() const { return height; }
  // * GetType() - Returns the type enum
  inline NodeType GetType() const { return type; }

  // * KeyInNode() - Return whether a given key is within the node's range
  inline bool KeyInNode(const KeyType &key) { 
    return *low_key_p <= key && \
           *high_key_p > key;
  }

  // * KeyLargerThanNode() - Return whether a given key is larger than
  //                         all keys in the node
  inline bool KeyLargerThanNode(const KeyType &key) {
    return *high_key_p <= key;
  }

  // * KeySmallerThanNode() - Returns whether the given key is smaller than
  //                          all keys in the node
  inline bool KeySmallerThanNode(const KeyType &key) {
    return *low_key_p > key;
  }

 private:
  // The following three are packed into a 64 bit integer
  NodeType type;
  // Height in the delta chain (0 means base node)
  NodeHeightType height;
  // Number of elements
  NodeSizeType size;
  KeyValuePairType *low_key_p;
  KeyValuePairType *high_key_p;
};

/*
 * class DefaultBaseNode - This class defines the way key and values are stored
 *                         in the base node
 * 
 * 1. Delta allocation is not defined
 * 2. Node consolidation is not defined
 * 3. Only unique key is supported; Non-unique key must be implemented
 *    outside the index
 */
template <typename KeyType, 
          typename ValueType, 
          typename DeltaChainType>
class DefaultBaseNode : NodeBase<KeyType, ValueType> {
 public:
  using BaseClassType = NodeBase<KeyType, ValueType>;
  using KeyValuePairType = typename BaseClassType::KeyValuePairType;
 private:
  /*
   * DefaultBaseNode() - Private Constructor
   */
  DefaultBaseNode(NodeType ptype, 
                  NodeHeightType pheight,
                  NodeSizeType psize,
                  const KeyValuePairType &phigh_key) :
    BaseClassType{ptype, pheight, psize, begin(), &high_key},
    delta_chain{} {
    return;
  } 
  
  /*
   * ~DefaultBaseNode() - Private Destructor
   */
  ~DefaultBaseNode() {}

 public:
  /*
   * Get() - Returns a base node with extended storage for key and value pairs
   * 
   * 1. The size of the node is determined at run time
   * 2. We allocate the sizeof() the class plus the extra storage for key and 
   *    values
   */
  static DefaultBaseNode *Get(NodeType ptype, 
                              NodeHeightType pheight,
                              NodeSizeType psize,
                              const KeyValuePairType &phigh_key) {
    // Size for key value pairs and size for the structure itself
    size_t extra_size = size_t{psize} * sizeof(KeyValuePairType);
    size_t total_size = extra_size + sizeof(DefaultBaseNode);

    void *p = new unsigned char[total_size];
    DefaultBaseNode *node_p = \
      static_cast<DefaultBaseNode *>(
        new (p) DefaultBaseNode{ptype, pheight, psize, phigh_key});
    
    return node_p;
  }

  /*
   * Destroy() - Frees the memory and calls destructor
   * 
   * 1. The delta chain's destructor will be called in this case. Make sure
   *    all delta chain elements have been destroyed before this is called
   */
  static void Destroy(DefaultBaseNode *node_p) {
    ~DefaultBaseNode(node_p);
    delete[] reinterpret_cast<unsigned char *>(node_p);
    return;
  }

  // * GetEnd() - Return the first out-of-bound pointer
  inline KeyValuePairType *GetEnd() { 
    return kv_begin + BaseClassType::GetSize(); 
  }
  // * begin() and * end() - C++ iterator interface
  inline KeyValuePairType *begin() { return kv_begin; }
  inline KeyValuePairType *end() { return GetEnd(); }

  /*
   * Search() - Find the lower bound item of the key
   * 
   * The lower bound item I is defined as the largest I such that key >= I
   */
  KeyValuePairType *Search(const KeyType &key) {
    assert(BaseClassType::KeyInNode(key));
    KeyValuePairType *kv_p = std::upper_bound(begin(), end(), key);
    return kv_p == GetEnd() ? nullptr : kv_p - 1;
  }

 private:
  // Instance of high key
  KeyValuePairType high_key;
  DeltaChainType delta_chain;
  // This member does not take any storage, but let us obtain the address
  // of the memory address after all class members
  KeyValuePairType kv_begin[0];
};

} // namespace bwtree
} // namespace index_building_block
} // namespace wangziqi2013

#endif