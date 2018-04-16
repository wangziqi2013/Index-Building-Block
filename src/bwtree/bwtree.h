
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

/*
 * BoundKey() - Represents low key and high key which can be infinities
 */
template <typename KeyType>
class BoundKey {
 public:
  KeyType key;
  bool inf;
  inline bool IsInf() const { return inf; }
  // Operators for checking magnitude with a key
  // An inf key could not be compared with a key using key comparator. The caller
  // should first call GetInf() to determine
  // * operator<
  inline bool operator<(const KeyType &k) const { assert(!inf); return key < k; }
  // * operator>
  inline bool operator>(const KeyType &k) const { assert(!inf); return key > k; }
  // * operator==
  inline bool operator==(const KeyType &k) const { assert(!inf); return key == k; }
  // * operator!=
  inline bool operator!=(const KeyType &k) const { assert(!inf); return key != k; }
  // * operator>=
  inline bool operator>=(const KeyType &k) const { assert(!inf); return key >= k; }
  // * operator<=
  inline bool operator<=(const KeyType &k) const { assert(!inf); return key <= k; }
  // * GetInf() - Returns the infinite key
  inline static BoundKey GetInf() { return BoundKey{KeyType{}, true}; }
  // * Get() - Returns a key
  inline static BoundKey Get(const KeyType &key) { return BoundKey{key, false}; }
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
  // Invalid node ID is defined as the largest possible unsigned value
  static constexpr NodeIDType INVALID_NODE_ID = static_cast<NodeIDType>(-1);

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
  // * Get() - Allocate an instance of the mapping table
  static DefaultMappingTable *Get() { return new DefaultMappingTable{}; }
  // * Destroy() - The destructor of the mapping table instance
  static void Destroy(DefaultMappingTable *mapping_table_p) { delete mapping_table_p; }

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

  // * At() - Returns the content on a given index
  inline BaseNodeType *At(NodeIDType node_id) {
    assert(node_id < TABLE_SIZE);
    return mapping_table[node_id].load();
  }

  // * Reset() - Clear the content as well as the index
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
 * 
 * 1. No pre-allocation is implemented. Override this class to 
 *    implement pre-allocation
 * 2. This class has zero size under release mode
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

  // * AllocateDelta() - Allocate a delta record of a given type
  template<typename DeltaType, typename ...Args>
  inline void AllocateDelta(Args &&...args) {
    IF_DEBUG(mem_usage.fetch_add(sizeof(DeltaType)));
    return new DeltaType{args...};
  }

 private:
  IF_DEBUG(std::atomic<size_t> mem_usage);
};

/*
 * class NodeBase - Base class of base node and delta node types
 * 
 * Virtual node abstraction is defined in this class
 */
template <typename KeyType>
class NodeBase {
 public:
  using BoundKeyType = BoundKey<KeyType>;
  using NodeSizeType = uint32_t;
  using NodeHeightType = uint16_t;

 protected:
  /*
   * NodeBase() - Constructor
   */
  NodeBase(NodeType ptype, NodeHeightType pheight, NodeSizeType psize,
           BoundKeyType *plow_key_p, BoundKeyType *phigh_key_p) :
    type{ptype}, height{pheight}, size{psize},
    low_key_p{plow_key_p}, high_key_p{phigh_key_p} {}

 public:
  // * GetSize() - Returns the size
  inline NodeSizeType GetSize() const { return size; }
  // * GetDepth() - Returns the depth
  inline NodeHeightType GetHeight() const { return height; }
  // * GetType() - Returns the type enum
  inline NodeType GetType() const { return type; }
  // * GetHighKey() - Returns high key
  inline const BoundKeyType *GetHighKey() const { return high_key_p; }
  // * GetLowKey() - Returns low key
  inline const BoundKeyType *GetLowKey() const { return low_key_p; }

  // * KeyLargerThanNode() - Return whether a given key is larger than
  //                         all keys in the node
  inline bool KeyLargerThanNode(const KeyType &key) {
    return high_key_p->IsInf() == false && *high_key_p <= key;
  }

  // * KeySmallerThanNode() - Returns whether the given key is smaller than
  //                          all keys in the node
  inline bool KeySmallerThanNode(const KeyType &key) {
    return low_key_p->IsInf() == false && *low_key_p > key;
  }

  // * KeyInNode() - Return whether a given key is within the node's range
  inline bool KeyInNode(const KeyType &key) { 
    return KeyLargerThanNode(key) == false && KeySmallerThanNode(key) == false;
  }

 private:
  // The following three are packed into a 64 bit integer
  NodeType type;
  // Height in the delta chain (0 means base node)
  NodeHeightType height;
  // Number of elements
  NodeSizeType size;
  BoundKeyType *low_key_p;
  BoundKeyType *high_key_p;
};

// * class DeltaNode - Stores the next node pointer
template <typename KeyType>
class DeltaNode : public NodeBase<KeyType> {
 public:
  using BaseClassType = NodeBase<KeyType>;
  using NodeSizeType = typename BaseClassType::NodeSizeType;
  using NodeHeightType = typename BaseClassType::NodeHeightType;
  using BoundKeyType = typename BaseClassType::BoundKeyType;

  inline BaseClassType *GetNext() const { return next_node_p; }
 protected:
  //* DeltaNode() - Constructor
  DeltaNode(NodeType ptype, NodeHeightType pheight, NodeSizeType psize,
            BoundKeyType *plow_key_p, BoundKeyType *phigh_key_p,
            BaseClassType *pnext_node_p) :
    BaseClassType{ptype, pheight, psize, plow_key_p, phigh_key_p},
    next_node_p{pnext_node_p} {}
 private:
  BaseClassType *next_node_p;
};

// * class KeyDeltaNode - Base class for delta nodes that contain the key
template <typename KeyType>
class KeyDeltaNode : DeltaNode<KeyType> {
 public:
  using BaseClassType = DeltaNode<KeyType>;
  using BaseBaseClassType = typename BaseClassType::BaseClassType;
  using NodeSizeType = typename BaseClassType::NodeSizeType;
  using NodeHeightType = typename BaseClassType::NodeHeightType;
  using BoundKeyType = typename BaseClassType::BoundKeyType;
  
  // * GetKey() - Returns the key
  KeyType &GetKey() { return key; }
 protected:
  // * KeyDeltaNode() - Constructor
  KeyDeltaNode(NodeType ptype, NodeHeightType pheight, NodeSizeType psize,
               BoundKeyType *plow_key_p, BoundKeyType *phigh_key_p,
               BaseBaseClassType *pnext_node_p, const KeyType &pkey) : 
    BaseClassType{ptype, pheight, psize, plow_key_p, phigh_key_p, pnext_node_p},
    key{pkey} {}
 private:
  KeyType key;
};

template <typename KeyType, typename ValueType>
class KeyValueDeltaNode : KeyDeltaNode<KeyType> {
 public:

 private:
};

/*
 * class DefaultBaseNode - This class defines the way key and values are stored
 *                         in the base node
 * 
 * 1. Delta allocation is defined in delta chain class
 * 2. Node consolidation is defined in node consolidator class
 * 3. Only unique key is supported; Non-unique key must be implemented
 *    outside the index
 * 4. The node should not expose its storage of keys and values to the external
 *    That requires that no method for accessing internal storage other than
 *    individual keys and values are provided. Iterators are not available.
 *    Search routine should only return an index rather than raw pointer
 * 5. Non-unique key support should be expposed by a constexpr var
 *    and the caller is responsible for checking consistency between 
 *    feature supports
 */
template <typename KeyType, 
          typename ValueType, 
          typename DeltaChainType>
class DefaultBaseNode : public NodeBase<KeyType> {
 public:
  using BaseClassType = NodeBase<KeyType>;
  using NodeSizeType = typename BaseClassType::NodeSizeType;
  using NodeHeightType = typename BaseClassType::NodeHeightType;
  using BoundKeyType = typename BaseClassType::BoundKeyType;
  // Whether only support unique keys
  static constexpr bool support_non_unique_key = false;
 private:
  // * DefaultBaseNode() - Private Constructor
  DefaultBaseNode(NodeType ptype, 
                  NodeHeightType pheight,
                  NodeSizeType psize,
                  const BoundKeyType &plow_key,
                  const BoundKeyType &phigh_key) :
    BaseClassType{ptype, pheight, psize, &low_key, &high_key},
    low_key{plow_key},
    high_key{phigh_key},
    delta_chain{} {
    return;
  } 
  
  // * ~DefaultBaseNode() - Private Destructor
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
                              NodeSizeType psize,
                              const BoundKeyType &plow_key,
                              const BoundKeyType &phigh_key) {
    assert(ptype == NodeType::InnerBase || ptype == NodeType::LeafBase);
    // Size for key value pairs and size for the structure itself
    size_t extra_size = size_t{psize} * (sizeof(KeyType) + sizeof(ValueType));
    size_t total_size = extra_size + sizeof(DefaultBaseNode);

    void *p = new unsigned char[total_size];
    DefaultBaseNode *node_p = \
      static_cast<DefaultBaseNode *>(
        new (p) DefaultBaseNode{ptype, NodeHeightType{0}, psize, plow_key, phigh_key});
    
    return node_p;
  }

  /*
   * Destroy() - Frees the memory and calls destructor
   * 
   * 1. The delta chain's destructor will be called in this case. Make sure
   *    all delta chain elements have been destroyed before this is called
   */
  static void Destroy(DefaultBaseNode *node_p) {
    node_p->~DefaultBaseNode();
    delete[] reinterpret_cast<unsigned char *>(node_p);
    return;
  }

  // * KeyAt() - Access key on a particular index
  inline KeyType &KeyAt(int index) { return KeyBegin()[index]; }
  // * ValueAt() - Access value on a particular index
  inline ValueType &ValueAt(int index) {
    assert(static_cast<NodeSizeType>(index) < BaseClassType::GetSize());
    return ValueBegin()[index];
  }

  /*
   * Search() - Find the lower bound item of a search key
   * 
   * The lower bound item I is defined as the largest I such that key >= I
   * We implement this using std::upper_bound and then decrement by 1. 
   * std::upper_bound finds the smallest I' such that key < I'. If no such
   * I' exists, which means the key is >= all items, it returns end()
   */
  int Search(const KeyType &key) {
    assert(BaseClassType::KeyInNode(key));
    // Note that the first key do not need to be searched for both leaf and 
    // inner nodes
    int ret = (std::upper_bound(KeyBegin() + 1, KeyEnd(), key) - KeyBegin()) - 1;
    assert(ret >= 0 && ret < static_cast<int>(BaseClassType::GetSize()));
    return ret;
  }

  /*
   * Split() - Split the node into two smaller halves
   * 
   * 1. Only unique key split is supported. For non-unique keys please override
   *    this method in a derived class
   * 2. The split point is chosen as the middle of the node. The current node
   *    is not changed, and we copy the upper half of the content into another
   *    node and return it
   * 3. The node size must be greater than 1. Otherwise assertion fails
   * 4. The low key of the upper half is set to the split key. The high key
   *    of the upper is the same as the current node. The current node's high
   *    key should be updated by the split delta
   */
  DefaultBaseNode *Split() {
    NodeSizeType old_size = BaseClassType::GetSize();
    assert(old_size > 1);
    // The index of the split key which is also the low key of the new node
    NodeSizeType pivot = old_size / 2;
    NodeSizeType new_size = old_size - pivot;
    // Note that low key for new node is always not inf
    DefaultBaseNode *node_p = \
      Get(BaseClassType::GetType(), new_size, 
          {KeyAt(static_cast<int>(pivot)), false}, *BaseClassType::GetHighKey());
    // Copy the upper half of the current node into the new node
    std::copy(KeyBegin() + pivot, KeyEnd(), node_p->KeyBegin());
    std::copy(ValueBegin() + pivot, ValueEnd(), node_p->ValueBegin());

    return node_p;
  }

 private:
  // * KeyBegin() - Return the first pointer for values
  inline ValueType *KeyBegin() { return key_begin; }
  // * KeyEnd() - Return the first out-of-bound pointer for keys
  inline KeyType *KeyEnd() { return key_begin + BaseClassType::GetSize(); }
  // * ValueBegin() - Return the first pointer for values
  inline ValueType *ValueBegin() { return reinterpret_cast<ValueType *>(KeyEnd()); }
  // * ValueEnd() - Return the first out-of-bound pointer for values
  inline ValueType *ValueEnd() { return ValueBegin() + BaseClassType::GetSize(); }

  // Instances of low and high keys
  BoundKeyType low_key;
  BoundKeyType high_key;
  DeltaChainType delta_chain;
  // This member does not take any storage, but let us obtain the address
  // of the memory address after all class members
  KeyType key_begin[0];
};

} // namespace bwtree
} // namespace index_building_block
} // namespace wangziqi2013

#endif