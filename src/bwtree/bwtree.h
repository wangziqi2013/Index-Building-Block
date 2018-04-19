
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
 * Type Naming and Argument Passing Rule
 * 
 * 1. Complete types must end with "Type" suffix to indicate no template argument is required
 * 2. Incomplete types must not end with "Type". Once they are instanciated with "using"
 *    directive, the complete type must then be the original type name suffixed by "Type"
 * 3. Types prefixed with "Default" cannot be directly used. They must be passed as template arguments
 *    Other types can be directly referred to.
 * 4. When passing types as template arguments, it is preferred that the type is passed as 
 *    template instances rather than raw templates. This, however, is not strict.
 */

/*
 * BoundKey() - Represents low key and high key which can be infinities
 */
template <typename KeyType>
class BoundKey {
 public:
  KeyType key;
  bool inf;
  // * BoundKey() - Constructor
  BoundKey(const KeyType &pkey) : key{pkey} {} 
  // * IsInf() - Whether the key is infinite
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
  static constexpr NodeIDType FIRST_NODE_ID = 0;

 private:
  /*
   * DefaultMappingTable() - Private Constructor
   * 
   * The constructor is private to avoid allocating a mapping table on the stack
   * or directly putting it as a memory, as the table can be potentially large
   */
  DefaultMappingTable() : 
    next_slot{FIRST_NODE_ID} {
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
 * class DefaultDeltaChainType - This class defines the storage of the delta chain
 * 
 * 1. No pre-allocation is implemented. Override this class to 
 *    implement pre-allocation
 * 2. This class has zero size under release mode
 */
class DefaultDeltaChainType {
 public:
  /*
   * DefaultDeltaChainType() - Constructor
   */
  DefaultDeltaChainType() {
    IF_DEBUG(mem_usage.store(0UL));
    return;
  }

  // * AllocateDelta() - Allocate a delta record of a given type
  template<typename AllocDeltaNodeType, typename ...Args>
  inline AllocDeltaNodeType *AllocateDelta(Args &&...args) {
    IF_DEBUG(mem_usage.fetch_add(sizeof(AllocDeltaNodeType)));
    return new AllocDeltaNodeType{args...};
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
  inline BoundKeyType *GetHighKey() const { return high_key_p; }
  // * SetHighKey() - Updates the high key of the node
  inline void SetHighKey(BoundKey *phigh_key_p) { high_key_p = phigh_key_p; }
  // * GetLowKey() - Returns low key
  inline BoundKeyType *GetLowKey() const { return low_key_p; }

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

#define LEAF_INSERT_TYPE(KeyType, ValueType) \
  DeltaNode<KeyType, KeyType, ValueType, char[0], char[0], char[0], char[0]>
#define LEAF_DELETE_TYPE(KeyType, ValueType) \
  DeltaNode<KeyType, KeyType, ValueType, char[0], char[0], char[0], char[0]>
#define LEAF_SPLIT_TYPE(KeyType, NodeIDType) \
  DeltaNode<BoundKey<KeyType>, KeyType, NodeIDType, char[0], char[0], char[0], char[0]>
#define INNER_SPLIT_TYPE(KeyType, NodeIDType) \
  DeltaNode<BoundKey<KeyType>, KeyType, NodeIDType, char[0], char[0], char[0], char[0]>
#define LEAF_MERGE_TYPE(KeyType, NodeIDType) \
  DeltaNode<KeyType, KeyType, NodeIDType, NodeBase<KeyType> *, char[0], char[0], char[0]>
#define INNER_MERGE_TYPE(KeyType, NodeIDType) \
  DeltaNode<KeyType, KeyType, NodeIDType, NodeBase<KeyType> *, char[0], char[0], char[0]>
#define LEAF_REMOVE_TYPE(KeyType, NodeIDType) \
  DeltaNode<KeyType, NodeIDType, char[0], char[0], char[0], char[0], char[0]>
#define INNER_REMOVE_TYPE(KeyType, NodeIDType) \
  DeltaNode<KeyType, NodeIDType, char[0], char[0], char[0], char[0], char[0]>
#define INNER_INSERT_TYPE(KeyType, NodeIDType) \
  DeltaNode<KeyType, KeyType, NodeIDType, KeyType, NodeIDType, char[0], char[0]>
#define INNER_DELETE_TYPE(KeyType, NodeIDType) \
  DeltaNode<KeyType, KeyType, NodeIDType, KeyType, NodeIDType, KeyType, NodeIDType>

/*
 * class DeltaNode - Stores the next node pointer
 * 
 * This class is heavily templatized. Different combinations of types
 * yield different delta types:
 * 
 * LeafInsertType/LeafDeleteType = 
 *   DeltaNode<KeyType, KeyType, ValueType, char[0], char[0], char[0], char[0]>
 * LeafSplitType/InnerSplitType = 
 *   DeltaNode<KeyType, KeyType, NodeIDType, char[0], char[0], char[0], char[0]>
 * LeafMergeType/InnerMergeType = 
 *   DeltaNode<KeyType, KeyType, NodeIDType, NodeBase<KeyType> *, char[0], char[0], char[0]>
 * LeafRemoveType/InnerRemoveType = 
 *   DeltaNode<KeyType, NodeIDType, char[0], char[0], char[0], char[0], char[0]>
 * InnerInsertType = 
 *   DeltaNode<KeyType, KeyType, NodeIDType, KeyType, NodeIDType, char[0], char[0]>
 * InnerDeleteType = 
 *   DeltaNode<KeyType, KeyType, NodeIDType, KeyType, NodeIDType, KeyType, NodeIDType>
 */
template <typename KeyType, 
          typename T1, typename T2, typename T3, 
          typename T4, typename T5, typename T6>
class DeltaNode : public NodeBase<KeyType> {
 public:
  using BaseClassType = NodeBase<KeyType>;
  using NodeSizeType = typename BaseClassType::NodeSizeType;
  using NodeHeightType = typename BaseClassType::NodeHeightType;
  using BoundKeyType = typename BaseClassType::BoundKeyType;

  inline BaseClassType *GetNext() const { return next_node_p; }

  //* DeltaNode() - Constructors
  DeltaNode(NodeType ptype, NodeHeightType pheight, NodeSizeType psize,
            BoundKeyType *plow_key_p, BoundKeyType *phigh_key_p,
            BaseClassType *pnext_node_p, 
            const T1 &pt1) :
    BaseClassType{ptype, pheight, psize, plow_key_p, phigh_key_p},
    next_node_p{pnext_node_p}, 
    t1{pt1} {}
  
  DeltaNode(NodeType ptype, NodeHeightType pheight, NodeSizeType psize,
            BoundKeyType *plow_key_p, BoundKeyType *phigh_key_p,
            BaseClassType *pnext_node_p, 
            const T1 &pt1, const T2 &pt2) :
    BaseClassType{ptype, pheight, psize, plow_key_p, phigh_key_p},
    next_node_p{pnext_node_p}, 
    t1{pt1}, t2{pt2} {}

  DeltaNode(NodeType ptype, NodeHeightType pheight, NodeSizeType psize,
            BoundKeyType *plow_key_p, BoundKeyType *phigh_key_p,
            BaseClassType *pnext_node_p, 
            const T1 &pt1, const T2 &pt2, const T3 &pt3) :
    BaseClassType{ptype, pheight, psize, plow_key_p, phigh_key_p},
    next_node_p{pnext_node_p}, 
    t1{pt1}, t2{pt2}, t3{pt3} {}

  DeltaNode(NodeType ptype, NodeHeightType pheight, NodeSizeType psize,
            BoundKeyType *plow_key_p, BoundKeyType *phigh_key_p,
            BaseClassType *pnext_node_p, 
            const T1 &pt1, const T2 &pt2, const T3 &pt3,
            const T4 &pt4) :
    BaseClassType{ptype, pheight, psize, plow_key_p, phigh_key_p},
    next_node_p{pnext_node_p}, 
    t1{pt1}, t2{pt2}, t3{pt3}, t4{pt4} {}

  DeltaNode(NodeType ptype, NodeHeightType pheight, NodeSizeType psize,
            BoundKeyType *plow_key_p, BoundKeyType *phigh_key_p,
            BaseClassType *pnext_node_p, 
            const T1 &pt1, const T2 &pt2, const T3 &pt3,
            const T4 &pt4, const T5 &pt5, const T6 &pt6) :
    BaseClassType{ptype, pheight, psize, plow_key_p, phigh_key_p},
    next_node_p{pnext_node_p}, 
    t1{pt1}, t2{pt2}, t3{pt3}, t4{pt4}, t5{pt5}, t6{pt6} {}
  
  // The following series of functions defines methods for retriving
  // delta attributes according to delta type
  inline T1 &GetInsertKey() { return t1; }
  inline T1 &GetDeleteKey() { return t1; }
  inline T1 &GetSplitKey() { return t1.key; }
  inline T1 &GetMergeKey() { return t1.key; }
  inline T1 &GetRemoveNodeID() { return t1; }
  inline void SetSplitHighKey() { BaseClassType::SetHighKey(&t1); }
  
  inline T2 &GetInsertValue() { return t2; }
  inline T2 &GetDeleteValue() { return t2; }
  inline T2 &GetInsertNodeID() { return t2; }
  inline T2 &GetDeleteNodeID() { return t2; }
  inline T2 &GetSplitNodeID() { return t2; }
  inline T2 &GetMergeNodeID() { return t2; }

  inline T3 &GetMergeSibling() { return t3; }
  inline T3 &GetNextKey() { return t3; }

  inline T4 &GetNextNodeID() { return t4; }
  inline T5 &GetPrevKey() { return t5; }
  inline T6 &GetPrevNodeID() { return t6; }

 private:
  BaseClassType *next_node_p;
  // Delta node elements
  T1 t1; T2 t2; T3 t3; T4 t4; T5 t5; T6 t6;
};

// * class DeltaType - Declares the full type of deltas
template <typename KeyType, typename ValueType, typename NodeIDType>
class Delta {
 public:
  using LeafInsertType = LEAF_INSERT_TYPE(KeyType, ValueType);
  using LeafDeleteType = LEAF_DELETE_TYPE(KeyType, ValueType);
  using LeafSplitType = LEAF_SPLIT_TYPE(KeyType, NodeIDType);
  using LeafMergeType = LEAF_MERGE_TYPE(KeyType, NodeIDType);
  using LeafRemoveType = LEAF_REMOVE_TYPE(KeyType, NodeIDType);

  using InnerInsertType = INNER_INSERT_TYPE(KeyType, NodeIDType);
  using InnerDeleteType = INNER_DELETE_TYPE(KeyType, NodeIDType);
  using InnerSplitType = INNER_SPLIT_TYPE(KeyType, NodeIDType);
  using InnerMergeType = INNER_MERGE_TYPE(KeyType, NodeIDType);
  using InnerRemoveType = INNER_REMOVE_TYPE(KeyType, NodeIDType);
};

// * class ExtendedNodeBase - Type irrelevant part of the base node
template <typename KeyType, typename DeltaChainType>
class ExtendedNodeBase : public NodeBase<KeyType> {
 public:
  using BaseClassType = NodeBase<KeyType>;
  using NodeSizeType = typename BaseClassType::NodeSizeType;
  using NodeHeightType = typename BaseClassType::NodeHeightType;
  using BoundKeyType = typename BaseClassType::BoundKeyType;

  // * ExtendedNodeBase() - Constructor
  ExtendedNodeBase(NodeType ptype, 
                   NodeHeightType pheight,
                   NodeSizeType psize,
                   const BoundKeyType &plow_key,
                   const BoundKeyType &phigh_key) : 
    BaseClassType{ptype, pheight, psize, &low_key, &high_key},
    low_key{plow_key},
    high_key{phigh_key},
    delta_chain{} {}

  // * AllocateDelta() - Wrapping around the delta chain
  template <typename AllocDeltaNodeType, typename ...Args>
  inline AllocDeltaNodeType *AllocateDelta(Args &&...args) {
    return delta_chain.AllocateDelta<AllocDeltaNodeType>(args...);
  }

  // This data member does not space but it has the same address as the low key
  char low_key_addr[0];
 private:
  // Instances of low and high keys
  BoundKeyType low_key;
  BoundKeyType high_key;
  DeltaChainType delta_chain;
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
class DefaultBaseNode : public ExtendedNodeBase<KeyType, DeltaChainType> {
 public:
  using BaseClassType = ExtendedNodeBase<KeyType, DeltaChainType>;
  using BaseBaseClassType = typename BaseClassType::BaseClassType;
  using NodeSizeType = typename BaseBaseClassType::NodeSizeType;
  using NodeHeightType = typename BaseBaseClassType::NodeHeightType;
  using BoundKeyType = typename BaseBaseClassType::BoundKeyType;
  // Whether only support unique keys
  static constexpr bool support_non_unique_key = false;
 private:
  // * DefaultBaseNode() - Private Constructor
  DefaultBaseNode(NodeType ptype, 
                  NodeHeightType pheight,
                  NodeSizeType psize,
                  const BoundKeyType &plow_key,
                  const BoundKeyType &phigh_key) :
    BaseClassType{ptype, pheight, psize, plow_key, phigh_key} {
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
    assert(static_cast<NodeSizeType>(index) < BaseBaseClassType::GetSize());
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
    assert(BaseBaseClassType::KeyInNode(key));
    // Note that the first key do not need to be searched for both leaf and 
    // inner nodes
    int ret = (std::upper_bound(KeyBegin() + 1, KeyEnd(), key) - KeyBegin()) - 1;
    assert(ret >= 0 && ret < static_cast<int>(BaseBaseClassType::GetSize()));
    return ret;
  }

  // * PointSearch() - Returns the index if exact match is found or -1 otherwise
  int PointSearch(const KeyType &key) {
    int index = Search(key);
    return KeyAt(index) == key ? index : -1;
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
    NodeSizeType old_size = BaseBaseClassType::GetSize();
    assert(old_size > 1);
    // The index of the split key which is also the low key of the new node
    NodeSizeType pivot = old_size / 2;
    NodeSizeType new_size = old_size - pivot;
    // Note that low key for new node is always not inf
    DefaultBaseNode *node_p = \
      Get(BaseBaseClassType::GetType(), new_size, 
          {KeyAt(static_cast<int>(pivot)), false}, *BaseBaseClassType::GetHighKey());
    // Copy the upper half of the current node into the new node
    std::copy(KeyBegin() + pivot, KeyEnd(), node_p->KeyBegin());
    std::copy(ValueBegin() + pivot, ValueEnd(), node_p->ValueBegin());

    return node_p;
  }
 public:
  
 private:
  // * KeyBegin() - Return the first pointer for values
  inline ValueType *KeyBegin() { return key_begin; }
  // * KeyEnd() - Return the first out-of-bound pointer for keys
  inline KeyType *KeyEnd() { return key_begin + BaseBaseClassType::GetSize(); }
  // * ValueBegin() - Return the first pointer for values
  inline ValueType *ValueBegin() { return reinterpret_cast<ValueType *>(KeyEnd()); }
  // * ValueEnd() - Return the first out-of-bound pointer for values
  inline ValueType *ValueEnd() { return ValueBegin() + BaseBaseClassType::GetSize(); }

  // This member does not take any storage, but let us obtain the address
  // of the memory address after all class members
  KeyType key_begin[0];
};

/* 
 * class TraverseHandlerBase - The base class of traverse handlers
 * 
 * All traverse handlers must inherit from this class. They do not have to implement
 * all methods (and most of them don't). For detailed template of derived handlers
 * please refer to traverser class
 * 
 * 1. "finished" is set if the traverse should end
 * 2. next_p is set to the next pointer the traverse must go if it has not finished
 * 3. Init() is called at the beginning of the traverse, including recursive traverses
 */
template <typename KeyType, typename ValueType, typename NodeIDType, 
          typename DeltaChainType>
class TraverseHandlerBase {
 public:
  using NodeBaseType = NodeBase<KeyType>;
  using DeltaType = Delta<KeyType, ValueType, NodeIDType>;
  using LeafBaseType = DefaultBaseNode<KeyType, ValueType, DeltaChainType>;
  using InnerBaseType = DefaultBaseNode<KeyType, NodeIDType, DeltaChainType>;

  // * TraverseHandlerBase() - Constructor
  TraverseHandlerBase() :
    finished{false}, next_p{nullptr} {}

  // Handler functions. If not used in the derived class just leave them undefined
  void HandleLeafBase(LeafBaseType *node_p) { Fail(); }
  void HandleInnerBase(InnerBaseType *node_p) { Fail(); }

  void HandleLeafInsert(typename DeltaType::LeafInsertType *node_p) { Fail(); }
  void HandleInnerInsert(typename DeltaType::InnerInsertType *node_p) { Fail(); }

  void HandleLeafDelete(typename DeltaType::LeafDeleteType *node_p) { Fail(); }
  void HandleInnerDelete(typename DeltaType::InnerDeleteType *node_p) { Fail(); }

  void HandleLeafISplit(typename DeltaType::LeafSplitType *node_p) { Fail(); }
  void HandleInnerSplit(typename DeltaType::InnerSplitType *node_p) { Fail(); }

  bool HandleLeafMerge(typename DeltaType::LeafMergeType *node_p) { Fail(); return false; }
  bool HandleInnerMerge(typename DeltaType::InnerMergeType *node_p) { Fail(); return false; }

  void HandleLeafRemove(typename DeltaType::LeafRemoveType *node_p) { Fail(); }
  void HandleInnerRemove(typename DeltaType::InnerRemoveType *node_p) { Fail(); }

  // * GetNext() - Returns the next pointer
  inline NodeBaseType *GetNext() { return next_p; }
  // * Finished() - Returns true if the traverse terminates
  inline bool Finished() { return finished; }
  // * Init() - Initialize states
  void Init(NodeBaseType *node_p) { (void)node_p; }
 private:
  // * Fail() - Called if the handler is not defined
  inline void Fail() { assert(false && "Unknown delta nodes"); }

 protected:
  bool finished;
  NodeBaseType *next_p;
};

/*
 * class DeltaChainTraverser - This class implements a state machine for abstracting 
 *                             away details of delta chain traversal. 
 * 
 * The TraverseHandlerType is a template argument that defines states and functions
 * for handling deltas and base nodes. Details of interfacing with the 
 * call back type is presented below:
 * 
  template <typename KeyType, typename ValueType, typename NodeIDType, 
            typename DeltaChainType>
  class TraverseHandlerType : public TraverseHandlerBase<KeyType, ValueType, NodeIDType, DeltaChainType> {
  public:
    using BaseClassType = TraverseHandlerBase<KeyType, ValueType, NodeIDType, DeltaChainType>;
    using NodeBaseType = typename BaseClassType::NodeBaseType;
    using DeltaType = typename BaseClassType::DeltaType;
    using LeafBaseType = typename BaseClassType::LeafBaseType;
    using InnerBaseType = typename BaseClassType::InnerBaseType;

    TraverseHandlerType() : TraverseHandlerBase<KeyType, ValueType, NodeIDType, DeltaChainType>{} {}

    void HandleLeafBase(LeafBaseType *node_p) { }
    void HandleInnerBase(InnerBaseType *node_p) { }

    void HandleLeafInsert(typename DeltaType::LeafInsertType *node_p) { }
    void HandleInnerInsert(typename DeltaType::InnerInsertType *node_p) { }

    void HandleLeafDelete(typename DeltaType::LeafDeleteType *node_p) { }
    void HandleInnerDelete(typename DeltaType::InnerDeleteType *node_p) { }

    void HandleLeafSplit(typename DeltaType::LeafSplitType *node_p) { }
    void HandleInnerSplit(typename DeltaType::InnerSplitType *node_p) { }

    bool HandleLeafMerge(typename DeltaType::LeafMergeType *node_p) { }
    bool HandleInnerMerge(typename DeltaType::InnerMergeType *node_p) { }

    void HandleLeafRemove(typename DeltaType::LeafRemoveType *node_p) { }
    void HandleInnerRemove(typename DeltaType::InnerRemoveType *node_p) { }

    // Omit if not needed
    void Init(NodeBaseType *node_p) { ... }
    // * GetNext() - Interface for accessing next_p
    NodeBaseType *&GetNext() { return BaseClassType::next_p; }
    // * Finished() - Interface for accessing finished
    bool &Finished() { return BaseClassType::finished; }
  };
 * 
 * 1. Base nodes must be the terminating node because they do not have next pointer.
 * 2. If merge nodes must be accessed recursively (e.g. node consolidation), then
 *    the traverser must perform this in the merge handler itself, and set finished
 *    flag to true
 */
template <typename KeyType, typename ValueType, typename NodeIDType, 
          typename DeltaChainType, template <typename, typename, typename> typename BaseNode, 
          typename TraverseHandlerType>
class DeltaChainTraverser {
 public:
  using DeltaType = Delta<KeyType, ValueType, NodeIDType>;
  using NodeBaseType = NodeBase<KeyType>;
  using LeafBaseType = BaseNode<KeyType, ValueType, DeltaChainType>;
  using InnerBaseType = BaseNode<KeyType, NodeIDType, DeltaChainType>;

  // * HandleMergeRecursive() - Handles recursive traversal of merge nodes
  template <typename MergeDeltaType>
  static void HandleMergeRecursive(bool recursive, TraverseHandlerType *handler_p, 
                                   NodeBaseType *node_p) {
    if(recursive == true) {
      Traverse(static_cast<MergeDeltaType *>(node_p)->GetNext(), handler_p);
      // Manually reset the finished flag here because we should recurse
      handler_p->Finished() = false;
      Traverse(static_cast<MergeDeltaType *>(node_p)->GetMergeSibling(), handler_p);
    }

    return;
  }

  // * Traverse() - Starts traversing the delta chain
  static void Traverse(NodeBaseType *node_p, TraverseHandlerType *handler_p) {
    // Initialization may also be based on the attributes of the virtual node.
    handler_p->Init(node_p);
    while(true) {
      NodeType type = node_p->GetType();
      switch(type) {
        case NodeType::LeafBase:
          handler_p->HandleLeafBase(static_cast<LeafBaseType *>(node_p));
          assert(handler_p->Finished() == true);
          break;
        case NodeType::InnerBase:
          handler_p->HandleInnerBase(static_cast<InnerBaseType *>(node_p));
          assert(handler_p->Finished() == true);
          break;
        case NodeType::LeafInsert:
          handler_p->HandleLeafInsert(static_cast<typename DeltaType::LeafInsertType *>(node_p));
          break;
        case NodeType::InnerInsert:
          handler_p->HandleInnerInsert(static_cast<typename DeltaType::InnerInsertType *>(node_p));
          break;
        case NodeType::LeafDelete:
          handler_p->HandleLeafDelete(static_cast<typename DeltaType::LeafDeleteType *>(node_p));
          break;
        case NodeType::InnerDelete:
          handler_p->HandleInnerDelete(static_cast<typename DeltaType::InnerDeleteType *>(node_p));
          break;
        case NodeType::LeafSplit:
          handler_p->HandleLeafSplit(static_cast<typename DeltaType::LeafSplitType *>(node_p));
          break;
        case NodeType::InnerSplit:
          handler_p->HandleInnerSplit(static_cast<typename DeltaType::InnerSplitType *>(node_p));
          break;
        case NodeType::LeafMerge:
          HandleMergeRecursive<typename DeltaType::LeafMergeType>(
            handler_p->HandleLeafMerge(static_cast<typename DeltaType::LeafMergeType *>(node_p)),
                                       handler_p, node_p);
            assert(handler_p->Finished());
          return;
        case NodeType::InnerMerge:
          HandleMergeRecursive<typename DeltaType::InnerMergeType>(
            handler_p->HandleInnerMerge(static_cast<typename DeltaType::InnerMergeType *>(node_p)),
                                        handler_p, node_p);
            assert(handler_p->Finished());
          return;
        case NodeType::LeafRemove:
          handler_p->HandleLeafRemove(static_cast<typename DeltaType::LeafRemoveType *>(node_p));
          break;
        case NodeType::InnerRemove:
          handler_p->HandleInnerRemove(static_cast<typename DeltaType::InnerRemoveType *>(node_p));
          break;
        default:
          assert(false && "Unknown node type during traversal");
      } // switch

      // If the handler says to stop then break
      if(handler_p->Finished()) {
        break;
      } else {
        node_p = handler_p->GetNext();
      }
    } // while

    return;
  }
};

/*
 * class AppendHelper - Helper class that acts as a proxy for appending deltas
 * 
 * 1. We decouple the appending of deltas from node base or mapping table class
 *    to modularize the design, because this step is largely independent from
 *    the design choices of the mapping table or the node base
 * 2. This class will be optimized out by the compiler
 */
template <typename KeyType, typename ValueType, 
          typename MappingTableType, typename DeltaChainType>
class AppendHelper {
 public:
  using NodeIDType = typename MappingTableType::NodeIDType;
  using DeltaType = Delta<KeyType, ValueType, NodeIDType>;
  using NodeBaseType = NodeBase<KeyType>;
  using NodeSizeType = typename NodeBaseType::NodeSizeType;
  using NodeHeightType = typename NodeBaseType::NodeHeightType;
  using ExtendedBaseType = ExtendedNodeBase<KeyType, DeltaChainType>;
  using BoundKeyType = typename ExtendedBaseType::BoundKeyType;
  using LeafInsertType = typename DeltaType::LeafInsertType;
  using LeafDeleteType = typename DeltaType::LeafDeleteType;
  using LeafSplitType = typename DeltaType::LeafSplitType;
  using LeafMergeType = typename DeltaType::LeafMergeType;
  using LeafRemoveType = typename DeltaType::LeafRemoveType;
  using InnerInsertType = typename DeltaType::InnerInsertType;
  using InnerDeleteType = typename DeltaType::InnerDeleteType;
  using InnerSplitType = typename DeltaType::InnerSplitType;
  using InnerMergeType = typename DeltaType::InnerMergeType;
  using InnerRemoveType = typename DeltaType::InnerRemoveType;

  // This is required for using the low key to determine the delta chain
  static constexpr size_t LOW_KEY_OFFSET = offsetof(ExtendedBaseType, low_key_addr);

  // * AppendHelper() - Constructor
  AppendHelper(NodeIDType pnode_id, NodeBaseType *pnode_p, MappingTableType *ptable_p) : 
    node_id{pnode_id}, node_p{pnode_p}, table_p{ptable_p} {}

  // * GetBase() - Returns a pointer to the base node of the delta chain
  inline ExtendedBaseType *GetBase() { 
    return reinterpret_cast<ExtendedBaseType *>(
      reinterpret_cast<char *>(node_p->GetLowKey()) - LOW_KEY_OFFSET); 
  }

  //template <typename DeltaNodeType>
  //inline DestroyDelta() {

  //}
  
  // * AppendLeafInsert() - Appends a leaf insert delta
  inline LeafInsertType *AppendLeafInsert(const KeyType &key, const ValueType &value) {
    assert(node_p->KeyInNode(key));
    // NOTE: For some strange reasons the compiler could not deduce the type of this
    // template function call. We explicitly specify the height type
    LeafInsertType *delta_p = GetBase()->template AllocateDelta<LeafInsertType, NodeType, NodeHeightType>(
      NodeType::LeafInsert, node_p->GetHeight() + 1, node_p->GetSize() + 1,
      node_p->GetLowKey(), node_p->GetHighKey(), node_p,
      key, value);
    return table_p->CAS(node_id, node_p, delta_p) ? nullptr : delta_p;
  }

  // * AppendLeafDelete() - Appends a leaf delete delta
  inline LeafDeleteType *AppendLeafDelete(const KeyType &key, const ValueType &value) {
    assert(node_p->KeyInNode(key));
    // NOTE: For some strange reasons the compiler could not deduce the type of this
    // template function call. We explicitly specify the height type
    LeafDeleteType *delta_p = GetBase()->template AllocateDelta<LeafDeleteType, NodeType, NodeHeightType>(
      NodeType::LeafDelete, node_p->GetHeight() + 1, node_p->GetSize() - 1,
      node_p->GetLowKey(), node_p->GetHighKey(), node_p,
      key, value);
    return table_p->CAS(node_id, node_p, delta_p) ? nullptr : delta_p;
  }

  // * AppendLeafSplit() - Appends a leaf split delta
  inline LeafSplitType *AppendLeafSplit(const KeyType &key, NodeIDType node_id, NodeSizeType new_size) {
    assert(node_p->KeyInNode(key));
    // NOTE: For some strange reasons the compiler could not deduce the type of this
    // template function call. We explicitly specify the height type
    LeafSplitType *delta_p = GetBase()->template AllocateDelta<LeafSplitType, NodeType, NodeHeightType>(
      NodeType::LeafSplit, node_p->GetHeight() + 1, node_p->GetSize() - new_size,
      node_p->GetLowKey(), nullptr, node_p,
      key, node_id);
    // Special code here to set the high key of the delta chain to the split key
    // which itself is a bound key
    delta_p->SetSplitHighKey();
    return table_p->CAS(node_id, node_p, delta_p) ? nullptr : delta_p;
  }

  // * AppendLeafMerge() - Appends a leaf merge delta
  inline LeafMergeType *AppendLeafMerge(const KeyType &key, NodeIDType sibling_id, NodeBaseType *sibling_p) {
    assert(node_p->KeyInNode(key));
    // NOTE: For some strange reasons the compiler could not deduce the type of this
    // template function call. We explicitly specify the height type
    LeafMergeType *delta_p = GetBase()->template AllocateDelta<LeafMergeType, NodeType, NodeHeightType>(
      NodeType::LeafMerge, node_p->GetHeight() + 1, node_p->GetSize() + sibling_p->GetSize(),
      node_p->GetLowKey(), sibling_p->GetHighKey(), node_p,
      key, sibling_id, sibling_p);
    return table_p->CAS(node_id, node_p, delta_p) ? nullptr : delta_p;
  }

 private:
  NodeIDType node_id;
  NodeBaseType *node_p;
  MappingTableType *table_p;
};

template <typename _KeyType, typename _ValueType, 
          template <typename, size_t> typename MappingTable, typename _DeltaChainType, 
          template <typename, typename, typename> typename BaseNode>
class BwTree {
 public:
  static constexpr size_t MAPPING_TABLE_SIZE = 1204 * 1024 * 16;
  // Argument types
  using KeyType = _KeyType;
  using ValueType = _ValueType;
  using DeltaChainType = _DeltaChainType;
  // Derived types
  using NodeBaseType = NodeBase<KeyType>;
  using ExtendedBaseType = ExtendedNodeBase<KeyType, DeltaChainType>;
  using MappingTableType = MappingTable<NodeBaseType, MAPPING_TABLE_SIZE>;
  // Metadata variable types
  using NodeIDType = typename MappingTableType::NodeIDType;
  using NodeSizeType = typename NodeBaseType::NodeSizeType;
  using NodeHeightType = typename NodeBaseType::NodeHeightType;
  using BoundKeyType = typename NodeBaseType::BoundKeyType;
  // Delta and base node types
  using DeltaType = Delta<KeyType, ValueType, NodeIDType>;
  using LeafBaseType = BaseNode<KeyType, ValueType, DeltaChainType>;
  using InnerBaseType = BaseNode<KeyType, NodeIDType, DeltaChainType>;
  using LeafInsertType = typename DeltaType::LeafInsertType;
  using LeafDeleteType = typename DeltaType::LeafDeleteType;
  using LeafSplitType = typename DeltaType::LeafSplitType;
  using LeafMergeType = typename DeltaType::LeafMergeType;
  using LeafRemoveType = typename DeltaType::LeafRemoveType;
  using InnerInsertType = typename DeltaType::InnerInsertType;
  using InnerDeleteType = typename DeltaType::InnerDeleteType;
  using InnerSplitType = typename DeltaType::InnerSplitType;
  using InnerMergeType = typename DeltaType::InnerMergeType;
  using InnerRemoveType = typename DeltaType::InnerRemoveType;
};

} // namespace bwtree
} // namespace index_building_block
} // namespace wangziqi2013

#endif