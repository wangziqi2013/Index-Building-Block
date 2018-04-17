
#include "bwtree/bwtree.h"
#include "test-util.h"

using namespace wangziqi2013;
using namespace index_building_block;
using namespace bwtree;

/*
 * MappingTableTest() - Tests mapping table
 * 
 * 1. Single thread allocation of node ID
 * 2. Concurrent allocation of node ID
 * 3. CAS
 * 4. Overflow under debug mode
 */
BEGIN_DEBUG_TEST(MappingTableTest) {
  constexpr size_t size = 1024 * 1024;
  constexpr size_t thread_num = 16;
  using MappingTableType = DefaultMappingTable<char, size>;
  using NodeIDType = typename MappingTableType::NodeIDType;
  MappingTableType *mapping_table = MappingTableType::Get();

  // Print this
  test_printf("INVALID_NODE_ID = 0x%lX\n", MappingTableType::INVALID_NODE_ID);
  always_assert(MappingTableType::INVALID_NODE_ID + 1 == 0);

  auto func = [size, mapping_table](size_t thread_id, size_t thread_num) {
    always_assert(thread_id < thread_num);
    const size_t per_thread = size / thread_num;
    const size_t begin_node_id = thread_id * per_thread;
    const size_t end_node_id = begin_node_id + per_thread;
    char *p = reinterpret_cast<char *>(begin_node_id);
    for(size_t i = begin_node_id;i < end_node_id;i++) {
      NodeIDType node_id = mapping_table->AllocateNodeID(p);
      always_assert(mapping_table->At(node_id) == \
                    reinterpret_cast<char *>(p));
      p++;
    }

    return;
  };

  // This function checks the mapping table
  // And will change its content
  auto verify = [mapping_table]() {
    for(size_t i = 0;i < size;i++) {
      char *node_p = mapping_table->At(i);
      bool ret;
      ret = \
        mapping_table->CAS(i, node_p, node_p + 1);
      always_assert(ret == true);
      ret = \
        mapping_table->CAS(i, node_p, node_p + 1);
      always_assert(ret == false);
      ret = \
        mapping_table->CAS(i, node_p + 1, node_p);
      always_assert(ret == true);
    }

    return;
  };

  test_printf("Single thread test\n");
  // Single thread allocation test
  func(0, 1);
  verify();
  // Reset
  mapping_table->Reset();
  test_printf("Multithread test\n");
  // Multithreaded test
  StartThread(thread_num, func, thread_num);
  verify();

  MappingTableType::Destroy(mapping_table);

  return;
} END_TEST

/*
 * BoundKeyTest() - Tests whether bound key works correctly
 */
BEGIN_DEBUG_TEST(BoundKeyTest) {
  using BoundKeyType = BoundKey<int>;
  BoundKeyType inf_key = BoundKeyType::GetInf();
  BoundKeyType normal_key = BoundKeyType::Get(100);

  always_assert(TestAssertionFail(inf_key > 1));
  always_assert(TestAssertionFail(inf_key < 1));
  always_assert(TestAssertionFail(inf_key == 1));
  always_assert(TestAssertionFail(inf_key != 1));

  always_assert(normal_key == 100);
  always_assert(normal_key < 101);
  always_assert(normal_key > 99);
  always_assert(normal_key != -1);
  always_assert(normal_key >= 100);
  always_assert(normal_key <= 100);

  return;
} END_TEST

/*
 * BaseNodeTest() - Tests whether base nodes work correctly
 * 
 * 1. The allocation and destruction of base nodes
 * 2. The iterator semantics
 * 3. Search semantics
 */
BEGIN_DEBUG_TEST(BaseNodeTest) {
  // Ignore delta chain type here
  using BaseNodeType = DefaultBaseNode<int, int, DefaultDeltaChain>;
  using NodeSizeType = typename BaseNodeType::NodeSizeType;
  constexpr NodeSizeType size = 256;
  constexpr int high_key = 1000;
  constexpr int low_key = 0;

  BaseNodeType *node_p = BaseNodeType::Get(NodeType::LeafBase, size, {low_key, true}, {high_key, true});
  for(int i = 0;i < (int)size;i++) {
    node_p->KeyAt(i) = i * 2;
    node_p->ValueAt(i) = i * 2 + 1;
  }

  for(int i = 0;i < high_key;i++) {
    int index = node_p->Search(i);
    const int &value = node_p->ValueAt(index);
    if(i < (int)size * 2) { 
      if(i % 2 == 0) {
        always_assert(value == i + 1);
      } else {
        always_assert(value == i);
      }
    } else {
      always_assert(value == size * 2 - 1);
    }
  }

  // The following two would not fail beause the low key and high key are set to inf
  always_assert(TestAssertionFail(node_p->Search(-1)) == 0);
  always_assert(TestAssertionFail(node_p->Search(high_key)) == 0);

  // Split the new node
  BaseNodeType *new_node_p = node_p->Split();
  NodeSizeType new_size = new_node_p->GetSize();
  always_assert(new_node_p->GetSize() == size / 2);
  always_assert(new_node_p->KeyAt(0) == size);
  always_assert(new_node_p->ValueAt(0) == size + 1);
  always_assert((new_node_p->KeyAt(new_size - 1)) == (size - 1) * 2);
  always_assert((new_node_p->ValueAt(new_size - 1)) == (size - 1) * 2 + 1);
  always_assert(new_node_p->GetHighKey()->key == high_key);

  int key = new_node_p->KeyAt(0);
  for(NodeSizeType i = 0;i < new_node_p->GetSize();i++) {
    always_assert(new_node_p->KeyAt(i) == key);
    always_assert(new_node_p->ValueAt(i) == key + 1);
    key += 2;
  }

  BaseNodeType::Destroy(node_p);

  // Test illegal split (size = 1); Should fail assertion
  // Also illegal search would fail as the low key and high key are finite (0 and 1000 resp.)
  node_p = BaseNodeType::Get(NodeType::LeafBase, 1, {low_key, false}, {high_key, false});
  always_assert(TestAssertionFail(node_p->Split()));
  always_assert(TestAssertionFail(node_p->Search(-1)));
  always_assert(TestAssertionFail(node_p->Search(high_key)));
  BaseNodeType::Destroy(node_p);

  return;
} END_TEST

/*
 * DeltaNodeTest() - Tests whether delta node template works as expected
 */
BEGIN_DEBUG_TEST(DeltaNodeTest) {
  using KeyType = int;
  using ValueType = std::string;
  using NodeIDType = uint64_t;

  using LeafInsertType = LEAF_INSERT_TYPE(KeyType, ValueType);
  using LeafDeleteType = LEAF_DELETE_TYPE(KeyType, ValueType);
  using LeafSplitType = LEAF_SPLIT_TYPE(KeyType, NodeIDType);
  using LeafMergeType = LEAF_MERGE_TYPE(KeyType, NodeIDType);
  using LeafRemoveType = LEAF_REMOVE_TYPE(KeyType, NodeIDType);
  //using InnerInsertType = LEAF_INSERT_TYPE(KeyType, NodeIDType);
  //using InnerDeleteType = LEAF_INSERT_TYPE(KeyType, NodeIDType);
  //using InnerSplitType = LEAF_INSERT_TYPE(KeyType, NodeIDType);
  //using InnerMergeType = LEAF_INSERT_TYPE(KeyType, NodeIDType);
  //using InnerRemoveType = LEAF_INSERT_TYPE(KeyType, NodeIDType);

  using LeafBaseNodeType = DefaultBaseNode<KeyType, ValueType, DefaultDeltaChain>;
  using NodeSizeType = typename LeafBaseNodeType::NodeSizeType;
  using NodeHeightType = typename LeafBaseNodeType::NodeHeightType;
  NodeSizeType size = 256;
  NodeHeightType height = 0;
  KeyType high_key = 1000;
  KeyType low_key = 25;
  KeyType insert_key = 100, delete_key = 200;
  std::string insert_value = "key = 100", delete_value = "key = 200";
  KeyType split_high_key = 500;
  KeyType merge_middle_key = 600;
  NodeIDType merge_sibling_id = NodeIDType{8888};
  NodeIDType split_sibling = NodeIDType{9999};
  NodeIDType remove_id = NodeIDType{7777};
  NodeBase<KeyType> *merge_sibling = nullptr;

  LeafBaseNodeType *node_p = LeafBaseNodeType::Get(NodeType::LeafBase, size, {low_key, true}, {high_key, true});

  LeafInsertType *insert_node_p = node_p->AllocateDelta<LeafInsertType>(
    NodeType::LeafInsert, ++height, size + 1, node_p->GetLowKey(), node_p->GetHighKey(), node_p, 
    insert_key, insert_value);

  LeafDeleteType *delete_node_p = node_p->AllocateDelta<LeafDeleteType>(
    NodeType::LeafDelete, ++height, size - 1, insert_node_p->GetLowKey(), insert_node_p->GetHighKey(), insert_node_p, 
    delete_key, delete_value);

  LeafSplitType *split_node_p = node_p->AllocateDelta<LeafSplitType>(
    NodeType::LeafSplit, ++height, size / 2, delete_node_p->GetLowKey(), delete_node_p->GetHighKey(), delete_node_p, 
    split_high_key, split_sibling);

  LeafMergeType *merge_node_p = node_p->AllocateDelta<LeafMergeType>(
    NodeType::LeafMerge, ++height, size * 2, split_node_p->GetLowKey(), split_node_p->GetHighKey(), split_node_p, 
    merge_middle_key, merge_sibling_id, merge_sibling);

  LeafRemoveType *remove_node_p = node_p->AllocateDelta<LeafRemoveType>(
    NodeType::LeafRemove, ++height, size * 2, merge_node_p->GetLowKey(), merge_node_p->GetHighKey(), merge_node_p, 
    remove_id);

  // Check whether attributes are as expected
  always_assert(insert_node_p->GetInsertKey() == insert_key);
  always_assert(insert_node_p->GetInsertValue() == insert_value);
  always_assert(delete_node_p->GetDeleteKey() == delete_key);
  always_assert(delete_node_p->GetDeleteValue() == delete_value);
  always_assert(split_node_p->GetSplitKey() == split_high_key);
  always_assert(split_node_p->GetSplitNodeID() == split_sibling);
  always_assert(merge_node_p->GetMergeKey() == merge_middle_key);
  always_assert(merge_node_p->GetMergeNodeID() == merge_sibling_id);
  always_assert(merge_node_p->GetMergeSibling() == merge_sibling);
  always_assert(remove_node_p->GetRemoveNodeID() == remove_id);

  return;
} END_TEST

int main() {
  MappingTableTest();
  BoundKeyTest();
  BaseNodeTest();
  DeltaNodeTest();

  return 0;
}