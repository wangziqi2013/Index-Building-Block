
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
 * BaseNodeTest() - Tests whether base nodes work correctly
 * 
 * 1. The allocation and destruction of base nodes
 * 2. The iterator semantics
 * 3. Search semantics
 */
BEGIN_DEBUG_TEST(BaseNodeTest) {
  // Ignore delta chain type here
  using BaseNodeType = DefaultBaseNode<int, int, DefaultDeltaChain>;
  constexpr NodeSizeType size = 256;
  using KeyValuePairType = typename BaseNodeType::KeyValuePairType;
  constexpr int high_key = 1000;

  BaseNodeType *node_p = BaseNodeType::Get(NodeType::LeafBase, size, {high_key, high_key + 1});
  for(int i = 0;i < (int)size;i++) {
    (*node_p)[i] = KeyValuePairType{i * 2, i * 2 + 1};
  }

  for(int i = 0;i < high_key;i++) {
    KeyValuePairType *kv_p = node_p->Search(i);
    always_assert(kv_p != nullptr);
    if(i < (int)size * 2) { 
      if(i % 2 == 0) {
        always_assert(kv_p->value == i + 1);
      } else {
        always_assert(kv_p->value == i);
      }
    } else {
      always_assert(kv_p->value == size * 2 - 1);
    }
  }

  // The following two would fail
  TestAssertionFail([node_p](){node_p->Search(-1);});
  TestAssertionFail([node_p, high_key](){node_p->Search(high_key);});

  // Split the new node
  BaseNodeType *new_node_p = node_p->Split();
  always_assert(new_node_p->GetSize() == size / 2);
  always_assert(new_node_p->begin()->key == size);
  always_assert(new_node_p->begin()->value == size + 1);
  always_assert((new_node_p->end() - 1)->key == (size - 1) * 2);
  always_assert((new_node_p->end() - 1)->value == (size - 1) * 2 + 1);
  always_assert(new_node_p->GetHighKeyValuePair()->key == high_key);
  always_assert(new_node_p->GetHighKeyValuePair()->value == high_key + 1);

  int key = new_node_p->begin()->key;
  for(const KeyValuePairType &kvp : *new_node_p) {
    always_assert(kvp.key == key);
    always_assert(kvp.value == key + 1);
    key += 2;
  }

  BaseNodeType::Destroy(node_p);

  // Test illegal split (size = 1); Should fail assertion
  node_p = BaseNodeType::Get(NodeType::LeafBase, 1, {high_key, high_key + 1});
  TestAssertionFail([node_p](){node_p->Split();});

  return;
} END_TEST

int main() {
  MappingTableTest();
  BaseNodeTest();
  return 0;
}