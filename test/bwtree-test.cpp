
#include "bwtree/bwtree.h"
#include "test-util.h"

using namespace wangziqi2013;
using namespace index_building_block;

/*
 * MappingTableTest() - Tests mapping table
 * 
 * 1. Single thread allocation of node ID
 * 2. Concurrent allocation of node ID
 * 3. CAS
 * 4. Overflow under debug mode
 */
BEGIN_TEST(MappingTableTest) {
  constexpr size_t size = 1024 * 1024;
  constexpr size_t thread_num = 16;
  using MappingTableType = DefaultMappingTable<char, size>;
  using NodeIDType = typename MappingTableType::NodeIDType;
  MappingTableType *mapping_table = new MappingTableType{};

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

  return;
} END_TEST

int main() {
  MappingTableTest();
  return 0;
}