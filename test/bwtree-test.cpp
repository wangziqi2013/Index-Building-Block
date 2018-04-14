
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
  constexpr size_t size = 64;
  using MappingTableType = DefaultMappingTable<char, size>;
  using NodeIDType = typename MappingTableType::NodeIDType;
  

  MappingTableType mapping_table;
  char *p = reinterpret_cast<char *>(0x0);
  for(size_t i = 0;i < size;i++) {
    NodeIDType node_id = mapping_table.AllocateNodeID(p);
    p++;
    always_assert(node_id == i);
  }

  return;
} END_TEST

int main() {
  MappingTableTest();
  return 0;
}