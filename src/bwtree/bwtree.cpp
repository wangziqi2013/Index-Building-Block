
#include "bwtree.h"

namespace wangziqi2013 {
namespace index_building_block {
namespace bwtree {

template <typename KeyType>
template <typename DeltaChainType>
ExtendedNodeBase<KeyType, DeltaChainType> *NodeBase<KeyType>::GetBase() { 
  return reinterpret_cast<ExtendedNodeBase<KeyType, DeltaChainType> *>(
    reinterpret_cast<char *>(GetLowKey()) - ExtendedNodeBase<KeyType, DeltaChainType>::LOW_KEY_OFFSET); 
}

} // namespace bwtree
} // namespace index_building_block
} // namespace wangziqi2013