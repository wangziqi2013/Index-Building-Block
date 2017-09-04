
/*
 * kvnode.h - This file implements the most general form of fix-sized
 *            key-value container
 */

#pragma once
#ifndef _KVNODE_H
#define _KVNODE_H

#include "common.h"

OPTIONAL_NAMESPACE_WANGZIQI2013
namespace PROJECT_NAMESPACE {

/*
 * class KVNode - This is the base class of a tree node. 
 *
 * This class does not define any concrete functoinality, and it just provides
 * interfaces.
 */
template <typename KeyType, 
          typename ValueType, 
          typename KeyLess, 
          typename KeyEq,
          typename Extra>
class KVNode {

};

} // namespace PROJECT_NAMESPACE
NAMESPACE_WANGZIQI2013_END

#endif