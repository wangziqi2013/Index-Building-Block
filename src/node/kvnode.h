
/*
 * kvnode.h - This file implements the most general form of fix-sized
 *            key-value container
 */

#pragma once
#ifndef _KVNODE_H
#define _KVNODE_H

#include "common.h"

NAMESPACE_WANGZIQI2013
namespace PROJECT_NAMESPACE {

template <typename KeyType,
          typename ValueType>
class SimpleKVNodeAlloc {
 public:
  void *Alloc(size_t kv_count) {
  }
};

template <typename KeyType,
          typename ValueType>
class 

/*
 * class KVNodeBase - This is the base class of a tree node. 
 *
 * This class does not define any concrete functoinality, and it just provides
 * interfaces.
 */
template <typename _KeyType, 
          typename _ValueType,
          typename _AllocType,
          typename _KeyLessType = std::less<KeyType>,
          typename _KeyEqType = std::equal_to<KeyType>,
          typename _ValueEqType = std::equal_to<ValueType>,
          bool store_pair = true>
class KVNodeBase {
 public:
  // Make them usable by external classes by declaring the 
  // template arguments
  using KeyType = _KeyType;
  using ValueType = _ValueType;
  using AllocType = _AllocType;
  using KeyLessType = _KeyLessType;
  using KeyEqType = _KeyEqType;
  using ValueEqType = _ValueEqType;

 private:
   
};

} // namespace PROJECT_NAMESPACE
NAMESPACE_WANGZIQI2013_END

#endif