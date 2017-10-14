
/*
 * binary-util.h - This file contains binary utilities that manipulates, 
 *                 modifies and prints binary data in a flaxible way
 */

#pragma once
#ifndef _BINARY_UTIL_H
#define _BINARY_UTIL_H

#include "common.h"

namespace wangziqi2013 {
namespace index_building_block {

/*
 * class BitSequence - This class defines an abstraction of a bit sequence
 *                     that allows the user to view, modify and print any
 *                     sub-sequence within this bit sequence
 * 
 * This data structure stores any raw bit array as an array of opaque types,
 * and interpret it using small-endian
 */
class BitSequence {
 private:
  // 0 if not initialized
  size_t length;
  size_t capacity;
  // nullptr if not initialized
  uint8_t *data_p;

 public:
  /*
   * BitSequence() - Empty construction
   */
  BitSequence() : length{0UL},
                  capacity{0UL},
                  data_p{nullptr} {
    return;
  }

  /*
   * BitSequence() - Raw type construction
   */
  BitSequence(size_t p_length, 
              const void *p_data_p) : length{p_length},
                                      capacity{ALLOC_SIZE(p_length)},
                                      data_p{new uint8_t[ALLOC_SIZE(p_length)]} {
    memcpy(data_p, p_data_p, capacity);
    return;
  }

  /*
   * BitSequence() - Types construction; using a typed variable to construct
   * 
   * Note that although we do not check whether data is a POD type, but it 
   * should be, because we just shadowly copy the content of the data using
   * the length of its indicated type, and treat it as a binary sequence.
   * 
   * Pointers in T are safe, because we do not dereference any pointers in it.
   */
  template <typename T>
  BitSequence(const T &data) : BitSequence(sizeof(T) * 8, &data) {
    return;
  }

  /*
   * BitSequence() - Copy constructor
   */
  BitSequence(const BitSequence &other) : length{other.length},
                                          capacity{other.capacity},
                                          data_p{new uint8_t[other.capacity]} {
    memcpy(data_p, other.data_p, capacity);
    return;
  }

  /*
   * BitSequence() - Move constructor; clears the source
   */
  BitSequence(BitSequence &&other) : length{other.length},
                                     capacity{other.capacity},
                                     data_p{other.data_p} {
    other.data_p = nullptr;
    other.length = 0UL;
    other.capacity = 0UL;
    return;
  }

  BitSequence &operator=(const BitSequence &other);
  BitSequence &operator=(BitSequence &&other);

  /*
   * ~BitSequence() - Deletes the data array if it is not nullptr
   */
  ~BitSequence() {
    if(data_p != nullptr) {
      delete[] data_p;
    }

    return;
  }

  /*
   * BYTE_OFFSET() - Returns the byte offset of a given bit offset
   */
  static inline size_t BYTE_OFFSET(size_t bit_offset) {
    return bit_offset >> 3;
  }

  /*
   * BIT_OFFSET() - The bit offset inside a byte
   */
  static inline size_t BIT_OFFSET(size_t bit_offset) {
    return bit_offset % 8;
  }

  /*
   * ALLOC_SIZE() - Number of bytes to allocate given the bit length
   */
  static inline size_t ALLOC_SIZE(size_t length) {
    return (length + 7) / 8;
  }

  /*
   * UNUSED_BITS() - Returns the number of unused bits in the last byte
   */
  static inline size_t UNUSED_BITS(size_t length, size_t alignment_unit) {
    return (alignment_unit - (length % alignment_unit)) % alignment_unit;
  }

  // Get a zeroed-out sequence with certain size
  void Make(size_t new_size);
  // Set/Get a single bit
  bool SetBit(size_t pos, bool value);
  bool GetBit(size_t pos) const;
  // Set a range within 64 bits
  bool SetRange(size_t range_start, size_t range_end, uint64_t value);
  // Set a range using arbitraty binary data
  void SetRange(size_t range_start, size_t range_end, const void *range_data_p);
  // Print the sequence from MSB to LSB. We print a white space after every
  // "group" digits, and prints a new line after every "line" digits
  void Print(int group=8, int line=32) const;
  // Print the title of the bit sequence
  static void PrintTitle(int group=8, int line=32);

  bool operator==(const BitSequence &other) const;
};

} // namespace index_building_block
} // namespace wangziqi2013

#endif