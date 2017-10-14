
#pragma once
#ifndef _BINARY_UTIL_H
#define _BINARY_UTIL_H

/*
 * class BitSequence - This class defines an abstraction of a bit sequence
 *                     that allows the user to view, modify and print any
 *                     sub-sequence within this bit sequence
 */
class BitSequence {
 private:
  // 0 if not initialized
  size_t length;
  // nullptr if not initialized
  uint8_t *data_p;

 public:
  /*
   * BitSequence() - Empty construction
   */
  BitSequence() : length{0UL},
                  data_p{nullptr} {
    return;
  }

  /*
   * BitSequence() - Raw type construction
   */
  BitSequence(size_t p_length, void *p_data_p) : length{p_length},
                                                 data_p{new uint8_t[p_length]} {
    memcpy(data_p, p_data_p, length);
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
  BitSequence(const T &data) : BitSequence(sizeof(T), &data)
    return;
  }

  /*
   * ~BitSequence() - Deletes the data array if it is not nullptr
   */
  ~BitSequence() {
    if(data_p != nullptr) {
      delete[] data_p;
    }

    return;
  }
};

#endif