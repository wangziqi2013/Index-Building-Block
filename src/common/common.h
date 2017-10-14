
/*
 * common.h - This file defines the most common environmental variables
 *            and declarations.
 *
 * Since this file is included by almost every source files, please be cautious 
 * when changing this file. Your change may have unintended affect if not 
 * treated seriously. Please obey the following rules when you add things into 
 * this file:
 *   - No variable definitions. Put them into a CPP file and link them properly
 *   - No macro definition. Put them into common-macro.h
 *   - This file does not have a name space, which means all names are global
 *     To avoid possible name collisions, it is advised to add special prefixs
 *     or suffixs for global names
 *   - If you are not sure, please be reasonable and conservative
 *   - All system library file inclusion should be put into this file.
 *   - All testing utility should go to test-util.h or cpp
 */

// The following are C library inclusions
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cctype>
#include <cstdint>

// The following are C++ STL inclusions
#include <vector>

// Common macro definition
#include "common-macro.h"