/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "../value.h"

// initialize a new SIValue array type with given capacity
SIValue SIArray_New
(
	u_int32_t initialCapacity
);

// appends a new SIValue to a given array
void SIArray_Append
(
	SIValue *siarray,  // pointer to array
	SIValue value      // value to add
);

// returns a volatile copy of the SIValue from an array in a given index
// if index is out of bound, SI_NullVal is returned
// caller is expected either to not free the returned value or take ownership on
// its own copy
SIValue SIArray_Get
(
	SIValue siarray,
	u_int32_t index
);

// returns the array length
u_int32_t SIArray_Length
(
	SIValue siarray
);

// returns true if any of the types in 't' are contained in the array
// or its nested array children, if any
// returns a boolean indicating whether any types were matched
bool SIArray_ContainsType
(
	SIValue siarray,  // array to inspect
	SIType t          // bitmap of types to search for
);

// returns true if all of the elements in the array are of type 't'
//a boolean indicating whether all elements are of type 't'
bool SIArray_AllOfType
(
	SIValue siarray,  // array to inspect
	SIType t          // type to compare
);

// returns a copy of the array, caller needs to free the array
SIValue SIArray_Clone
(
	SIValue siarray
);

// prints an array into a given buffer
void SIArray_ToString
(
	SIValue list,         // array to print
	char **buf,           // print buffer
	size_t *bufferLen,    // buffer length
	size_t *bytesWritten  // actual number of bytes written to buffer
);

// returns the array hash code
XXH64_hash_t SIArray_HashCode
(
	SIValue siarray
);

// returns the number of bytes required for a binary representation of `v`
size_t SIArray_BinarySize
(
	const SIValue *arr  // array
);

// writes a binary representation of `v` into `stream`
void SIArray_ToBinary
(
	FILE *stream,       // stream to populate
	const SIValue *arr  // array
);

// creates an array from its binary representation
// this is the reverse of SIArray_ToBinary
// x = SIArray_FromBinary(SIArray_ToBinary(y));
// x == y
SIValue SIArray_FromBinary
(
	FILE *stream  // stream containing binary representation of an array
);

// delete an array
void SIArray_Free
(
	SIValue siarray
);

