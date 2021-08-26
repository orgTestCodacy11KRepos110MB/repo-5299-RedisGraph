/*
* Copyright 2018-2021 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#pragma once

#include "op.h"
#include "op_argument.h"
#include "../execution_plan.h"
#include "../../resultset/resultset_statistics.h"

/* The Merge operation accepts exactly one path in the query and attempts to match it.
 * If the path is not found, it will be created, making new instances of every path variable
 * not bound in an earlier clause in the query. */
typedef struct {
	OpBase op;                          // Base op.
	OpBase *match_stream;               // Child stream that attempts to resolve the pattern.
	OpBase *create_stream;              // Child stream that will create the pattern if not found.
	OpBase *bound_variable_stream;      // Optional child stream to resolve previously bound variables.
	Argument *match_argument_tap;       // Argument tap to populate Match stream with bound variables.
	Argument *create_argument_tap;      // Argument tap to populate Create stream with bound variables.
	rax *on_match;                      // Updates to be performed on a successful match.
	rax *on_create;                     // Updates to be performed on creation.
} OpMerge;

OpBase *NewMergeOp(const ExecutionPlan *plan, rax *on_match, rax *on_create);
