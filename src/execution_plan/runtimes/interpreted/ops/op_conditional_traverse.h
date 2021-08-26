/*
* Copyright 2018-2021 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#pragma once

#include "op.h"
#include "../runtime_execution_plan.h"
#include "../../../ops/shared/traverse_functions.h"
#include "../../../../graph/rg_matrix/rg_matrix_iter.h"
#include "../../../../arithmetic/algebraic_expression.h"
#include "../../../../../deps/GraphBLAS/Include/GraphBLAS.h"

// OP Traverse
typedef struct {
	RT_OpBase op;
	Graph *graph;
	AlgebraicExpression *ae;
	RG_Matrix F;                // Filter matrix.
	RG_Matrix M;                // Algebraic expression result.
	int dest_label_id;          // ID of destination node label if known.
	const char *dest_label;     // Label of destination node if known.
	EdgeTraverseCtx *edge_ctx;  // Edge collection data if the edge needs to be set.
	GxB_MatrixTupleIter *iter;  // Iterator over M.
	uint srcNodeIdx;            // Source node index into record.
	uint destNodeIdx;           // Destination node index into record.
	uint record_count;          // Number of held records.
	uint record_cap;            // Max number of records to process.
	Record *records;            // Array of records.
	Record r;                   // Currently selected record.
} RT_OpCondTraverse;

// Creates a new Traverse operation
RT_OpBase *RT_NewCondTraverseOp(const RT_ExecutionPlan *plan, AlgebraicExpression *ae, int dest_label_id, const char *dest_label);
