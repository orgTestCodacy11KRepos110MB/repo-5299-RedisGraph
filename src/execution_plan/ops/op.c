/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "op.h"
#include "RG.h"
#include "../../util/rmalloc.h"
#include "../../util/simple_timer.h"

/* Forward declarations */
Record ExecutionPlan_BorrowRecord(struct ExecutionPlan *plan);
rax *ExecutionPlan_GetMappings(const struct ExecutionPlan *plan);
void ExecutionPlan_ReturnRecord(struct ExecutionPlan *plan, Record r);

OpDesc _op_descs[36];

void OpDesc_Register
(
	OPType type,
	const char *name,
	fpInit init,
	fpReset reset,
	fpToString toString,
	fpClone clone,
	fpFree free,
	bool writer
) {
	OpDesc *desc = _op_descs + type;
	desc->type     = type;
	desc->name     = name;
	desc->writer   = writer;
	desc->init     = init;
	desc->reset    = reset;
	desc->clone    = clone;
	desc->free     = free;
	desc->toString = toString;
}

OpDesc *OpDesc_Get
(
	OPType type
) {
	return _op_descs + type;
}

void OpBase_Init
(
	OpBase *op,
	OPType type,
	fpConsume consume,
	const struct ExecutionPlan *plan
) {
	op->desc       = _op_descs + type;
	op->plan       = plan;
	op->stats      = NULL;
	op->parent     = NULL;
	op->childCount = 0;
	op->children   = NULL;
	op->parent     = NULL;
	op->stats      = NULL;
	op->modifies   = NULL;
	op->op_initialized = false;

	// function pointers
	op->consume = consume;
	op->profile = NULL;
}

inline Record OpBase_Consume
(
	OpBase *op
) {
	return op->consume(op);
}

int OpBase_Modifies
(
	OpBase *op,
	const char *alias
) {
	if(!op->modifies) op->modifies = array_new(const char *, 1);
	array_append(op->modifies, alias);

	// make sure alias has an entry associated with it
	// within the record mapping
	rax *mapping = ExecutionPlan_GetMappings(op->plan);

	void *id = raxFind(mapping, (unsigned char *)alias, strlen(alias));
	if(id == raxNotFound) {
		id = (void *)raxSize(mapping);
		raxInsert(mapping, (unsigned char *)alias, strlen(alias), id, NULL);
	}

	return (intptr_t)id;
}

int OpBase_AliasModifier
(
	OpBase *op,
	const char *modifier,
	const char *alias
) {
	rax *mapping = ExecutionPlan_GetMappings(op->plan);
	void *id = raxFind(mapping, (unsigned char *)modifier, strlen(modifier));
	ASSERT(id != raxNotFound);

	// make sure to not introduce the same modifier twice
	if(raxInsert(mapping, (unsigned char *)alias, strlen(alias), id, NULL)) {
		array_append(op->modifies, alias);
	}

	return (intptr_t)id;
}

bool OpBase_ChildrenAware
(
	OpBase *op,
	const char *alias,
	int *idx
) {
	for (int i = 0; i < op->childCount; i++) {
		OpBase *child = op->children[i];
		if(op->plan == child->plan && child->modifies != NULL) {
			uint count = array_len(child->modifies);
			for (uint i = 0; i < count; i++) {
				if(strcmp(alias, child->modifies[i]) == 0) {
					if(idx) {
						rax *mapping = ExecutionPlan_GetMappings(op->plan);
						void *rec_idx = raxFind(mapping, (unsigned char *)alias, strlen(alias));
						*idx = (intptr_t)rec_idx;
					}
					return true;				
				}
			}
		}
		if(OpBase_ChildrenAware(child, alias, idx)) return true;
	}
	
	return false;
}

bool OpBase_Aware
(
	OpBase *op,
	const char *alias,
	int *idx
) {
	rax *mapping = ExecutionPlan_GetMappings(op->plan);
	void *rec_idx = raxFind(mapping, (unsigned char *)alias, strlen(alias));
	if(idx) *idx = (intptr_t)rec_idx;
	return (rec_idx != raxNotFound);
}

void OpBase_PropagateReset
(
	OpBase *op
) {
	if(op->desc->reset) {
		OpResult res = op->desc->reset(op);
		ASSERT(res == OP_OK);
	}
	for(int i = 0; i < op->childCount; i++) OpBase_PropagateReset(op->children[i]);
}

static void _OpBase_StatsToString
(
	const OpBase *op,
	sds *buff
) {
	*buff = sdscatprintf(*buff,
					" | Records produced: %d, Execution time: %f ms",
					op->stats->profileRecordCount,
					op->stats->profileExecTime);
}

void OpBase_ToString
(
	const OpBase *op,
	sds *buff
) {
	int bytes_written = 0;

	if(op->desc->toString) op->desc->toString(op, buff);
	else *buff = sdscatprintf(*buff, "%s", op->desc->name);

	if(op->stats) _OpBase_StatsToString(op, buff);
}

Record OpBase_Profile
(
	OpBase *op
) {
	double tic [2];
	// Start timer.
	simple_tic(tic);
	Record r = op->profile(op);
	// Stop timer and accumulate.
	op->stats->profileExecTime += simple_toc(tic);
	if(r) op->stats->profileRecordCount++;
	return r;
}

bool OpBase_IsWriter
(
	OpBase *op
) {
	return op->desc->writer;
}

void OpBase_UpdateConsume
(
	OpBase *op,
	fpConsume consume
) {
	ASSERT(op != NULL);
	// if Operation is profiled, update profiled function
	// otherwise update consume function
	if(op->profile != NULL) op->profile = consume;
	else op->consume = consume;
}

inline Record OpBase_CreateRecord
(
	const OpBase *op
) {
	return ExecutionPlan_BorrowRecord((struct ExecutionPlan *)op->plan);
}

Record OpBase_CloneRecord
(
	Record r
) {
	Record clone = ExecutionPlan_BorrowRecord((struct ExecutionPlan *)r->owner);
	Record_Clone(r, clone);
	return clone;
}

Record OpBase_DeepCloneRecord
(
	Record r
) {
	Record clone = ExecutionPlan_BorrowRecord((struct ExecutionPlan *)r->owner);
	Record_DeepClone(r, clone);
	return clone;
}

inline OPType OpBase_Type
(
	const OpBase *op
) {
	ASSERT(op != NULL);
	return op->desc->type;
}

inline void OpBase_DeleteRecord
(
	Record r
) {
	ExecutionPlan_ReturnRecord(r->owner, r);
}

OpBase *OpBase_Clone
(
	const struct ExecutionPlan *plan,
	const OpBase *op
) {
	ASSERT(op        != NULL);
	ASSERT(plan      != NULL);
	ASSERT(op->desc->clone != NULL);
	
	return op->desc->clone(plan, op);
}

void OpBase_Free
(
	OpBase *op
) {
	// free internal operation
	if(op->desc->free) op->desc->free(op);
	if(op->children) rm_free(op->children);
	if(op->modifies) array_free(op->modifies);
	if(op->stats) rm_free(op->stats);
	rm_free(op);
}

