/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "RG.h"
#include "effects.h"
#include "../undo_log/undo_log.h"

#include <struct.h>

//bool _EffectFromUndoNodeCreate
//(
//	char **buffer,
//	uint buffer_size,
//	const UndoCreateOp *undo_op
//) {
//	// format:
//	//    effect type
//	//    node ID
//	//    number of labels
//	//    lables
//	//    number of attributes
//	//    attributes
//	
//	Node *n = &undo_op->n;
//	
//	effect->create.id = ENTITY_GET_ID(n);
//	memcpy(*buffer, 
//
//	uint lbl_count = 0;
//	NODE_GET_LABELS(g, n, lbl_count);
//
//	effect->create.n_labels = lbl_count;
//	effect->create.labels = 
//	effect->create.attrs = 
//}

//bool _EffectFromUndoEdgeCreate
//(
//	Effect *effect,
//	UndoCreateOp *undo_op
//) {
//
//}

static void _EffectFromUndoNodeDelete
(
	FILE *stream,     // effects stream
	const UndoOp *op  // undo operation to capture
) {
	// format:
	//    effect type
	//    node ID
	
	const UndoDeleteNodeOp *_op = (const UndoDeleteNodeOp*)op;

	EffectType t = EFFECT_DELETE_NODE;

	// write effect type
	fwrite_assert(&t, sizeof(EffectType), 1, stream); 

	// write node ID
	fwrite_assert(&_op->id, sizeof(EntityID), 1, stream); 
}

static void _EffectFromUndoEdgeDelete
(
	FILE *stream,     // effects stream
	const UndoOp *op  // undo operation to capture
) {
	// format:
	//    effect type
	//    edge ID
	//    relation ID
	//    src ID
	//    dest ID

	const UndoDeleteEdgeOp *_op = (const UndoDeleteEdgeOp*)op;

	EffectType t = EFFECT_DELETE_EDGE;

	// write effect type
	fwrite_assert(&t, sizeof(t), 1, stream); 

	// write edge ID
	fwrite_assert(&_op->id, sizeof(_op->id), 1, stream); 

	// write relation ID
	fwrite_assert(&_op->relationID, sizeof(_op->relationID), 1, stream); 

	// write src ID
	fwrite_assert(&_op->srcNodeID, sizeof(_op->srcNodeID), 1, stream); 

	// write dest ID
	fwrite_assert(&_op->destNodeID, sizeof(_op->destNodeID), 1, stream); 
}

static void _EffectFromUndoUpdate
(
	FILE *stream,     // effects stream
	const UndoOp *op  // undo operation to capture
) {
	// format:
	//    effect type
	//    entity type node/edge
	//    entity ID
	//    attribute ID
	//    attribute value
	
	const UndoUpdateOp *_op = (const UndoUpdateOp*)op;

	GraphEntity *e = (_op->entity_type == GETYPE_NODE) ?
		(GraphEntity*)&_op->n : (GraphEntity*)&_op->e;

	// write effect type
	EffectType t = EFFECT_UPDATE;
	fwrite_assert(&t, sizeof(EffectType), 1, stream); 

	// write entity type
	fwrite_assert(&_op->entity_type, sizeof(_op->entity_type), 1, stream);

	// write entity ID
	fwrite_assert(&ENTITY_GET_ID(e), sizeof(EntityID), 1, stream);

	// write attribute ID
	fwrite_assert(&_op->attr_id, sizeof(_op->attr_id), 1, stream);

	// write attribute value and advance buffer
	SIValue *v = GraphEntity_GetProperty(e, _op->attr_id);
	SIValue_ToBinary(stream, v);
}

static void _EffectFromUndoOp
(
	FILE *stream,     // effects stream
	const UndoOp *op  // undo op to convert into an effect
) {
	ASSERT(op     != NULL);
	ASSERT(stream != NULL);

	switch(op->type) {
		case UNDO_DELETE_NODE:
			_EffectFromUndoNodeDelete(stream, op);
			break;
		case UNDO_DELETE_EDGE:
			_EffectFromUndoEdgeDelete(stream, op);
			break;
		case UNDO_UPDATE:
			_EffectFromUndoUpdate(stream, op);
			break;
		default:
			//assert(false && "unknown undo operation");
			//return false;
			break;
		//case UNDO_CREATE_NODE:
		//	_EffectFromUndoNodeCreate(effect, &op->create_op);
		//	break;
		//case UNDO_CREATE_EDGE:
		//	_EffectFromUndoEdgeCreate(effect, op);
		//	break;
		//case UNDO_SET_LABELS:
		//	_EffectFromUndoSetLabels(effect, op);
		//	break;
		//case UNDO_REMOVE_LABELS:
		//	_EffectFromUndoRemoveLabels(effect, op);
		//	break;
		//case UNDO_ADD_SCHEMA:
		//	_EffectFromUndoSchemaAdd(effect, op);
		//	break;
		//case UNDO_ADD_ATTRIBUTE:
		//	_EffectFromUndoAttrAdd(effect, op);
		//	break;
	}
}

size_t _ComputeUpdateSize
(
	const UndoOp *op
) {
	// format:
	//    effect type
	//    entity type node/edge
	//    entity ID
	//    attribute ID
	//    attribute value
	
	const UndoUpdateOp *_op = (const UndoUpdateOp*)op;

	GraphEntity *e = (_op->entity_type == GETYPE_NODE) ?
		(GraphEntity*)&_op->n : (GraphEntity*)&_op->e;

	SIValue *v = GraphEntity_GetProperty(e, _op->attr_id);

	size_t s = sizeof(EffectType)                +
		       fldsiz(UndoUpdateOp, entity_type) +
			   sizeof(EntityID)                  +
			   fldsiz(UndoUpdateOp, attr_id)     +
			   SIValue_BinarySize(v);

	return s;
}

size_t _ComputeBufferSize
(
	const UndoLog undolog
) {
	size_t s = 0;
	uint n = UndoLog_Length(undolog);

	for(uint i = 0; i < n; i++) {
		const UndoOp *op = undolog + i;
		switch(op->type) {
			case UNDO_DELETE_NODE:
				s += sizeof(EffectType) +
					 fldsiz(UndoDeleteNodeOp, id);
				break;
			case UNDO_DELETE_EDGE:
				s += sizeof(EffectType)                   +
					 fldsiz(UndoDeleteEdgeOp, id)         +
					 fldsiz(UndoDeleteEdgeOp, relationID) +
					 fldsiz(UndoDeleteEdgeOp, srcNodeID)  +
					 fldsiz(UndoDeleteEdgeOp, destNodeID);
				break;
			case UNDO_UPDATE:
				s += _ComputeUpdateSize(op);
				break;
			default:
				//assert(false && "unknown undo operation");
				break;
				//case UNDO_CREATE_NODE:
				//	_EffectFromUndoNodeCreate(effect, &undo_op->create_op);
				//	break;
				//case UNDO_CREATE_EDGE:
				//	_EffectFromUndoEdgeCreate(effect, undo_op);
				//	break;
				//case UNDO_SET_LABELS:
				//	_EffectFromUndoSetLabels(effect, undo_op);
				//	break;
				//case UNDO_REMOVE_LABELS:
				//	_EffectFromUndoRemoveLabels(effect, undo_op);
				//	break;
				//case UNDO_ADD_SCHEMA:
				//	_EffectFromUndoSchemaAdd(effect, undo_op);
				//	break;
				//case UNDO_ADD_ATTRIBUTE:
				//	_EffectFromUndoAttrAdd(effect, undo_op);
				//	break;
		}
	}

	return s;
}

// create a list of effects from the undo-log
u_char *Effects_FromUndoLog
(
	UndoLog log,
	size_t *len
) {
	ASSERT(log != NULL);

	uint n = UndoLog_Length(log);
	ASSERT(n > 0);

	//--------------------------------------------------------------------------
	// determine required buffer size
	//--------------------------------------------------------------------------

	size_t size = _ComputeBufferSize(log);
	if(size == 0) {
		return NULL;
	}

	u_char *buffer = rm_malloc(sizeof(u_char) * size);
	FILE *stream = fmemopen(buffer, size, "w");

	//--------------------------------------------------------------------------
	// encode effects
	//--------------------------------------------------------------------------

	for(uint i = 0; i < n; i++) {
		UndoOp *op = log + i;

		// convert undo-op to effect
		_EffectFromUndoOp(stream, op);

		// free undo-op
		UndoLog_FreeOp(op);
	}

	// we should have reached end of buffer
	ASSERT(ftell(stream) == size);

	// close stream
	fclose(stream);

	// clear undo-log
	UndoLog_Clear(log);

	// set output length and return buffer
	*len = size;
	return buffer;
}

