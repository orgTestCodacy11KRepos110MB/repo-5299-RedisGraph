/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "RG.h"
#include "effects.h"
#include "../query_ctx.h"
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

// convert UndoNodeDelete into a NodeDelete effect
static void EffectFromUndoNodeDelete
(
	FILE *stream,     // effects stream
	const UndoOp *op  // undo operation to convert
) {
	// effect format:
	//    effect type
	//    node ID
	
	// undo operation
	const UndoDeleteNodeOp *_op = (const UndoDeleteNodeOp*)op;

	// effect type
	EffectType t = EFFECT_DELETE_NODE;

	// write effect type
	fwrite_assert(&t, sizeof(t), stream); 

	// write node ID
	fwrite_assert(&_op->id, sizeof(EntityID), stream); 
}

// convert UndoEdgeDelete into a EdgeDelete effect
static void EffectFromUndoEdgeDelete
(
	FILE *stream,     // effects stream
	const UndoOp *op  // undo operation to convert
) {
	// effect format:
	//    effect type
	//    edge ID
	//    relation ID
	//    src ID
	//    dest ID

	// undo operation
	const UndoDeleteEdgeOp *_op = (const UndoDeleteEdgeOp*)op;

	// effect type
	EffectType t = EFFECT_DELETE_EDGE;

	// write effect type
	fwrite_assert(&t, sizeof(t), stream); 

	// write edge ID
	fwrite_assert(&_op->id, sizeof(_op->id), stream); 

	// write relation ID
	fwrite_assert(&_op->relationID, sizeof(_op->relationID), stream); 

	// write edge src node ID
	fwrite_assert(&_op->srcNodeID, sizeof(_op->srcNodeID), stream); 

	// write edge dest node ID
	fwrite_assert(&_op->destNodeID, sizeof(_op->destNodeID), stream); 
}

// convert UndoAttrAdd into a AddAttr effect
static void EffectFromUndoAttrAdd
(
	FILE *stream,     // effects stream
	const UndoOp *op  // undo operation to convert
) {
	//--------------------------------------------------------------------------
	// effect format:
	// effect type
	// attribute name
	//--------------------------------------------------------------------------

	const UndoAddAttributeOp *_op = (const UndoAddAttributeOp*)op;

	//--------------------------------------------------------------------------
	// write effect type
	//--------------------------------------------------------------------------

	EffectType t = EFFECT_ADD_ATTRIBUTE;
	fwrite_assert(&et, sizeof(EffectType), stream); 

	//--------------------------------------------------------------------------
	// write attribute name
	//--------------------------------------------------------------------------

	GraphContext *gc = QueryCtx_GetGraphCtx();
	Attribute_ID attr_id = _op->attribute_id;
	const char *attr_name = GraphContext_GetAttributeString(gc, attr_id);
	fwrite_assert(attr_name, strlen(attr_name), stream); 
}

// convert UndoCreateOp into a Create effect
static void EffectFromUndoEntityCreate
(
	FILE *stream,       // effects stream
	const UndoOp *op,   // undo operation to convert
	GraphEntityType t   // type of entity created (node/edge)
) {
	//--------------------------------------------------------------------------
	// effect format:
	// effect type
	// label count
	// labels
	// attribute count
	// attributes (id,value) pair
	//--------------------------------------------------------------------------
	
	Graph *g = QueryCtx_GetGraph();
	GraphContext *gc = QueryCtx_GetGraphCtx();
	const UndoCreateOp *_op = (const UndoCreateOp*)op;
	GraphEntity *e = (t == GETYPE_NODE) ? (GraphEntity*)&_op->n :
		(GraphEntity*)&_op->e;

	//--------------------------------------------------------------------------
	// write effect type
	//--------------------------------------------------------------------------

	EffectType et = (t == GETYPE_NODE) ? EFFECT_CREATE_NODE : EFFECT_CREATE_EDGE;
	fwrite_assert(&et, sizeof(EffectType), stream); 

	//--------------------------------------------------------------------------
	// write labels / relationship type
	//--------------------------------------------------------------------------

	uint lbl_count;

	if(t == GETYPE_NODE) {
		// node
		NODE_GET_LABELS(g, (Node*)e, lbl_count);
		fwrite_assert(&lbl_count, sizeof(lbl_count), stream);

		// write labels
		for(uint i = 0; i < lbl_count; i++) {
			fwrite_assert(labels + i, sizeof(LabelID), stream);
		}
	} else {
		// edge
		lbl_count = 1;  // default number of relationship types
		fwrite_assert(&lbl_count, sizeof(lbl_count), stream);

		int rel_id = Edge_GetRelationID((Edge*)e);
		fwrite_assert(&rel_id, sizeof(int), stream);
	}

	//--------------------------------------------------------------------------
	// write attribute count
	//--------------------------------------------------------------------------

	const AttributeSet attrs = GraphEntity_GetAttributes(e);
	ushort attr_count = ATTRIBUTE_SET_COUNT(attrs);
	fwrite_assert(&attr_count, sizeof(attr_count), stream);

	// write attributes
	for(ushort i = 0; i < attr_count; i++) {
		// get current attribute name and value
		Attribute_ID attr_id;
		SIValue attr = AttributeSet_GetIdx(attrs, i, &attr_id);

		// write attribute ID
		fwrite_assert(&attr_id, sizeof(Attribute_ID), stream);

		// write attribute value
		SIValue_ToBinary(stream, &attr);
	}
}

// convert UndoUpdate into a Update effect
static void EffectFromUndoUpdate
(
	FILE *stream,     // effects stream
	const UndoOp *op  // undo operation to convert
) {
	// effect format:
	//    effect type
	//    entity type node/edge
	//    entity ID
	//    attribute id
	//    attribute value
	
	// undo operation
	const UndoUpdateOp *_op = (const UndoUpdateOp*)op;

	// entity type node/edge
	GraphEntity *e = (_op->entity_type == GETYPE_NODE) ?
		(GraphEntity*)&_op->n : (GraphEntity*)&_op->e;

	// write effect type
	EffectType t = EFFECT_UPDATE;
	fwrite_assert(&t, sizeof(EffectType), stream); 

	// write entity type
	fwrite_assert(&_op->entity_type, sizeof(_op->entity_type), stream);

	// write entity ID
	fwrite_assert(&ENTITY_GET_ID(e), sizeof(EntityID), stream);

	// write attribute ID
	GraphContext *gc = QueryCtx_GetGraphCtx();
	fwrite_assert(&_op->attr_id, sizeof(Attribute_ID), stream);

	// write attribute value
	SIValue *v = GraphEntity_GetProperty(e, _op->attr_id);
	SIValue_ToBinary(stream, v);
}

// convert undo-operation into an effect
// and write effect to stream
static void EffectFromUndoOp
(
	FILE *stream,     // effects stream
	const UndoOp *op  // undo op to convert into an effect
) {
	// validations
	ASSERT(op     != NULL);  // undo-op can't be NULL
	ASSERT(stream != NULL);  // effects stream can't be NULL

	// encode undo-op as an effect
	switch(op->type) {
		case UNDO_DELETE_NODE:
			EffectFromUndoNodeDelete(stream, op);
			break;
		case UNDO_DELETE_EDGE:
			EffectFromUndoEdgeDelete(stream, op);
			break;
		case UNDO_UPDATE:
			EffectFromUndoUpdate(stream, op);
			break;
		case UNDO_CREATE_NODE:
			EffectFromUndoEntityCreate(stream, op, GETYPE_NODE);
			break;
		case UNDO_CREATE_EDGE:
			EffectFromUndoEntityCreate(stream, op, GETYPE_EDGE);
			break;
		default:
			assert(false && "unknown effect");
			break;
		//case UNDO_SET_LABELS:
		//	_EffectFromUndoSetLabels(effect, op);
		//	break;
		//case UNDO_REMOVE_LABELS:
		//	_EffectFromUndoRemoveLabels(effect, op);
		//	break;
		//case UNDO_ADD_SCHEMA:
		//	_EffectFromUndoSchemaAdd(effect, op);
		//	break;
		case UNDO_ADD_ATTRIBUTE:
			EffectFromUndoAttrAdd(effect, op);
			break;
	}
}

static size_t ComputeCreateSize
(
	const UndoOp *op,
	GraphEntityType t
) {
	//--------------------------------------------------------------------------
	// effect format:
	// effect type
	// label count
	// labels
	// attribute count
	// attributes (id,value) pair
	//--------------------------------------------------------------------------

	// undo operation
	Graph *g = QueryCtx_GetGraph();
	const UndoCreateOp *_op = (const UndoCreateOp*)op;

	// entity-type node/edge
	GraphEntity *e = (t == GETYPE_NODE) ? (GraphEntity*)&_op->n :
		(GraphEntity*)&_op->e;
	
	// get number of labels
	uint lbl_count = 1;
	if(t == GETYPE_NODE) {
		NODE_GET_LABELS(g, (Node*)e, lbl_count);
	}

	// get number of attributes
	const AttributeSet attrs = GraphEntity_GetAttributes(e);
	ushort attr_count = ATTRIBUTE_SET_COUNT(attrs);

	//--------------------------------------------------------------------------
	// compute effect size
	//--------------------------------------------------------------------------

	size_t s = sizeof(EffectType)                 +  // effect type
		       sizeof(uint)                       +  // label count
			   lbl_count * sizeof(LabelID)        +  // labels
			   sizeof(ushort)                     +  // attribute count
			   attr_count * sizeof(Attribute_ID);    // attribute IDs

	// compute attribute-set size
	for(ushort i = 0; i < attr_count; i++) {
		// compute attribute size
		Attribute_ID attr_id;
		SIValue attr = AttributeSet_GetIdx(attrs, i, &attr_id);
		s += SIValue_BinarySize(&attr);  // attribute value
	}

	return s;
}

// compute required update-effect size for undo-update operation
static size_t ComputeUpdateSize
(
	const UndoOp *op
) {
	//--------------------------------------------------------------------------
	// effect format:
	//    effect type
	//    entity type node/edge
	//    entity ID
	//    attribute name
	//    attribute value
	//--------------------------------------------------------------------------
	
	// undo operation
	const UndoUpdateOp *_op = (const UndoUpdateOp*)op;

	// entity-type node/edge
	GraphEntity *e = (_op->entity_type == GETYPE_NODE) ?
		(GraphEntity*)&_op->n : (GraphEntity*)&_op->e;

	
	// get updated value
	SIValue *v = GraphEntity_GetProperty(e, _op->attr_id);

	// compute effect byte size
	size_t s = sizeof(EffectType)                +  // effect type
		       fldsiz(UndoUpdateOp, entity_type) +  // entity type
			   sizeof(EntityID)                  +  // entity ID
			   sizeof(Attribute_ID)              +  // attribute ID
			   SIValue_BinarySize(v);               // attribute value

	return s;
}

// compute required effects buffer byte size from undo-log
static size_t ComputeBufferSize
(
	const UndoLog undolog
) {
	size_t s = 0;  // effects-buffer required size in bytes
	uint n = UndoLog_Length(undolog);  // number of undo entries

	// compute effect size from each undo operation
	for(uint i = 0; i < n; i++) {
		const UndoOp *op = undolog + i;
		switch(op->type) {
			case UNDO_DELETE_NODE:
				// DeleteNode effect size
				s += sizeof(EffectType) +
					 fldsiz(UndoDeleteNodeOp, id);
				break;
			case UNDO_DELETE_EDGE:
				// DeleteEdge effect size
				s += sizeof(EffectType)                   +
					 fldsiz(UndoDeleteEdgeOp, id)         +
					 fldsiz(UndoDeleteEdgeOp, relationID) +
					 fldsiz(UndoDeleteEdgeOp, srcNodeID)  +
					 fldsiz(UndoDeleteEdgeOp, destNodeID);
				break;
			case UNDO_UPDATE:
				// Update effect size
				s += ComputeUpdateSize(op);
				break;
			case UNDO_CREATE_NODE:
				s += ComputeCreateSize(op, GETYPE_NODE);
				break;
			case UNDO_CREATE_EDGE:
				s += ComputeCreateSize(op, GETYPE_EDGE);
				break;
			default:
				assert(false && "unknown undo operation");
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
	UndoLog log,  // undo-log to convert into effects buffer
	size_t *len   // size of generated effects buffer
) {
	// validations
	ASSERT(log != NULL);  // undo-log can't be NULL

	// expecting at least one undo operation
	uint n = UndoLog_Length(log);
	ASSERT(n > 0);

	//--------------------------------------------------------------------------
	// determine required effects buffer size
	//--------------------------------------------------------------------------

	size_t buff_size = ComputeBufferSize(log);
	if(buff_size == 0) {
		return NULL;
	}

	// allocate effects buffer and treat it as a stream
	u_char *buffer = rm_malloc(sizeof(u_char) * buff_size);
	FILE *stream = fmemopen(buffer, buff_size, "w");

	//--------------------------------------------------------------------------
	// encode effects
	//--------------------------------------------------------------------------

	for(uint i = 0; i < n; i++) {
		UndoOp *op = log + i;

		// convert undo-op into an effect and write it to stream
		EffectFromUndoOp(stream, op);

		// free undo-op
		UndoLog_FreeOp(op);
	}

	// we should have reached end of buffer
	ASSERT(ftell(stream) == buff_size);

	// close stream
	fclose(stream);

	// clear undo-log
	UndoLog_Clear(log);

	// set output length and return buffer
	*len = buff_size;
	return buffer;
}

