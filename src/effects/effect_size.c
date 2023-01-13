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
		       sizeof(ushort)                     +  // label count
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

// compute required update-effect size for undo-add-attribute operation
static size_t ComputeAttrAddSize
(
	const UndoOp *op
) {
	//--------------------------------------------------------------------------
	// effect format:
	// effect type
	// attribute name
	//--------------------------------------------------------------------------

	GraphContext *gc = QueryCtx_GetGraphCtx();
	const UndoAddAttributeOp *_op = (const UndoAddAttributeOp*)op;

	// get added attribute name
	Attribute_ID attr_id =  _op->attribute_id;
	const char *attr_name = GraphContext_GetAttributeString(gc, attr_id);

	// compute effect byte size
	size_t s = sizeof(EffectType) + strlen(attr_name);

	return s;
}

// compute required update-effect size for undo-set-labels operation
static size_t ComputeSetLabelSize
(
	const UndoOp *op
) {
	//--------------------------------------------------------------------------
	// effect format:
	//    effect type
	//    node ID
	//    labels count
	//    label IDs
	//--------------------------------------------------------------------------
	
	const UndoLabelsOp *_op = (const UndoLabelsOp*)op;

	size_t s = sizeof(EffectType) +
		       sizeof(EntityID)   +
		       sizeof(ushort)     +
			   sizeof(LabelID) * _op->labels_count;

	return s;
}

// compute required update-effect size for undo-remove-labels operation
static size_t ComputeRemoveLabelSize
(
	const UndoOp *op
) {
	//--------------------------------------------------------------------------
	// effect format:
	//    effect type
	//    node ID
	//    labels count
	//    label IDs
	//--------------------------------------------------------------------------
	
	// both remove labels and set labels share the same format
	return ComputeSetLabelSize(op);
}

static size_t ComputeSchemaAddSize
(
	const UndoOp *op
) {
	//--------------------------------------------------------------------------
	// effect format:
	//    effect type
	//    schema type
	//    schema name
	//--------------------------------------------------------------------------
	
	GraphContext *gc = QueryCtx_GetGraphCtx();
	const UndoAddSchemaOp *_op = (const UndoAddSchemaOp *)op;
	Schema *schema = GraphContext_GetSchemaByID(gc, _op->schema_id, _op->t);
	ASSERT(schema != NULL);

	size_t s = sizeof(EffectType) +
		       sizeof(SchemaType) +
			   strlen(Schema_GetName(schema));

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
size_t ComputeBufferSize
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
			case UNDO_ADD_ATTRIBUTE:
				s += ComputeAttrAddSize(op);
				break;
			case UNDO_SET_LABELS:
				s += ComputeSetLabelSize(op);
				break;
			case UNDO_REMOVE_LABELS:
				s += ComputeRemoveLabelSize(op);
				break;
			case UNDO_ADD_SCHEMA:
				s += ComputeSchemaAddSize(op);
				break;
			default:
				assert(false && "unknown undo operation");
				break;
		}
	}

	return s;
}

