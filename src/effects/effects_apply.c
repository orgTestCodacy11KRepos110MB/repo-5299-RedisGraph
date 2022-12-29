/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "RG.h"
#include "effects.h"
#include "../graph/graph_hub.h"

#include <stdio.h>
#include <struct.h>

static EffectType _ReadEffectType
(
	FILE *stream
) {
	EffectType t = EFFECT_UNKNOWN;
	fread_assert(&t, sizeof(EffectType), 1, stream);
	return t;
}

static void _ApplyUpdate
(
	FILE *stream,
	GraphContext *gc
) {
	// format:
	//    entity type node/edge
	//    entity ID
	//    attribute ID
	//    attribute value
	
	Node n;
	SIValue v;
	uint props_set;
	GraphEntityType t;
	uint props_removed;
	Attribute_ID attr_id;

	Graph *g = gc->g;
	EntityID id = INVALID_ENTITY_ID;

	//--------------------------------------------------------------------------
	// read effect data
	//--------------------------------------------------------------------------

	// read entity type
	fread_assert(&t, sizeof(GraphEntityType), 1, stream);

	// read entity ID
	fread_assert(&id, sizeof(EntityID), 1, stream);

	// read attribute ID
	fread_assert(&attr_id, sizeof(Attribute_ID), 1, stream);

	// read attributes
	v = SIValue_FromBinary(stream);

	//--------------------------------------------------------------------------
	// fetch updated entity
	//--------------------------------------------------------------------------

	int res;
	GraphEntity ge;
	if(t == GETYPE_NODE) {
		res = Graph_GetNode(g, id, (Node*)&ge);
	} else {
		res = Graph_GetEdge(g, id, (Edge*)&ge);
	}

	// make sure entity was found
	UNUSED(res);
	ASSERT(res == true);

	//--------------------------------------------------------------------------
	// construct update attribute-set
	//--------------------------------------------------------------------------

	AttributeSet set = AttributeSet_New();
	AttributeSet_Add(&set, attr_id, v);
	UpdateEntityProperties(gc, &ge, set, t, &props_set, &props_removed);

	// clean up
	AttributeSet_Free(&set);
}

static void _ApplyDeleteNode
(
	FILE *stream,
	GraphContext *gc
) {
	// format:
	//    node ID
	
	Node n;
	EntityID id = INVALID_ENTITY_ID;
	Graph *g = gc->g;

	fread_assert(&id, sizeof(EntityID), 1, stream);

	int res = Graph_GetNode(g, id, &n);
	ASSERT(res != 0);

	DeleteNode(gc, &n, false);
}

static void _ApplyDeleteEdge
(
	FILE *stream,
	GraphContext *gc
) {
	// format:
	//    edge ID
	//    relation ID
	//    src ID
	//    dest ID

	Edge e;  // edge to delete
	Node s;  // edge src node
	Node t;  // edge dest node

	EntityID id   = INVALID_ENTITY_ID;
	int      r_id = GRAPH_UNKNOWN_RELATION;
	NodeID   s_id = INVALID_ENTITY_ID;
	NodeID   t_id = INVALID_ENTITY_ID;

	int res;
	UNUSED(res);

	Graph *g = gc->g;

	// read edge ID
	fread_assert(&id, fldsiz(UndoDeleteEdgeOp, id), 1, stream);

	// read relation ID
	fread_assert(&r_id, fldsiz(UndoDeleteEdgeOp, relationID), 1, stream);

	// read src node ID
	fread_assert(&s_id, fldsiz(UndoDeleteEdgeOp, srcNodeID), 1, stream);

	// read dest node ID
	fread_assert(&t_id, fldsiz(UndoDeleteEdgeOp, destNodeID), 1, stream);

	// get src node, dest node and edge from the graph
	res = Graph_GetNode(g, s_id, (Node*)&s);
	ASSERT(res != 0);
	res = Graph_GetNode(g, t_id, (Node*)&t);
	ASSERT(res != 0);
	res = Graph_GetEdge(g, id, (Edge*)&e);
	ASSERT(res != 0);

	// set edge relation, src and destination node
	Edge_SetSrcNode(&e, &s);
	Edge_SetDestNode(&e, &t);
	Edge_SetRelationID(&e, r_id);

	// delete edge
	DeleteEdge(gc, &e, false);
}

void Effects_Apply
(
	GraphContext *gc,
	const char *effects_buff,
	size_t l
) {
	ASSERT(l > 0);
	ASSERT(effects_buff != NULL);
	
	FILE *stream = fmemopen((void*)effects_buff, l, "r");
	
	while(ftell(stream) < l) {
		// read effect type
		EffectType t = _ReadEffectType(stream);
		switch(t) {
			case EFFECT_DELETE_NODE:
				_ApplyDeleteNode(stream, gc);
				break;
			case EFFECT_DELETE_EDGE:
				_ApplyDeleteEdge(stream, gc);
				break;
			case EFFECT_UPDATE:
				_ApplyUpdate(stream, gc);
				break;
			default:
				//assert(false && "unknown undo operation");
				//return false;
				break;
		}
	}

	fclose(stream);
}

