/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <stdio.h>
#include "effects.h"

static EffectType _ReadEffectType
(
	FILE *stream
) {
	EffectType t;
	ssize_t n = fread((void*)&t, sizeof(EffectType), 1, stream);

	if(n != sizeof(EffectType)) {
		// short read!
		assert("short read" && false);
	}

	return t;
}

static bool _ApplyDeleteNode
(
	FILE *stream
	GraphContext *gc,
) {
	EntityID id;
	ssize_t n = fread((void*)id, sizeof(EntityID), 1, stream);
	if(n != sizeof(EffectType)) {
		// short read!
		assert("short read" && false);
	}

	Node n;
	int res = Graph_GetNode(g, id, Node &n);
	ASSERT(res != 0);

	DeleteNode(gc, n);

	return true;
}

void Effects_Apply
(
	GraphContext *gc,
	char *effects_buff,
	size_t l
) {
	ASSERT(l > 0);
	ASSERT(effects_buff != NULL);

	EffectType t;	
	
	FILE *stream = fmemopen(effects_buff, l, "r");
	
	while(l > 0) {
		// read effect type
		t = _ReadEffectType(stream);
		switch(t) {
			case EFFECT_DELETE_NODE:
				_ApplyDeleteNode(stream, gc);
				break;
			default:
				//assert(false && "unknown undo operation");
				//return false;
				break;
		}
	}

	fclose(stream);
}

