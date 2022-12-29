/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "RG.h"
#include "../effects/effects.h"
#include "../util/simple_timer.h"
#include "../graph/graphcontext.h"

int Graph_Effect
(
	RedisModuleCtx *ctx,
	RedisModuleString **argv,
	int argc
) {
	double tic[2];
	simple_tic(tic);

	// GRAPH.EFFECT <key> <effects>
	if(argc != 3) {
		return RedisModule_WrongArity(ctx);
	}

	// get graph context
	GraphContext *gc = GraphContext_Retrieve(ctx, argv[1], false, true);
	ASSERT(gc != NULL);

	//--------------------------------------------------------------------------
	// process effects
	//--------------------------------------------------------------------------

	size_t l = 0;  // effects buffer length
	const char *effects_buff = RedisModule_StringPtrLen(argv[2], &l);

	Effects_Apply(gc, effects_buff, l);

	// release the GraphContext
	GraphContext_DecreaseRefCount(gc);

	// DEBUG report time
	printf("effect execution time: %f milliseconds\n", simple_toc(tic) * 1000);

	return REDISMODULE_OK;
}

