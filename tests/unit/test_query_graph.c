/*
* Copyright 2018-2020 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include "acutest.h"
#include "../../src/ast/ast.h"
#include "../../src/util/arr.h"
#include "../../src/query_ctx.h"
#include "../../src/util/rmalloc.h"
#include "../../src/graph/query_graph.h"
#include "../../src/graph/graphcontext.h"

static void _fake_graph_context() {
	TEST_ASSERT(QueryCtx_Init());
	GraphContext *gc = (GraphContext *)calloc(1, sizeof(GraphContext));
	QueryCtx_SetGraphCtx(gc);
}

void compare_nodes(const QGNode *a, const QGNode *b) {
	uint a_lbl_count = QGNode_LabelCount(a);
	uint b_lbl_count = QGNode_LabelCount(b);
	TEST_ASSERT(a_lbl_count == b_lbl_count);
	for(uint i = 0; i < a_lbl_count; i++) {
		TEST_ASSERT(a->labelsID[i] == b->labelsID[i]);
	}
	TEST_ASSERT(array_len(a->incoming_edges) == array_len(b->incoming_edges));
	TEST_ASSERT(array_len(a->outgoing_edges) == array_len(b->outgoing_edges));
}

void compare_edges(const QGEdge *a, const QGEdge *b) {
	TEST_ASSERT(a->alias == b->alias);
	uint relcount = array_len(a->reltypes);
	TEST_ASSERT(relcount == array_len(b->reltypes));
	for(uint i = 0; i < relcount; i ++) {
		TEST_ASSERT(strcmp(a->reltypes[i], b->reltypes[i]) == 0);
		TEST_ASSERT(a->reltypeIDs[i] == b->reltypeIDs[i]);
	}

	compare_nodes(a->src, b->src);
	compare_nodes(a->dest, b->dest);
}

bool contains_node(const QueryGraph *qg, const QGNode *n) {
	uint count = QueryGraph_NodeCount(qg);
	for(uint i = 0; i < count; i ++) {
		if(qg->nodes[i] == n) return true;
	}
	return false;
}

bool contains_edge(const QueryGraph *qg, const QGEdge *e) {
	uint count = QueryGraph_EdgeCount(qg);
	for(uint i = 0; i < count; i ++) {
		if(qg->edges[i] == e) return true;
	}
	return false;
}

QueryGraph *SingleNodeGraph() {
	// Create a single node graph
	// (A)
	size_t node_cap = 1;
	size_t edge_cap = 0;

	// Create nodes.
	QGNode *A = QGNode_New("A");
	QueryGraph *g = QueryGraph_New(node_cap, edge_cap);
	QueryGraph_AddNode(g, A);

	return g;
}

QueryGraph *TriangleGraph() {
	// Create a triangle graph
	// (A)->(B)->(C)->(A)
	size_t node_cap = 3;
	size_t edge_cap = 3;

	// Create nodes.
	const char *relation = "R";

	QGNode *A = QGNode_New("A");
	QGNode *B = QGNode_New("B");
	QGNode *C = QGNode_New("C");

	QGEdge *AB = QGEdge_New(relation, "AB");
	QGEdge *BC = QGEdge_New(relation, "BC");
	QGEdge *CA = QGEdge_New(relation, "CA");

	QueryGraph *g = QueryGraph_New(node_cap, edge_cap);
	QueryGraph_AddNode(g, A);
	QueryGraph_AddNode(g, B);
	QueryGraph_AddNode(g, C);

	QueryGraph_ConnectNodes(g, A, B, AB);
	QueryGraph_ConnectNodes(g, B, C, BC);
	QueryGraph_ConnectNodes(g, C, A, CA);

	return g;
}

QueryGraph *DisjointGraph() {
	// Create a disjoint graph
	// (A)->(B) (C)
	size_t node_cap = 3;
	size_t edge_cap = 1;

	// Create nodes.
	const char *relation = "R";

	QGNode *A = QGNode_New("A");
	QGNode *B = QGNode_New("B");
	QGNode *C = QGNode_New("C");

	QGEdge *AB = QGEdge_New(relation, "AB");

	QueryGraph *g = QueryGraph_New(node_cap, edge_cap);
	QueryGraph_AddNode(g, A);
	QueryGraph_AddNode(g, B);
	QueryGraph_AddNode(g, C);

	QueryGraph_ConnectNodes(g, A, B, AB);

	return g;
}

QueryGraph *SingleNodeCycleGraph() {
	// Create a disjoint graph
	// (A)->(A)
	size_t node_cap = 1;
	size_t edge_cap = 1;

	// Create nodes.
	const char *relation = "R";

	QGNode *A = QGNode_New("A");
	QGEdge *AA = QGEdge_New(relation, "AA");

	QueryGraph *g = QueryGraph_New(node_cap, edge_cap);
	QueryGraph_AddNode(g, A);
	QueryGraph_ConnectNodes(g, A, A, AA);

	return g;
}

void test_QueryGraphClone() {
	Alloc_Reset();
	_fake_graph_context();

	// Create a triangle graph
	// (A)->(B)->(C)->(A)
	size_t node_cap = 3;
	size_t edge_cap = 3;

	// Create nodes.
	const char *relation = "R";

	QGNode *A = QGNode_New("A");
	QGNode *B = QGNode_New("B");
	QGNode *C = QGNode_New("C");

	QGEdge *AB = QGEdge_New(relation, "AB");
	QGEdge *BC = QGEdge_New(relation, "BC");
	QGEdge *CA = QGEdge_New(relation, "CA");

	QueryGraph *g = QueryGraph_New(node_cap, edge_cap);
	QueryGraph_AddNode(g, A);
	QueryGraph_AddNode(g, B);
	QueryGraph_AddNode(g, C);

	QueryGraph_ConnectNodes(g, A, B, AB);
	QueryGraph_ConnectNodes(g, B, C, BC);
	QueryGraph_ConnectNodes(g, C, A, CA);

	QueryGraph *clone = QueryGraph_Clone(g);

	// Validations.
	TEST_ASSERT(QueryGraph_NodeCount(g) == QueryGraph_NodeCount(clone));
	TEST_ASSERT(QueryGraph_EdgeCount(g) == QueryGraph_EdgeCount(clone));

	// Validate nodes.
	for(int i = 0; i < QueryGraph_NodeCount(g); i++) {
		QGNode *a = g->nodes[i];
		QGNode *b = clone->nodes[i];
		compare_nodes(a, b);
	}

	// Validate edges.
	for(int i = 0; i < QueryGraph_EdgeCount(g); i++) {
		QGEdge *a = g->edges[i];
		QGEdge *b = clone->edges[i];
		compare_edges(a, b);
	}

	// Clean up.
	QueryGraph_Free(g);
	QueryGraph_Free(clone);
}


void test_QueryGraphRemoveEntities() {
	Alloc_Reset();
	_fake_graph_context();

	// Create a triangle graph
	// (A)->(B)->(C)->(A)
	size_t node_cap = 3;
	size_t edge_cap = 3;

	// Create nodes.
	const char *relation = "R";

	QGNode *A = QGNode_New("A");
	QGNode *B = QGNode_New("B");
	QGNode *C = QGNode_New("C");

	QGEdge *AB = QGEdge_New(relation, "AB");
	QGEdge *BC = QGEdge_New(relation, "BC");
	QGEdge *CA = QGEdge_New(relation, "CA");

	QueryGraph *g = QueryGraph_New(node_cap, edge_cap);
	QueryGraph_AddNode(g, A);
	QueryGraph_AddNode(g, B);
	QueryGraph_AddNode(g, C);

	QueryGraph_ConnectNodes(g, A, B, AB);
	QueryGraph_ConnectNodes(g, B, C, BC);
	QueryGraph_ConnectNodes(g, C, A, CA);

	// Remove an edge.
	TEST_ASSERT(contains_edge(g, AB));
	TEST_ASSERT(QueryGraph_GetEdgeByAlias(g, "AB") != NULL);

	QueryGraph_RemoveEdge(g, AB);

	TEST_ASSERT(!contains_edge(g, AB));
	TEST_ASSERT(QueryGraph_GetEdgeByAlias(g, "AB") != NULL);

	// Remove node.
	TEST_ASSERT(contains_node(g, C));
	TEST_ASSERT(QueryGraph_GetNodeByAlias(g, "C") != NULL);

	QueryGraph_RemoveNode(g, C);

	TEST_ASSERT(!contains_node(g, C));
	TEST_ASSERT(QueryGraph_GetNodeByAlias(g, "C") == NULL);

	// Both CA BC edges should be removed.
	TEST_ASSERT(!contains_edge(g, CA));
	TEST_ASSERT(QueryGraph_GetEdgeByAlias(g, "CA") == NULL);

	TEST_ASSERT(!contains_edge(g, BC));
	TEST_ASSERT(QueryGraph_GetEdgeByAlias(g, "BC") == NULL);

	/* Assert entity count:
	 * Nodes - A was removed.
	 * Edges - AB explicitly removed, BC and CA implicitly removed. */
	TEST_ASSERT(QueryGraph_NodeCount(g) == 2);
	TEST_ASSERT(QueryGraph_EdgeCount(g) == 0);

	// Assert remaining entities,
	TEST_ASSERT(contains_node(g, A));
	TEST_ASSERT(QueryGraph_GetNodeByAlias(g, "A") != NULL);
	TEST_ASSERT(contains_node(g, B));
	TEST_ASSERT(QueryGraph_GetNodeByAlias(g, "B") != NULL);

	// Clean up.
	QueryGraph_Free(g);
}

void test_QueryGraphConnectedComponents() {
	Alloc_Reset();
	_fake_graph_context();
	
	QueryGraph *g;
	QueryGraph **connected_components;

	//------------------------------------------------------------------------------
	// Single node graph
	//------------------------------------------------------------------------------
	g = SingleNodeGraph();
	connected_components = QueryGraph_ConnectedComponents(g);

	TEST_ASSERT(array_len(connected_components) == 1);

	// Clean up.
	QueryGraph_Free(g);
	for(int i = 0; i < array_len(connected_components); i++) {
		QueryGraph_Free(connected_components[i]);
	}
	array_free(connected_components);

	//------------------------------------------------------------------------------
	// Triangle graph
	//------------------------------------------------------------------------------

	g = TriangleGraph();
	connected_components = QueryGraph_ConnectedComponents(g);

	TEST_ASSERT(array_len(connected_components) == 1);

	// Clean up.
	QueryGraph_Free(g);
	for(int i = 0; i < array_len(connected_components); i++) {
		QueryGraph_Free(connected_components[i]);
	}
	array_free(connected_components);

	//------------------------------------------------------------------------------
	// Disjoint graph
	//------------------------------------------------------------------------------

	g = DisjointGraph();
	connected_components = QueryGraph_ConnectedComponents(g);

	TEST_ASSERT(array_len(connected_components) == 2);

	// Clean up.
	QueryGraph_Free(g);
	for(int i = 0; i < array_len(connected_components); i++) {
		QueryGraph_Free(connected_components[i]);
	}
	array_free(connected_components);

	//------------------------------------------------------------------------------
	// Single node cycle graph
	//------------------------------------------------------------------------------

	g = SingleNodeCycleGraph();
	connected_components = QueryGraph_ConnectedComponents(g);

	TEST_ASSERT(array_len(connected_components) == 1);

	// Clean up.
	QueryGraph_Free(g);
	for(int i = 0; i < array_len(connected_components); i++) {
		QueryGraph_Free(connected_components[i]);
	}
	array_free(connected_components);
}

void test_QueryGraphExtractSubGraph() {
	Alloc_Reset();
	_fake_graph_context();

	//--------------------------------------------------------------------------
	// Construct graph
	//--------------------------------------------------------------------------

	// (A)->(B)->(C)->(D)
	const char *relation = "R";

	QGNode *A = QGNode_New("A");
	QGNode *B = QGNode_New("B");
	QGNode *C = QGNode_New("C");
	QGNode *D = QGNode_New("D");

	QGEdge *AB = QGEdge_New(relation, "AB");
	QGEdge *BC = QGEdge_New(relation, "BC");
	QGEdge *CD = QGEdge_New(relation, "CD");

	uint node_cap = 4;
	uint edge_cap = 3;
	QueryGraph *qg = QueryGraph_New(node_cap, edge_cap);
	QueryGraph_AddNode(qg, A);
	QueryGraph_AddNode(qg, B);
	QueryGraph_AddNode(qg, C);
	QueryGraph_AddNode(qg, D);

	QueryGraph_ConnectNodes(qg, A, B, AB);
	QueryGraph_ConnectNodes(qg, B, C, BC);
	QueryGraph_ConnectNodes(qg, C, A, CD);

	//--------------------------------------------------------------------------
	// Extract portions of the original query graph
	//--------------------------------------------------------------------------

	const char *query =
		"MATCH (A)-[AB]->(B), (B)-[BC]->(C) MATCH (C)-[CD]->(D) MATCH (D)-[DE]->(E) RETURN D";
	cypher_parse_result_t *parse_result = cypher_parse(query, NULL, NULL, CYPHER_PARSE_ONLY_STATEMENTS);
	AST *ast = AST_Build(parse_result);
	ast->referenced_entities = raxNew();

	const cypher_astnode_t **match_clauses = AST_GetClauses(ast, CYPHER_AST_MATCH);
	const cypher_astnode_t *patterns[3];

	// Extract patterns, one per MATCH clause
	patterns[0] = cypher_ast_match_get_pattern(match_clauses[0]);
	patterns[1] = cypher_ast_match_get_pattern(match_clauses[1]);
	patterns[2] = cypher_ast_match_get_pattern(match_clauses[2]);

	// Empty sub graph, as the number of patterns specified is 0.
	QueryGraph *sub = QueryGraph_ExtractPatterns(qg, patterns, 0);

	// Validation, expecting an empty query graph.
	TEST_ASSERT(QueryGraph_NodeCount(sub) == 0);
	TEST_ASSERT(QueryGraph_EdgeCount(sub) == 0);
	QueryGraph_Free(sub);

	// Single pattern, 2 paths, sub graph: a->b->c
	sub = QueryGraph_ExtractPatterns(qg, patterns, 1);
	TEST_ASSERT(QueryGraph_NodeCount(sub) == 3);
	TEST_ASSERT(QueryGraph_EdgeCount(sub) == 2);
	TEST_ASSERT(QueryGraph_GetNodeByAlias(sub, "A") != NULL);
	TEST_ASSERT(QueryGraph_GetNodeByAlias(sub, "B") != NULL);
	TEST_ASSERT(QueryGraph_GetNodeByAlias(sub, "C") != NULL);
	TEST_ASSERT(QueryGraph_GetEdgeByAlias(sub, "AB") != NULL);
	TEST_ASSERT(QueryGraph_GetEdgeByAlias(sub, "BC") != NULL);
	QueryGraph_Free(sub);

	// Multi path sub graph a->b->c->d
	sub = QueryGraph_ExtractPatterns(qg, patterns, 2);
	TEST_ASSERT(QueryGraph_NodeCount(sub) == 4);
	TEST_ASSERT(QueryGraph_EdgeCount(sub) == 3);
	TEST_ASSERT(QueryGraph_GetNodeByAlias(sub, "A") != NULL);
	TEST_ASSERT(QueryGraph_GetNodeByAlias(sub, "B") != NULL);
	TEST_ASSERT(QueryGraph_GetNodeByAlias(sub, "C") != NULL);
	TEST_ASSERT(QueryGraph_GetNodeByAlias(sub, "D") != NULL);
	TEST_ASSERT(QueryGraph_GetEdgeByAlias(sub, "AB") != NULL);
	TEST_ASSERT(QueryGraph_GetEdgeByAlias(sub, "BC") != NULL);
	TEST_ASSERT(QueryGraph_GetEdgeByAlias(sub, "CD") != NULL);
	QueryGraph_Free(sub);

	/* Extract path which is partially contained in 'qg'
	 * d->e where only 'd' is in 'qg' */
	sub = QueryGraph_ExtractPatterns(qg, &patterns[2], 1);
	TEST_ASSERT(QueryGraph_NodeCount(sub) == 2);
	TEST_ASSERT(QueryGraph_EdgeCount(sub) == 1);
	TEST_ASSERT(QueryGraph_GetNodeByAlias(sub, "D") != NULL);
	TEST_ASSERT(QueryGraph_GetNodeByAlias(sub, "E") != NULL);
	TEST_ASSERT(QueryGraph_GetEdgeByAlias(sub, "DE") != NULL);
	QueryGraph_Free(sub);

	// Clean up
	array_free(match_clauses);
	QueryGraph_Free(qg);
	AST_Free(ast);
}

TEST_LIST = {
	{"QueryGraphClone", test_QueryGraphClone},
	{"QueryGraphRemoveEntities", test_QueryGraphRemoveEntities},
	{"QueryGraphConnectedComponents", test_QueryGraphConnectedComponents},
	{"QueryGraphExtractSubGraph", test_QueryGraphExtractSubGraph},
	{NULL, NULL}
};

