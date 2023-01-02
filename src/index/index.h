/*
 * Copyright Redis Ltd. 2018 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "../graph/entities/node.h"
#include "../graph/entities/edge.h"
#include "../graph/entities/graph_entity.h"
#include "../graph/graph.h"
#include "redisearch_api.h"
#include "../constraint/constraint.h"

#define INDEX_OK 1
#define INDEX_FAIL 0
#define INDEX_NOT_CHANGED 2
#define INDEX_SEPARATOR '\1'  // can't use '\0', RediSearch will terminate on \0
#define INDEX_FIELD_NONE_INDEXED "NONE_INDEXABLE_FIELDS"

#define INDEX_FIELD_DEFAULT_WEIGHT 1.0
#define INDEX_FIELD_DEFAULT_NOSTEM false
#define INDEX_FIELD_DEFAULT_PHONETIC "no"

// initialize index field with default values
#define IndexField_Default(field, id, name) IndexField_New(field, id, name,  \
		INDEX_FIELD_DEFAULT_WEIGHT, INDEX_FIELD_DEFAULT_NOSTEM,              \
		INDEX_FIELD_DEFAULT_PHONETIC)

// forward declaration
typedef struct _Index _Index;
typedef _Index *Index;

typedef enum {
	IDX_ANY          =  0,
	IDX_EXACT_MATCH  =  1,
	IDX_FULLTEXT     =  2,
} IndexType;

typedef struct {
	EntityID src_id;
	EntityID dest_id;
	EntityID edge_id;
} EdgeIndexKey;

typedef struct {
	char *name;        // field name
	Attribute_ID id;   // field id
	double weight;     // the importance of text
	bool nostem;       // disable stemming of the text
	char *phonetic;    // phonetic search of text
	int ref_count;     // reference count of the field
} IndexField;

// create new index field
void IndexField_New
(
	IndexField *field,    // field to initialize
	Attribute_ID id,      // field id
	const char *name,     // field name
	double weight,        // the importance of text
	bool nostem,          // disable stemming of the text
	const char *phonetic  // phonetic search of text
);

// free index field return true if field was freed
bool IndexField_Free
(
	IndexField *field,  // index field to be freed
	bool enforce_free   // enforce free - skip ref count, used when freeing schema for (optimization)
);

// Increase the reference count of the given index field.
void IndexField_IncRef
(
	IndexField *field // index field to increase reference count
);

// create a new index
Index Index_New
(
	const char *label,           // indexed label
	int label_id,                // indexed label id
	IndexType type,              // exact match or full text
	GraphEntityType entity_type  // entity type been indexed
);

// returns number of pending changes
int Index_PendingChanges
(
	const Index idx  // index to inquery
);

// try to enable index by dropping number of pending changes by 1
// the index is enabled once there are no pending changes
void Index_Enable
(
	Index idx // index to enable
);

// Drop internal RediSearch index
void Index_DropInternalIndex
(
	Index idx  // index to drop
);

// disable index by increasing the number of pending changes
// and re-creating the internal RediSearch index
void Index_Disable
(
	Index idx  // index to disable
);

// returns true if index doesn't contains any pending changes
bool Index_Enabled
(
	const Index idx  // index to get state of
);

// responsible for adding new field to the index structure
void Index_AddFieldToStructure
(
	Index idx,
	IndexField *field
);

// responsible for creating the index structure only!
// e.g. fields, stopwords, language
void Index_ConstructStructure
(
	Index idx
);

// populates index
// returns true if the constraint satisfied
bool Index_Populate_enforce_constraint
(
	Index idx,  // index to populate
	Constraint c, // constraint to enforce
	struct GraphContext *gc    // graph holding entities to index
);

// adds field to index
void Index_AddField
(
	Index idx,         // index modified
	IndexField *field  // field added
);

// removes field from index
// returns true if field was removed
bool Index_RemoveField
(
	Index idx,         // index modified
	const char *field,  // field to remove
	const Constraint constraints, // array of constraints
	bool part_of_constraint_deletion // is this field being deleted as part of a constraint deletion?
);

// index node
void Index_IndexNode
(
	Index idx,     // index to populate
	const Node *n  // node to index
);

// index edge
void Index_IndexEdge
(
	Index idx,     // index to populate
	const Edge *e  // edge to index
);

// remove node from index
void Index_RemoveNode
(
	Index idx,     // index to update
	const Node *n  // node to remove from index
);

// remove edge from index
void Index_RemoveEdge
(
	Index idx,     // index to update
	const Edge *e  // edge to remove from index
);

// query an index
RSResultsIterator *Index_Query
(
	const Index idx,    // index to query
	const char *query,  // query to execute
	char **err          // [optional] report back error
);

// returns internal RediSearch index
RSIndex *Index_RSIndex
(
	const Index idx
);

// returns index type
IndexType Index_Type
(
	const Index idx
);

// returns index graph entity type
GraphEntityType Index_GraphEntityType
(
	const Index idx
);

// returns number of fields indexed
uint Index_FieldsCount
(
	const Index idx  // index to query
);

// returns a shallow copy of indexed fields
const IndexField *Index_GetFields
(
	const Index idx  // index to query
);

// checks if given attribute is indexed
IndexField * Index_getIndexField
(
	const Index idx,           // index to query
	Attribute_ID attribute_id  // attribute id to search
);

// returns indexed label ID
int Index_GetLabelID
(
	const Index idx  // index to query
);

// returns indexed language
const char *Index_GetLanguage
(
	const Index idx  // index to query
);

// returns indexed stopwords
char **Index_GetStopwords
(
	const Index idx,   // index to query
	size_t *size       // number of stopwords
);

// set indexed language
void Index_SetLanguage
(
	Index idx,            // index modified
	const char *language  // language to set
);

// set indexed stopwords
void Index_SetStopwords
(
	Index idx,        // index modified
	char **stopwords  // stopwords
);

// free fulltext index
void Index_Free
(
	Index idx  // index being freed
);

