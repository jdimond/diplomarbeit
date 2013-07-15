#ifndef SEARCH_H
#define SEARCH_H

#include <stdint.h>

typedef uint32_t term_t;

typedef uint64_t tfreq_t;

typedef struct doc_collection doc_collection_t;

typedef struct document doc_t;

doc_collection_t *new_collection();
void free_collection(doc_collection_t *coll);
void add_to_collection(doc_collection_t *coll, uint32_t doclen, term_t *terms);
uint32_t num_docs_in_collection(doc_collection_t *coll);
doc_t *get_docs_in_collection(doc_collection_t *coll);

#define DEBUG 1

#endif
