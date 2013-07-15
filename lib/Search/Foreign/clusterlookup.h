#ifndef CLUSTERLOOKUP_H
#define CLUSTERLOOKUP_H

#include <stdint.h>
#include <stdlib.h>

typedef struct clustering clustering_t;

typedef struct inverted_index inverted_index_t;

void free_clustering(clustering_t *clustering);

clustering_t *new_clustering(uint16_t *mapping, size_t num_elems);

inverted_index_t *new_inverted_index(clustering_t *clustering, size_t num_terms);

void free_inverted_index(inverted_index_t *index);

void set_docs(inverted_index_t *index, uint32_t term, const uint32_t *docids, size_t num_elems);

size_t intersect(inverted_index_t *index, uint32_t term1, uint32_t term2, uint32_t *out);

size_t list_size(inverted_index_t *index, uint32_t term);

#endif
