#ifndef LOOKUP_H
#define LOOKUP_H

#include <stdlib.h>
#include <stdint.h>

struct lookup;
typedef struct lookup lookup_t;

lookup_t *lookup_new_set(const uint32_t *elems, size_t num_elems);
void lookup_destroy(lookup_t *set);

size_t lookup_set_size(const lookup_t *set);
size_t lookup_intersect(const lookup_t *set1, const lookup_t *set2, uint32_t *out);

#endif
