#ifndef __UTILS_H
#define __UTILS_H

#include <stdint.h>

#define max(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _a : _b; })

#define min(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a < _b ? _a : _b; })

/* Random in range [min, max) */
int random_in_range(unsigned int min, unsigned int max);

void randomize_uint32_array(uint32_t* arr, int len, int places);

uint32_t min_index_offset(double* arr, uint32_t len, uint32_t offset);

int compare_uint16_t(const void *a, const void *b);
int compare_uint32_t(const void *a, const void *b);
int compare_terms(const void *a, const void *b);


#endif
