#include <stdlib.h>

#include "utils.h"
#include "search.h"

int random_in_range(unsigned int min, unsigned int max) {
    int base_random = rand(); /* in [0, RAND_MAX] */
    if (RAND_MAX == base_random) return random_in_range(min, max);
    /* now guaranteed to be in [0, RAND_MAX) */
    int range       = max - min,
        remainder   = RAND_MAX % range,
        bucket      = RAND_MAX / range;
    /* There are range buckets, plus one smaller interval
       within remainder of RAND_MAX */
    if (base_random < RAND_MAX - remainder) {
        return min + base_random/bucket;
    } else {
        return random_in_range (min, max);
    }
}

void randomize_uint32_array(uint32_t* arr, int len, int places) {
    int i;
    if (places > len || places < 0) {
        places = len-1;
    }

    for (i = 0; i < places; i++) {
        int j = random_in_range(i,len);
        uint32_t t = arr[i];
        arr[i] = arr[j];
        arr[j] = t;
    }
}

uint32_t min_index_offset(double* arr, uint32_t len, uint32_t offset) {
    int i;
    int minindex = offset;
    double min = arr[offset];
    for (i = 1; i < len; i++) {
        int index = (i+offset)%len;
        double val = arr[index];
        if (val<min) {
            min = val;
            minindex = index;
        }
    }
    return minindex;
}

int compare_uint16_t(const void *a, const void *b) {
    return *(uint16_t*)a - *(uint16_t*)b;
}

int compare_uint32_t(const void *a, const void *b) {
    return *(uint32_t*)a - *(uint32_t*)b;
}

int compare_terms(const void *a, const void *b) {
    return *(term_t*)a - *(term_t*)b;
}
