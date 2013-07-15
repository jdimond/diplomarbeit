#include "lookup.h"

#include <string.h>
#include <math.h>

#include "utils.h"

#define MAX(a,b) ((a > b) ? a : b)
#define RANGE_BITS(range) ((int)ceil(log2(MAX(1,range))))
/* At least one bit per bucket is necessary, or right shifts will fail */
#define BUCKET_BITS(range,bucketsize,len) \
    (MAX(1, RANGE_BITS(range)-(int)ceil(log2(range*(double)bucketsize/len))))
#define SHIFT_SIZE(bits, range, uint32_t) (RANGE_BITS(range) - bits)
#define NUM_BUCKETS(bits, range, uint32_t) (1L<<bits)

#define BUCKET_SIZE 16

#define BUCKET_LENGTH(s,i) (s->offsets[i+1]-s->offsets[i])

struct lookup {
    size_t length;
    uint8_t bits;
    uint32_t *offsets;
    uint32_t range;
    uint32_t *elems;
};


size_t lookup_set_size(const lookup_t *set) {
    return set->length;
}

lookup_t *lookup_new_set(const uint32_t *elems, size_t num_elems) {
    int i;

    uint32_t range = 0;

    int shift_size;
    size_t real_num_elems;
    size_t num_buckets;
    size_t cur_index;
    size_t last_index;

    for (i = 0; i < num_elems; i++) {
        range = MAX(elems[i], range);
    }

    lookup_t *set = malloc(sizeof(lookup_t));
    if (set == NULL) {
        return NULL;
    }
    set->elems = malloc(sizeof(uint32_t)*num_elems);
    if (set->elems == NULL) {
        free(set);
        return NULL;
    }
    memcpy(set->elems, elems, sizeof(uint32_t)*num_elems);

    qsort(set->elems, num_elems, sizeof(uint32_t), &compare_uint32_t);

    real_num_elems = (num_elems > 0) ? 1 : 0;
    for (i = 1; i < num_elems; i++) {
        if (set->elems[i] != set->elems[real_num_elems-1]) {
            set->elems[real_num_elems++] = set->elems[i];
        }
    }

    if (real_num_elems < num_elems) {
        set->elems = realloc(set->elems, sizeof(uint32_t)*real_num_elems);
    }

    set->length = real_num_elems;
    set->bits = BUCKET_BITS(range, BUCKET_SIZE, set->length);
    set->range = range;
    shift_size = SHIFT_SIZE(set->bits, set->range, uint32_t);

    num_buckets = NUM_BUCKETS(set->bits, set->range, uint32_t);
    set->offsets = malloc(sizeof(uint32_t)*(num_buckets+1));
    if (set->offsets == NULL) {
        free(set->elems);
        free(set);
        return NULL;
    }

    cur_index = 0;
    last_index = 0;
    for (i = 0; i < num_buckets; i++) {
        while (cur_index < set->length && set->elems[cur_index] >> shift_size == i) {
            cur_index++;
        }
        set->offsets[i] = last_index;
        last_index = cur_index;
    }

    set->offsets[num_buckets] = set->length;

    return set;
}

void lookup_destroy(lookup_t *set) {
    free(set->offsets);
    free(set->elems);
    free(set);
}

size_t lookup_intersect(const lookup_t *set1,
                        const lookup_t *set2,
                        uint32_t *out) {
    size_t o1 = 0;
    size_t o2 = 0;
    uint32_t i2 = 0;
    int bucket = -1;
    size_t bucket_length = -1;

    int shift_size;
    int num_buckets2;
    uint32_t *elems2 = NULL;

    size_t output_count = 0;

    if (set1->length > set2->length) {
        const lookup_t *tmp = set1;
        set1 = set2;
        set2 = tmp;
    }

    shift_size = SHIFT_SIZE(set2->bits, set2->range, uint32_t);
    num_buckets2 = NUM_BUCKETS(set2->bits, set2->range, uint32_t);

    for (o1 = 0; o1 < set1->length; o1++) {
        uint32_t i1 = set1->elems[o1];
        int newbucket = i1 >> shift_size;
        if (newbucket != bucket) {
            if (newbucket >= num_buckets2) {
                goto end;
            }
            bucket = newbucket;
            bucket_length = BUCKET_LENGTH(set2, bucket);
            if (bucket_length == 0) {
                bucket = -1;
                continue;
            }
            elems2 = set2->elems + set2->offsets[bucket];
            o2 = 0;
            i2 = elems2[o2];
        }
        while (o2 < bucket_length-1 && i2 < i1) {
            o2++;
            i2 = elems2[o2];
        }
        if (i1 == i2) {
            out[output_count] = i1;
            output_count++;
        }
    }
end:

    return output_count;
}
