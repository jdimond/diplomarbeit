#include "clusterlookup.h"

#include <string.h>
#include <time.h>
#include <math.h>
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>

#include "utils.h"

#define MAX(a,b) ((a > b) ? a : b)
#define RANGE_BITS(range) ((int)ceil(log2(MAX(1,range))))
#define BUCKET_BITS(range,bucketsize,len) \
    (MAX(1, RANGE_BITS(range)-(int)ceil(log2(range*(double)bucketsize/len))))
#define SHIFT_SIZE(bits, range) (RANGE_BITS(range) - bits)
#define NUM_BUCKETS(bits, range) (1L<<bits)
#define BUCKET_LENGTH(set, i) (set->offsets[i+1] - set->offsets[i])

typedef struct {
    uint32_t length;
    uint32_t *offsets;
    uint32_t *ids;
} lookup_docids_t;

typedef struct {
    uint32_t cluster_size;
    uint32_t *mapping;
} cluster_t;

struct clustering {
    int num_clusters;
    uint16_t *clusterid_mapping;
    uint32_t *docid_mapping;
    cluster_t *clusters;
};

typedef struct {
    uint16_t clusterid;
    lookup_docids_t lookup;
} cluster_elem_t;

typedef struct {
    uint16_t length;
    uint32_t num_docs;
    uint16_t *offsets;
    cluster_elem_t *elems;
} lookup_clusters_t;

struct inverted_index {
    size_t num_terms;
    clustering_t *clustering;
    lookup_clusters_t *lookups;
};

#define IDS_BUCKET_SIZE 16

void set_lookup_docids(lookup_docids_t *set, cluster_t *cluster, uint32_t *elems, size_t num_elems) {
    int i;
    int shift_size;
    size_t num_buckets;
    size_t cur_index;
    size_t last_index;

    uint32_t range = cluster->cluster_size;

    set->ids = elems;

    set->length = num_elems;
    unsigned int bits = BUCKET_BITS(range, IDS_BUCKET_SIZE, set->length);
    shift_size = SHIFT_SIZE(bits, range);

    num_buckets = NUM_BUCKETS(bits, range);
    set->offsets = malloc(sizeof(uint32_t)*(num_buckets + 1));
    assert(set->offsets); /* TODO: proper error handling */

    cur_index = 0;
    last_index = 0;
    for (i = 0; i < num_buckets; i++) {
        while (cur_index < set->length && set->ids[cur_index] >> shift_size == i) {
            cur_index++;
        }
        set->offsets[i] = last_index;

        last_index = cur_index;
    }
    set->offsets[num_buckets] = num_elems;
}

void free_lookup_docids(lookup_docids_t *set) {
    free(set->offsets);
}

#define CLUSTERS_BUCKET_SIZE 8

void set_lookup_clusters(lookup_clusters_t *set, int total_clusters, cluster_elem_t *elems, size_t num_elems) {
    int i;
    int shift_size;
    size_t num_buckets;
    size_t cur_index;
    size_t last_index;

    set->elems = elems;

    set->length = num_elems;
    unsigned int bits = BUCKET_BITS(total_clusters, CLUSTERS_BUCKET_SIZE, set->length);
    shift_size = SHIFT_SIZE(bits, total_clusters);

    num_buckets = NUM_BUCKETS(bits, total_clusters);
    set->offsets = malloc(sizeof(uint16_t)*(num_buckets + 1));
    assert(set->offsets); /* TODO: proper error handling */

    cur_index = 0;
    last_index = 0;
    for (i = 0; i < num_buckets; i++) {
        while (cur_index < set->length && set->elems[cur_index].clusterid >> shift_size == i) {
            cur_index++;
        }
        set->offsets[i] = last_index;

        last_index = cur_index;
    }
    set->offsets[num_buckets] = num_elems;
}

inline size_t intersect_lookup(const lookup_docids_t *set1,
                               const lookup_docids_t *set2,
                               cluster_t *cluster,
                               uint32_t *out) {
    size_t o1 = 0;
    size_t o2 = 0;
    unsigned int i2 = 0;
    int bucket = -1;
    size_t bucket_length = -1;

    uint32_t *elems2 = NULL;

    size_t output_count = 0;

    if (set1->length > set2->length) {
        const lookup_docids_t *tmp = set1;
        set1 = set2;
        set2 = tmp;
    }

    unsigned int bits = BUCKET_BITS(cluster->cluster_size, IDS_BUCKET_SIZE, set2->length);

    int shift_size = SHIFT_SIZE(bits, cluster->cluster_size);
    int num_buckets2 = NUM_BUCKETS(bits, cluster->cluster_size);

    for (o1 = 0; o1 < set1->length; o1++) {
        unsigned int i1 = set1->ids[o1];
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
            elems2 = set2->ids + set2->offsets[bucket];
            o2 = 0;
            i2 = elems2[o2];
        }
        while (o2 < bucket_length-1 && i2 < i1) {
            o2++;
            i2 = elems2[o2];
        }
        if (i1 == i2) {
            out[output_count] = cluster->mapping[i1];
            output_count++;
        }
    }
end:
    return output_count;
}

size_t intersect_lookup_clusters(const lookup_clusters_t *set1,
                                 const lookup_clusters_t *set2,
                                 const clustering_t *clustering,
                                 uint32_t *out) {
    size_t o1 = 0;
    size_t o2 = 0;
    unsigned int i2 = 0;
    int bucket = -1;
    size_t bucket_length = -1;

    cluster_elem_t *elems2 = NULL;

    size_t output_count = 0;

    if (set1->length > set2->length) {
        const lookup_clusters_t *tmp = set1;
        set1 = set2;
        set2 = tmp;
    }

    unsigned int bits = BUCKET_BITS(clustering->num_clusters, CLUSTERS_BUCKET_SIZE, set2->length);

    int shift_size = SHIFT_SIZE(bits, clustering->num_clusters);
    int num_buckets2 = NUM_BUCKETS(bits, clustering->num_clusters);

    for (o1 = 0; o1 < set1->length; o1++) {
        unsigned int i1 = set1->elems[o1].clusterid;
        int newbucket = i1 >> shift_size;
        if (newbucket != bucket) {
            if (newbucket >= num_buckets2) {
                goto end2;
            }
            bucket = newbucket;
            bucket_length = BUCKET_LENGTH(set2, bucket);
            if (bucket_length == 0) {
                bucket = -1;
                continue;
            }
            elems2 = set2->elems + set2->offsets[bucket];
            o2 = 0;
            i2 = elems2[o2].clusterid;
        }
        while (o2 < bucket_length-1 && i2 < i1) {
            o2++;
            i2 = elems2[o2].clusterid;
        }
        if (i1 == i2) {
            output_count += intersect_lookup(&set1->elems[o1].lookup, &elems2[o2].lookup, &clustering->clusters[i1], out + output_count);
        }
    }
end2:
    return output_count;
}

size_t intersect(inverted_index_t *index, uint32_t term1, uint32_t term2, uint32_t *out) {
    return intersect_lookup_clusters(&index->lookups[term1], &index->lookups[term2], index->clustering, out);
}

int compare_clusters(const void *elem1, const void *elem2, void *arg) {
    uint32_t d1 = *((uint32_t *)elem1);
    uint32_t d2 = *((uint32_t *)elem2);
    uint16_t *clusters = (uint16_t *)arg;

    return clusters[d1] - clusters[d2];
}

int cluster_lookup_create(lookup_clusters_t *lookup, const uint32_t *orig_elems, size_t num_elems, const clustering_t *clustering) {
    uint32_t *elems = NULL;
    uint32_t *ids = NULL;
    cluster_elem_t *lookup_elems = NULL;
    int i;

    if (num_elems == 0) {
        memset(lookup,0,sizeof(lookup_clusters_t));
        return 0;
    }

    elems = malloc(sizeof(uint32_t)*num_elems);
    ids = malloc(sizeof(uint32_t)*num_elems);
    if (elems == NULL || ids == NULL) {
        goto cleanup;
    }

    memcpy(elems, orig_elems, sizeof(uint32_t)*num_elems);

    /* sort elements according to clusters */
    qsort_r(elems, num_elems, sizeof(uint32_t), &compare_clusters, clustering->clusterid_mapping);

    /* count number of non_empty clusters */
    int last_cluster = -1;
    int num_clusters = 0;
    for (i = 0; i < num_elems; i++) {
        unsigned int cluster = clustering->clusterid_mapping[elems[i]];
        if (cluster != last_cluster) {
            last_cluster = cluster;
            num_clusters++;
        }
    }

    lookup_elems = malloc(sizeof(cluster_elem_t)*num_clusters);
    if (lookup_elems == NULL) {
        goto cleanup;
    }

    for (i = 0; i < num_elems; i++) {
        ids[i] = clustering->docid_mapping[elems[i]];
    }

    int cluster_count = 0;
    i = 0;
    while (i < num_elems) {
        unsigned int cluster = clustering->clusterid_mapping[elems[i]];

        /* find beginning of next cluster */
        int next_cluster_pos = i+1;
        while (next_cluster_pos < num_elems && clustering->clusterid_mapping[elems[next_cluster_pos]] == cluster) {
            next_cluster_pos++;
        }
        int length = next_cluster_pos - i;
        lookup_elems[cluster_count].clusterid = cluster;
        qsort(ids + i, length, sizeof(uint32_t), &compare_uint32_t);
        set_lookup_docids(&lookup_elems[cluster_count].lookup, &clustering->clusters[cluster], ids + i, length);
        cluster_count++;
        i = next_cluster_pos;
    }

    assert(cluster_count == num_clusters);

    set_lookup_clusters(lookup, clustering->num_clusters, lookup_elems, cluster_count);
    lookup->num_docs = num_elems;

    free(elems);

    return 0;

cleanup:
    free(elems);
    free(ids);
    free(lookup_elems);
    return 1;
}

void free_lookup_clusters(lookup_clusters_t *lookup) {
    int i;
    if (lookup == NULL || lookup->length == 0) {
        return;
    }
    free(lookup->elems[0].lookup.ids);
    for (i = 0; i < lookup->length; i++) {
        free_lookup_docids(&lookup->elems[i].lookup);
    }
    free(lookup->elems);
    free(lookup->offsets);
}

clustering_t *new_clustering(uint16_t *mapping, size_t num_elems) {
    int i;

    clustering_t *clustering = malloc(sizeof(clustering_t));
    if (clustering == NULL) {
        return NULL;
    }

    memset(clustering, 0, sizeof(clustering_t));

    /* find number of clusters */
    uint16_t num_clusters = 0;
    for (i = 0; i < num_elems ; i++) {
        num_clusters = MAX(mapping[i], num_clusters);
    }
    num_clusters++;

    clustering->num_clusters = num_clusters;
    clustering->clusterid_mapping = calloc(num_elems, sizeof(uint16_t));
    clustering->docid_mapping = calloc(num_elems, sizeof(uint32_t));
    clustering->clusters = calloc(num_clusters, sizeof(cluster_t));

    if (clustering->clusterid_mapping == NULL
        || clustering->docid_mapping == NULL
        || clustering->clusters == NULL) {
        free_clustering(clustering);
        return NULL;
    }

    memcpy(clustering->clusterid_mapping, mapping, sizeof(uint16_t)*num_elems);

    uint32_t *docids = malloc(sizeof(uint32_t)*num_elems);
    if (docids == NULL) {
        free_clustering(clustering);
        return NULL;
    }

    for (i = 0; i < num_elems; i++) {
        docids[i] = i;
    }

    qsort_r(docids, num_elems, sizeof(uint32_t), &compare_clusters, clustering->clusterid_mapping);

    int cid = 0;
    i = 0;
    while (i < num_elems) {
        unsigned int cluster = clustering->clusterid_mapping[docids[i]];

        /* find beginning of next cluster */
        int j = i;
        while (j < num_elems && clustering->clusterid_mapping[docids[j]] == cluster) {
            clustering->docid_mapping[docids[j]] = j-i;
            j++;
        }
        clustering->clusters[cid].cluster_size = j - i;
        clustering->clusters[cid].mapping = docids + i;

        cid++;
        i = j;
    }

    return clustering;
}

size_t list_size(inverted_index_t *index, uint32_t term) {
    return index->lookups[term].num_docs;
}

void free_clustering(clustering_t *clustering) {
    if (clustering == NULL) {
        return;
    }
    free(clustering->clusterid_mapping);
    free(clustering->docid_mapping);
    if (clustering->clusters != NULL) {
        /* Only free the first one, as it is a contigous region */
        free(clustering->clusters[0].mapping);
    }
    free(clustering->clusters);
    free(clustering);
}

void set_docs(inverted_index_t *index, uint32_t term, const uint32_t *docids, size_t num_elems) {
    int result;

    result = cluster_lookup_create(&index->lookups[term], docids, num_elems, index->clustering);

    assert(!result);
}

inverted_index_t *new_inverted_index(clustering_t *clustering, size_t num_terms) {
     inverted_index_t *index = calloc(1, sizeof(inverted_index_t));
     if (index == NULL) {
         return NULL;
     }

     index->num_terms = num_terms;
     index->clustering = clustering;

     index->lookups = calloc(num_terms, sizeof(lookup_clusters_t));
     if (index->lookups == NULL) {
         free(index);
         return NULL;
     }

     return index;
}

void free_inverted_index(inverted_index_t *index) {
    int i;
    for (i = 0; i < index->num_terms; i++) {
        free_lookup_clusters(&index->lookups[i]);
    }
    free(index->lookups);
    free(index);
}
