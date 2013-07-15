#include <string.h>
#include <stdlib.h>

#include "search.h"
#include "internal.h"
#include "utils.h"

#define DOC_CONTAINER_SIZE (64*1024*1024)

struct doc_collection {
    uint32_t num_docs;
    uint32_t max_docs;
    doc_t *docs;
    term_t **containers;
    uint32_t current_container_offset;
    uint32_t num_containers;
    uint32_t max_containers;
};

void add_to_collection(doc_collection_t *coll, uint32_t doclen, term_t *terms) {
    int i;

    term_t filtered_doc[doclen];

    for (i = 0; i < doclen; i++) {
        filtered_doc[i] = terms[i];
    }
    /* filter out double terms */
    qsort(filtered_doc, doclen, sizeof(term_t), &compare_terms);

    int filtered_len = (doclen == 0) ? 0 : 1;
    for (i = 1; i < doclen; i++) {
        if (filtered_doc[i-1] != filtered_doc[i]) {
            filtered_doc[filtered_len] = filtered_doc[i];
            filtered_len++;
        }
    }

    if (coll->current_container_offset + filtered_len > DOC_CONTAINER_SIZE) {
        if (coll->num_containers >= coll->max_containers) {
            coll->max_containers = coll->max_containers*2;
            coll->containers = realloc(coll->containers, coll->max_containers*sizeof(term_t *));
        }
        coll->current_container_offset = 0;
        uint32_t num_terms = (filtered_len > DOC_CONTAINER_SIZE) ? filtered_len : DOC_CONTAINER_SIZE;
        coll->containers[coll->num_containers] = malloc(sizeof(term_t)*num_terms);
        coll->num_containers++;
    }

    int current_container = coll->num_containers-1;

    term_t *container = coll->containers[current_container];
    term_t *container_doc_ptr = container+coll->current_container_offset;
    memcpy(container_doc_ptr, filtered_doc, filtered_len*sizeof(term_t));
    coll->current_container_offset += filtered_len;

    doc_t doc;
    doc.terms = container_doc_ptr;
    doc.len = filtered_len;

    if (coll->num_docs >= coll->max_docs) {
        coll->max_docs = coll->max_docs*2;
        coll->docs = realloc(coll->docs, coll->max_docs*sizeof(doc_t));
    }

    coll->docs[coll->num_docs] = doc;
    coll->num_docs++;
}

void free_collection(doc_collection_t *coll) {
    int i;
    for (i = 0; i < coll->num_containers; i++) {
        free(coll->containers[i]);
    }
    free(coll->containers);
    free(coll->docs);
}

doc_collection_t* new_collection() {
    doc_collection_t *coll;
    coll = malloc(sizeof(doc_collection_t));
    if (coll == NULL) {
        return NULL;
    }

    coll->num_docs = 0;
    coll->max_docs = 1024;
    coll->max_containers = 1024;
    coll->current_container_offset = 0;
    coll->num_containers = 1;

    coll->docs = malloc(coll->max_docs*sizeof(doc_t));
    if (coll->docs == NULL) {
        free(coll);
        return NULL;
    }
    coll->containers = malloc(coll->max_containers*sizeof(term_t *));
    if (coll->containers == NULL) {
        free(coll);
        free(coll->docs);
        return NULL;
    }
    coll->containers[0] = malloc(sizeof(term_t)*DOC_CONTAINER_SIZE);
    if (coll->containers[0] == NULL) {
        coll->num_containers = 0;
        free_collection(coll);
        return NULL;
    }

    return coll;
}

uint32_t num_docs_in_collection(doc_collection_t *coll) {
    return coll->num_docs;
}

doc_t *get_docs_in_collection(doc_collection_t *coll) {
    return coll->docs;
}
