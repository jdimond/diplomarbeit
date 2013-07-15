#ifndef FMF_H
#define FMF_H

#include "search.h"

void fmf_clustering(uint32_t num_docs,
                    doc_t *docs,
                    uint32_t num_clusters,
                    double shrink_factor,
                    int top_down,
                    int fast_scoring,
                    uint32_t *cluster_assignments,
                    uint32_t num_terms,
                    tfreq_t *freqs);

#endif
