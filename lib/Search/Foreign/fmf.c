#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>
#include <omp.h>
#include <assert.h>

#include "search.h"
#include "utils.h"
#include "internal.h"

typedef struct {
    uint32_t len;
    tfreq_t *term_frequencies;
    tfreq_t total;
} term_freqs_t;

typedef tfreq_t *cluster_center_t;

int compare_term_freq(const void *elem1, const void *elem2, void *arg) {
    term_t t1 = *((term_t *)elem1);
    term_t t2 = *((term_t *)elem2);
    tfreq_t *term_freqs = (tfreq_t *)arg;

    tfreq_t f1 = term_freqs[t1];
    tfreq_t f2 = term_freqs[t2];

    if (f1 > f2) return -1;
    if (f1 < f2) return  1;
    return 0;
}

int compare_score(const void *elem1, const void *elem2, void *arg) {
    uint32_t t1 = *((uint32_t *)elem1);
    uint32_t t2 = *((uint32_t *)elem2);
    double *scores = (double *)arg;

    double f1 = scores[t1];
    double f2 = scores[t2];

    if (f1 < f2) return -1;
    if (f1 > f2) return  1;
    return 0;
}


int assign_cluster(uint32_t did,
                   doc_t *docs,
                   int num_clusters,
                   term_freqs_t *tfreqs,
                   cluster_center_t *cluster_centers,
                   double** prefixsums,
                   tfreq_t *cfreqs,
                   double* cfreqs_prefixsums,
                   double* scorebuf) {
    int i, j;

    bzero(scorebuf, num_clusters*sizeof(double));

    doc_t doc = docs[did];

    term_t filtered_doc[doc.len];
    for (i = 0; i < doc.len; i++) {
        filtered_doc[i] = doc.terms[i];
    }
    /* filter out double terms */
    qsort(filtered_doc, doc.len, sizeof(term_t), &compare_terms);

    int filtered_len = doc.len == 0 ? 0 : 1;
    for (i = 1; i < doc.len; i++) {
        if (filtered_doc[i-1] != filtered_doc[i]) {
            filtered_doc[filtered_len] = filtered_doc[i];
            filtered_len++;
        }
    }

    if (filtered_len == 0) {
        return min_index_offset(scorebuf, num_clusters, did%num_clusters);
    }

    for (i = 0; i < filtered_len; i++) {
        term_t term = filtered_doc[i];
        double p1 = (double)tfreqs->term_frequencies[term]/tfreqs->total;
        for (j = 0; j < num_clusters; j++) {
            scorebuf[j] += p1*prefixsums[j][term];
            /* account for extra overhead if term does not occur in cluster */
            if (cluster_centers[j][term] == 0) {
                scorebuf[j] += p1*cfreqs_prefixsums[term];
            }
        }
    }

    /* calculate terms with same frequency */
    for (j = 0; j < num_clusters; j++) {
        qsort_r(filtered_doc, filtered_len, sizeof(term_t), &compare_term_freq, cluster_centers[j]);
        term_t term = filtered_doc[0];
        tfreq_t last_freq = cluster_centers[j][term];
        double psum = 0;
        for (i = 1; i < filtered_len; i++) {
            term = filtered_doc[i];
            double p1 = (double)tfreqs->term_frequencies[term]/tfreqs->total;
            scorebuf[j] += p1*psum;
            if (cluster_centers[j][term] == last_freq) {
                psum += p1;
            } else {
                last_freq = cluster_centers[j][term];
                psum = 0;
            }
        }
    }

    /* calculate terms with same clusterfrequency */
    qsort_r(filtered_doc, filtered_len, sizeof(term_t), &compare_term_freq, cfreqs);
    for (j = 0; j < num_clusters; j++) {
        tfreq_t last_freq = -1;
        double psum = 0;
        for (i = 0; i < filtered_len; i++) {
            term_t term = filtered_doc[i];
            if (cluster_centers[j][term] == 0) {
                double p1 = (double)tfreqs->term_frequencies[term]/tfreqs->total;
                scorebuf[j] += p1*psum;
                if (cfreqs[term] == last_freq) {
                    psum += p1;
                } else {
                    last_freq = cfreqs[term];
                    psum = 0;
                }
            }
        }
    }

    return min_index_offset(scorebuf, num_clusters, did%num_clusters);
}

int assign_cluster_fast(uint32_t did,
                        doc_t *docs,
                        int num_clusters,
                        term_freqs_t *tfreqs,
                        cluster_center_t *cluster_centers,
                        double** prefixsums,
                        tfreq_t *cfreqs,
                        double* cfreqs_prefixsums,
                        double* scorebuf) {
    int i, j;

    bzero(scorebuf, num_clusters*sizeof(double));

    const doc_t doc = docs[did];

    for (i = 0; i < doc.len; i++) {
        term_t term = doc.terms[i];
        double p1 = (double)tfreqs->term_frequencies[term]/tfreqs->total;
        for (j = 0; j < num_clusters; j++) {
            scorebuf[j] += p1*prefixsums[j][term];
            /* account for extra overhead if term does not occur in cluster */
            if (cluster_centers[j][term] == 0) {
                scorebuf[j] += p1*cfreqs_prefixsums[term];
            }
        }
    }

    return min_index_offset(scorebuf, num_clusters, did%num_clusters);
}

static inline uint32_t ilog2(const uint32_t x) {
  uint32_t y;
  asm ( "\tbsr %1, %0\n"
      : "=r"(y)
      : "r" (x)
  );
  return y;
}

int assign_cluster_scores(uint32_t did,
                          doc_t *docs,
                          int num_clusters,
                          term_freqs_t *tfreqs,
                          double** prefixsums,
                          double* scorebuf,
                          uint32_t **top_clusters,
                          uint32_t num_levels,
                          double *top_cluster_scores) {
    int i, j;
    int k;

    doc_t doc = docs[did];

    uint32_t minindex[num_levels];

    for (k = 0; k < num_levels; k++) {
        int num_top_clusters = (num_clusters >> (num_levels-1-k));
        for (i = 0; i < num_clusters; i++) {
            scorebuf[i] = 1e10;
        }

        for (i = 0; i < doc.len; i++) {
            term_t term = doc.terms[i];
            double p1 = (double)tfreqs->term_frequencies[term]/tfreqs->total;
            for (j = 0; j < num_top_clusters; j++) {
                uint32_t cid = top_clusters[term][j];
                if (scorebuf[cid] > 1e9) {
                    scorebuf[cid] = 0;
                }
                scorebuf[cid] += p1*prefixsums[cid][term];
            }
        }

        minindex[k] = min_index_offset(scorebuf, num_clusters, did%num_clusters);
        double score = 0;
        for (i = 0; i < doc.len; i++) {
            term_t term = doc.terms[i];
            double p1 = (double)tfreqs->term_frequencies[term]/tfreqs->total;
            score += p1*prefixsums[minindex[k]][term];
        }
        top_cluster_scores[k] += score;
    }

    /*
    for (k = 0; k < num_levels; k++) {
        if (minindex[k] == minindex[num_levels-1]) {
            top_cluster_histogram[k]++;
        }
    }
    */

    if (scorebuf[minindex[num_levels-1]] > 1e9) {
        scorebuf[minindex[num_levels-1]] = 0;
    }

    return minindex[num_levels-1];
}

void add_to_cluster(doc_t doc, cluster_center_t cluster_vector) {
    int i;
    for (i = 0; i < doc.len; i++) {
        term_t term = doc.terms[i];
        cluster_vector[term]++;
    }
}

void remove_from_cluster(doc_t doc, cluster_center_t cluster_vector) {
    int i;
    for (i = 0; i < doc.len; i++) {
        term_t term = doc.terms[i];
        cluster_vector[term]--;
    }
}

void calc_cfreqs(tfreq_t *cfreqs, cluster_center_t *cluster_centers, uint32_t num_clusters, uint32_t num_terms) {
    int term, i;

    bzero(cfreqs, sizeof(tfreq_t)*num_terms);

    for (term = 0; term < num_terms; term++) {
        for (i = 0; i < num_clusters; i++) {
            if (cluster_centers[i][term] > 0) {
                cfreqs[term]++;
            }
        }
    }
}

void calc_prefix_sum(cluster_center_t cluster_vector, term_freqs_t *freqs, double *prefixsums) {
    int i;

    term_t *terms = malloc(sizeof(term_t)*freqs->len);
    for (i = 0; i < freqs->len; i++) {
        terms[i] = i;
    }

    qsort_r(terms, freqs->len, sizeof(term_t), compare_term_freq, cluster_vector);

    term_t cur_term;
    tfreq_t last = 0;
    tfreq_t accum = 0;
    tfreq_t prefixsum = 0;
    for (i = 0; i < freqs->len; i++) {
        cur_term = terms[i];
        tfreq_t cur = cluster_vector[cur_term];
        if (last == cur) {
            prefixsums[cur_term] = (double)prefixsum/freqs->total;
        } else {
            prefixsum = accum;
            prefixsums[cur_term] = (double)prefixsum/freqs->total;
        }
        accum += freqs->term_frequencies[cur_term];
        last = cur;
    }

    free(terms);
}


void calc_top_clusters(uint32_t num_clusters, uint32_t **clusters, term_freqs_t *freqs, double **prefixsums) {
    int i,j;
    double scores[num_clusters];
    for (i = 0; i < freqs->len; i++) {
        for (j = 0; j < num_clusters; j++) {
            clusters[i][j] = j;
            scores[j] = prefixsums[j][i];
        }
        qsort_r(clusters[i], num_clusters, sizeof(uint32_t), compare_score, scores);
    }
}


void fmf_round(int num_docs,
               doc_t *docs,
               int num_clusters,
               double shrink_factor,
               int fast_scoring,
               cluster_center_t *cluster_centers,
               uint32_t *cluster_assignments,
               double **prefixsums,
               tfreq_t *cfreqs,
               double *cfreqs_prefixsums,
               term_freqs_t *tfreqs) {
    int i, k;
    if (num_docs == num_clusters) {
        for (i = 0; i < num_clusters; i++) {
            cluster_assignments[i] = i;
        }
        return;
    } else {
        int subset_size = (int)(num_docs*shrink_factor);
        if (subset_size < num_clusters) {
            subset_size = num_clusters;
        }
        fmf_round(subset_size,
                  docs,
                  num_clusters,
                  shrink_factor,
                  fast_scoring,
                  cluster_centers,
                  cluster_assignments,
                  prefixsums,
                  cfreqs,
                  cfreqs_prefixsums,
                  tfreqs);

        double totalscore = 0;
        double lastscore = 0;

        uint32_t num_terms = tfreqs->len;

        /* TEMP */
        /*
        uint32_t **top_clusters = malloc(tfreqs->len*sizeof(uint32_t *));
        uint32_t num_levels = ilog2(num_clusters)+1;
        for (i = 0; i < tfreqs->len; i++) {
            top_clusters[i] = malloc(num_clusters*sizeof(uint32_t));
        }
        double top_cluster_scores[num_levels];
        */
        /* END TEMP */

        for (k = 0; k < 3 || totalscore < lastscore*0.99; k++) {
            /* reset cluster centers */
            for (i = 0; i < num_clusters; i++) {
                bzero(cluster_centers[i], sizeof(tfreq_t)*num_terms);
            }

            int num_assignments = (k>0) ? num_docs : subset_size;

            #pragma omp parallel shared(num_clusters, cluster_centers, docs, cluster_assignments, num_assignments) private(i)
            {
                int numthreads = omp_get_num_threads();
                int tid = omp_get_thread_num();
                int bucketsize = (num_clusters + numthreads)/numthreads;
                /* Add docs to cluster center */
                for (i = 0; i < num_assignments; i++) {
                    uint32_t cid = cluster_assignments[i];
                    if (cid >= tid*bucketsize && cid < (tid+1)*bucketsize) {
                        add_to_cluster(docs[i], cluster_centers[cid]);
                    }
                }
            }


            #pragma omp parallel shared(num_clusters, cluster_centers, tfreqs, prefixsums) private(i)
            {
                /* Calculate prefix sums */
                #pragma omp for schedule(static) nowait
                for (i = 0; i < num_clusters; i++) {
                    calc_prefix_sum(cluster_centers[i], tfreqs, prefixsums[i]);
                }
            }

            calc_cfreqs(cfreqs, cluster_centers, num_clusters, num_terms);
            calc_prefix_sum(cfreqs, tfreqs, cfreqs_prefixsums);

            lastscore = totalscore;
            totalscore = 0;

            int clustercount[num_clusters];
            for (i = 0; i < num_clusters; i++) {
                clustercount[i] = 0;
            }

            if (num_docs < num_clusters*100) {
                double scorebuf[num_clusters];
                for (i = 0; i < num_docs; i++) {
                    uint32_t clusterid;
                    if (fast_scoring) {
                        clusterid = assign_cluster_fast(i,
                                                        docs,
                                                        num_clusters,
                                                        tfreqs,
                                                        cluster_centers,
                                                        prefixsums,
                                                        cfreqs,
                                                        cfreqs_prefixsums,
                                                        scorebuf);
                    } else {
                        clusterid = assign_cluster(i,
                                                   docs,
                                                   num_clusters,
                                                   tfreqs,
                                                   cluster_centers,
                                                   prefixsums,
                                                   cfreqs,
                                                   cfreqs_prefixsums,
                                                   scorebuf);
                    }
                    if (i < num_assignments) {
                        uint32_t oldclusterid = cluster_assignments[i];
                        if (oldclusterid != clusterid) {
                            remove_from_cluster(docs[i], cluster_centers[oldclusterid]);
                            add_to_cluster(docs[i], cluster_centers[clusterid]);
                            calc_prefix_sum(cluster_centers[clusterid], tfreqs, prefixsums[clusterid]);
                            calc_prefix_sum(cluster_centers[oldclusterid], tfreqs, prefixsums[oldclusterid]);
                            calc_cfreqs(cfreqs, cluster_centers, num_clusters, num_terms);
                            calc_prefix_sum(cfreqs, tfreqs, cfreqs_prefixsums);
                        }
                    } else {
                        add_to_cluster(docs[i], cluster_centers[clusterid]);
                        calc_prefix_sum(cluster_centers[clusterid], tfreqs, prefixsums[clusterid]);
                        calc_cfreqs(cfreqs, cluster_centers, num_clusters, num_terms);
                        calc_prefix_sum(cfreqs, tfreqs, cfreqs_prefixsums);
                    }
                    cluster_assignments[i] = clusterid;
                    totalscore += scorebuf[clusterid];
                    clustercount[clusterid]++;
                }
            } else {
                #pragma omp parallel shared(num_clusters, docs, tfreqs, prefixsums, totalscore, clustercount) private(i)
                {
                    double scorebuf[num_clusters];
                    double localscore = 0;
                    int localclustercount[num_clusters];
                    for (i = 0; i < num_clusters; i++) {
                        localclustercount[i] = 0;
                    }
                    #pragma omp for schedule(static) nowait
                    for (i = 0; i < num_docs; i++) {
                        uint32_t clusterid;
                        if (fast_scoring) {
                            clusterid = assign_cluster_fast(i,
                                                            docs,
                                                            num_clusters,
                                                            tfreqs,
                                                            cluster_centers,
                                                            prefixsums,
                                                            cfreqs,
                                                            cfreqs_prefixsums,
                                                            scorebuf);
                        } else {
                            clusterid = assign_cluster(i,
                                                       docs,
                                                       num_clusters,
                                                       tfreqs,
                                                       cluster_centers,
                                                       prefixsums,
                                                       cfreqs,
                                                       cfreqs_prefixsums,
                                                       scorebuf);
                        }
                        cluster_assignments[i] = clusterid;
                        localclustercount[clusterid]++;
                        localscore += scorebuf[clusterid];
                    }
                    #pragma omp atomic update
                    totalscore += localscore;
                    for (i = 0; i < num_clusters; i++) {
                        #pragma omp atomic update
                        clustercount[i] += localclustercount[i];
                    }
                }
            }
        }
    }
}

#define BRANCH_FACTOR 8

int top_down_round(int num_docs,
                   doc_t *docs,
                   uint32_t *docids,
                   uint32_t min_cluster_size,
                   double shrink_factor,
                   int fast_scoring,
                   uint32_t clusterid_offset,
                   uint32_t *cluster_assignments,
                   term_freqs_t *tfreqs) {
    int i,j;

    int num_clusters = min(max(1, num_docs/min_cluster_size), BRANCH_FACTOR);

    cluster_center_t *cluster_centers = malloc(sizeof(cluster_center_t)*num_clusters);
    double **prefixsums = malloc(sizeof(double *)*num_clusters);
    for (i = 0; i < num_clusters; i++) {
        cluster_centers[i] = calloc(tfreqs->len, sizeof(tfreq_t));
        prefixsums[i] = calloc(tfreqs->len, sizeof(double));
    }
    tfreq_t *cfreqs = calloc(tfreqs->len, sizeof(tfreq_t));
    double *cfreqs_prefixsums = calloc(tfreqs->len, sizeof(double));
    doc_t *subdocs = malloc(sizeof(doc_t)*num_docs);
    for (i = 0; i < num_docs; i++) {
        subdocs[i] = docs[docids[i]];
    }
    uint32_t *subcluster_assignments = calloc(num_docs, sizeof(uint32_t));

    fmf_round(num_docs,
              subdocs,
              num_clusters,
              shrink_factor,
              fast_scoring,
              cluster_centers,
              subcluster_assignments,
              prefixsums,
              cfreqs,
              cfreqs_prefixsums,
              tfreqs);

    int numsubclusters = 0;

    int cur_clusterdocs = 0;

    for (i = 0; i < num_clusters; i++) {
        /* count number of docs in cluster */
        uint32_t num_subdocs = 0;
        for (j = 0; j < num_docs; j++) {
            if (subcluster_assignments[j] == i) {
                num_subdocs++;
            }
        }
        cur_clusterdocs += num_subdocs;
        if (num_subdocs > 0) {
            /* accumulate sub docs */
            uint32_t *subdocids = malloc(sizeof(uint32_t)*num_subdocs);
            int k = 0;
            for (j = 0; j < num_docs; j++) {
                if (subcluster_assignments[j] == i) {
                    subdocids[k] = docids[j];
                    k++;
                }
            }

            randomize_uint32_array(subdocids, num_subdocs, num_subdocs);

            assert(k==num_subdocs);

            if (num_subdocs/min_cluster_size <= 1 || num_subdocs > num_docs*0.95) {
                for (j = 0; j < num_subdocs; j++) {
                    cluster_assignments[subdocids[j]] = clusterid_offset + numsubclusters;
                }
                if (cur_clusterdocs > min_cluster_size/2) {
                    numsubclusters++;
                    cur_clusterdocs = 0;
                }
            } else {
                numsubclusters += top_down_round(num_subdocs,
                                                 docs,
                                                 subdocids,
                                                 min_cluster_size,
                                                 shrink_factor,
                                                 fast_scoring,
                                                 clusterid_offset + numsubclusters,
                                                 cluster_assignments,
                                                 tfreqs);
                cur_clusterdocs = 0;
            }
            free(subdocids);
        }
    }
    for (i = 0; i < num_clusters; i++) {
        free(cluster_centers[i]);
        free(prefixsums[i]);
    }
    free(cluster_centers);
    free(prefixsums);
    free(subcluster_assignments);
    free(cfreqs);
    free(cfreqs_prefixsums);
    free(subdocs);

    return numsubclusters;
}

uint32_t remove_empty_clusters(uint32_t *cluster_assignments, uint32_t num_docs) {
    int initial_num_clusters = 0;
    int i;
    for(i = 0; i < num_docs; i++) {
        initial_num_clusters = max(cluster_assignments[i], initial_num_clusters);
    }
    initial_num_clusters = initial_num_clusters+1;

    uint32_t *cluster_size = malloc(initial_num_clusters*sizeof(uint32_t));
    for (i = 0; i < initial_num_clusters; i++) {
        cluster_size[i] = 0;
    }

    for (i = 0; i < num_docs; i++) {
        cluster_size[cluster_assignments[i]]++;
    }

    uint32_t *remapping = malloc(initial_num_clusters*sizeof(uint32_t));
    uint32_t current_num_clusters = 0;
    for (i = 0; i < initial_num_clusters; i++) {
        if (cluster_size[i] > 0) {
            remapping[i] = current_num_clusters;
            current_num_clusters++;
        }
    }
    for (i = 0; i < num_docs; i++) {
        cluster_assignments[i] = remapping[cluster_assignments[i]];
    }
    free(remapping);

    return current_num_clusters;
}

void normalize_clusters(uint32_t *cluster_assignments, uint32_t num_docs, uint32_t final_num_clusters) {
    int i;
    int initial_num_clusters = remove_empty_clusters(cluster_assignments, num_docs);

    uint32_t *cluster_size = malloc(max(final_num_clusters, initial_num_clusters)*sizeof(uint32_t));
    for (i = 0; i < initial_num_clusters; i++) {
        cluster_size[i] = 0;
    }

    for (i = 0; i < num_docs; i++) {
        cluster_size[cluster_assignments[i]]++;
    }

    /* merge clusters */
    if (initial_num_clusters > final_num_clusters) {
        uint32_t *remapping = malloc(initial_num_clusters*sizeof(uint32_t));
        for (i = 0; i < initial_num_clusters; i++) {
            remapping[i] = i;
        }
        uint32_t new_num_clusters = initial_num_clusters;
        while (new_num_clusters >= final_num_clusters) {
            int min_index_1 = 0;
            while (cluster_size[min_index_1] == 0) {
                min_index_1++;
            }
            int min_index_2 = min_index_1+1;
            while (cluster_size[min_index_2] == 0) {
                min_index_2++;
            }
            for (i = 2; i < initial_num_clusters; i++) {
                if (cluster_size[i] > 0 && cluster_size[i] < cluster_size[min_index_1]) {
                    min_index_1 = i;
                } else if (cluster_size[i] > 0 && cluster_size[i] < cluster_size[min_index_2]) {
                    min_index_2 = i;
                }
            }

            cluster_size[min_index_1] += cluster_size[min_index_2];
            cluster_size[min_index_2] = 0;
            remapping[min_index_2] = min_index_1;
            new_num_clusters--;
        }
        /* find multiple merges */
        for (i = 0; i < initial_num_clusters; i++) {
            int j = i;
            while (remapping[j] != j) {
                j = remapping[j];
            }
            remapping[i] = j;
        }
        for (i = 0; i < num_docs; i++) {
            cluster_assignments[i] = remapping[cluster_assignments[i]];
        }
        free(remapping);
    }

    uint32_t current_num_clusters = remove_empty_clusters(cluster_assignments, num_docs);

    for (i = 0; i < current_num_clusters; i++) {
        cluster_size[i] = 0;
    }

    for (i = 0; i < num_docs; i++) {
        cluster_size[cluster_assignments[i]]++;
    }

    /* split to large clusters */
    while (current_num_clusters < final_num_clusters) {
        int max_cluster_id = 0;
        int split_cluster_size = 0;
        for (i = 0; i < current_num_clusters; i++) {
            if (cluster_size[i] > cluster_size[max_cluster_id]) {
                max_cluster_id = i;
            }
        }
        for (i = 0; i < num_docs; i++) {
            if (cluster_assignments[i] == max_cluster_id) {
                split_cluster_size++;
                if (split_cluster_size > cluster_size[max_cluster_id]/2) {
                    cluster_assignments[i] = current_num_clusters;
                }
            }
        }
        cluster_size[current_num_clusters] = split_cluster_size - cluster_size[max_cluster_id]/2;
        cluster_size[max_cluster_id] = cluster_size[max_cluster_id]/2;
        current_num_clusters++;
    }

    free(cluster_size);
}

void fmf_clustering(uint32_t num_docs,
                    doc_t *docs,
                    uint32_t num_clusters,
                    double shrink_factor,
                    int top_down,
                    int fast_scoring,
                    uint32_t *cluster_assignments,
                    uint32_t num_terms,
                    tfreq_t *freqs) {
    int i;

    srand(time(NULL));

    /* Randomize doc order */
    uint32_t *docids = malloc(sizeof(uint32_t)*num_docs);
    for (i = 0; i < num_docs; i++) {
        docids[i] = i;
    }

    randomize_uint32_array(docids, num_docs, num_docs);

    doc_t *randomized_docs = malloc(sizeof(doc_t)*num_docs);
    for (i = 0; i < num_docs; i++) {
        randomized_docs[i] = docs[docids[i]];
    }

    term_freqs_t *tfreqs = malloc(sizeof(term_freqs_t));
    tfreqs->len = num_terms;
    tfreqs->term_frequencies = malloc(sizeof(tfreq_t)*num_terms);
    memcpy(tfreqs->term_frequencies, freqs, sizeof(tfreq_t)*num_terms);
    tfreq_t total = 0;
    for (i = 0; i < num_terms; i++) {
        total += tfreqs->term_frequencies[i];
    }
    tfreqs->total = total;

    uint32_t *cluster_assignments_random = malloc(sizeof(uint32_t)*num_docs);

    if (top_down) {
        int docs_per_cluster = (int)(num_docs/num_clusters);
        top_down_round(num_docs,
                       randomized_docs,
                       docids,
                       docs_per_cluster,
                       shrink_factor,
                       fast_scoring,
                       0,
                       cluster_assignments_random,
                       tfreqs);

        normalize_clusters(cluster_assignments_random, num_docs, num_clusters);
    } else {
        /* Initialize data structures */
        cluster_center_t *cluster_centers = malloc(sizeof(cluster_center_t)*num_clusters);
        for (i = 0; i < num_clusters; i++) {
            cluster_centers[i] = calloc(num_terms, sizeof(tfreq_t));
        }

        tfreq_t *cfreqs = calloc(num_terms, sizeof(tfreq_t));
        double *cfreqs_prefixsums = calloc(num_terms, sizeof(double));

        double **prefixsums = malloc(sizeof(double *)*num_clusters);
        for (i = 0; i < num_clusters; i++) {
            prefixsums[i] = calloc(num_terms, sizeof(double));
        }

        fmf_round(num_docs,
                  randomized_docs,
                  num_clusters,
                  shrink_factor,
                  fast_scoring,
                  cluster_centers,
                  cluster_assignments_random,
                  prefixsums,
                  cfreqs,
                  cfreqs_prefixsums,
                  tfreqs);

        /* free memory */
        for (i = 0; i < num_clusters; i++) {
            free(cluster_centers[i]);
            free(prefixsums[i]);
        }
        free(cluster_centers);
        free(prefixsums);
        free(cfreqs);
        free(cfreqs_prefixsums);
    }

    /* copy ids back */
    for (i = 0; i < num_docs; i++) {
        cluster_assignments[docids[i]] = cluster_assignments_random[i];
    }
    free(cluster_assignments_random);
    free(docids);
    free(tfreqs->term_frequencies);
    free(tfreqs);
}
