#ifndef INTERNAL_H
#define INTERNAL_H

#include <stdint.h>
#include <search.h>

struct document {
    uint32_t len;
    term_t *terms;
};

#endif
