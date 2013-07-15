#!/bin/bash

# options may be followed by one colon to indicate they have a required argument
if ! options=$(getopt -o tn: -l query-log-training:,term-threshold:,doc-limit:,query-log-testing:,cores:,top-down -- "$@")
then
    # something went wrong, getopt will put out an error message for us
    exit 1
fi

eval set -- $options

usage() {
    echo "$0 [-t] [--query-log-trainig FILE] [--query-log-testing FILE]
    [--doc-limit N] [--term-threshold N] -n SIZE_PARAM BASE_DIR" 1>&2
}

top_down=false
term_threshold=0
cores=1

while [ $# -gt 0 ]
do
    case "$1" in
    -n) size_param="$2"; shift;;
    --query-log-training) query_log_training="$2"; shift;;
    --query-log-testing) query_log_testing="$2"; shift;;
    -t|--top-down) top_down=true;;
    --term-threshold) term_threshold="$2"; shift;;
    --doc-limit) doc_limit="$2"; shift;;
    --cores) cores="$2"; shift;;
    (--) shift; break;;
    (-*) echo "$0: error - unrecognized option $1" 1>&2 ; usage; exit 1;;
    (*) break;;
    esac
    shift
done

base_dir=$1

if [[ -z "$base_dir" ]]; then
    usage
    exit 1
fi

if [[ -z "$size_param" ]]; then
    usage
    exit 1
fi

if [[ ! -d "${base_dir}" ]]; then
    echo "$base_dir doesn't exist" 1>&2
    exit 1
fi

modifier="$size_param"

[[ -n "$top_down" ]] && modifier="${modifier}_td"
[[ -n "$query_log_training" ]] && modifier="${modifier}_ql"
[[ -n "$doc_limit" ]] && modifier="${modifier}_dl_${doc_limit}"
[[ -n "$term_threshold" ]] && modifier="${modifier}_tt_${term_threshold}"

echo "Running with configuration: $modifier"

ROOT_DIR=$(cd $(dirname $0) > /dev/null; pwd)

cmds="$ROOT_DIR/../dist/build"
clustercmd="$cmds/Cluster/Cluster"
indexcmd="$cmds/BuildIndex/BuildIndex"
lookupcmd="$cmds/LookupComplexity/LookupComplexity"

RTS_OPTS="+RTS -N$cores -RTS"

runcluster="$clustercmd -n=$size_param"

[[ -n "$top_down" ]] && runcluster="$runcluster --topdown"
[[ -n "$query_log_training" ]] && runcluster="$runcluster --use-query-log=$query_log_training"
[[ -n "$doc_limit" ]] && runcluster="$runcluster --doc-limit=$doc_limit"
[[ -n "$term_threshold" ]] && runcluster="$runcluster --term-threshold=$term_threshold"

index_dir="$base_dir/index"
cluster_file="$index_dir/clustering_${modifier}"
sindex_dir="$base_dir/sindex_${modifier}"
results_dir="$base_dir/results"

runcluster="$runcluster $index_dir $cluster_file"

$runcluster || exit 1

mkdir -p "$sindex_dir" || exit 1

runindex="$indexcmd $index_dir $sindex_dir --cluster-file=$cluster_file"

$runindex || exit 1

mkdir -p "$results_dir" || exit 1

if [[ -n "$query_log_testing" ]]; then
    result_file="$results_dir/results_ql_${modifier}"
    cat $query_log_testing | $lookupcmd "$sindex_dir" "$index_dir" > "$result_file" || exit 1
else
    result_file="$results_dir/results_random_${modifier}"
    $lookupcmd -r 10000 "$sindex_dir" "$index_dir" > "$result_file" || exit 1
fi
