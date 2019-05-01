#!/bin/bash -e

function es_download() {
    local tmpfile=$(mktemp)
    local scrollid=
    local finished=false
    local prog=$1
    local log="$prog.log"
    local query="{\"query\": { \"term\": { \"program.keyword\": \"$prog-1\"}}}"
    rm -rf $log
    curl -X GET -s -H 'accept: application/json' -H 'content-type: application/json' \
	 "$ES_ENDPOINT/$ES_INDEX/_search?size=10000&scroll=1m" \
	 -d "$query" > $tmpfile
    local scrollid=$(jq -r ._scroll_id $tmpfile)
    local tot=0

    while ! $finished ; do
	jq -r '.hits.hits[]._source.message|(try .[] catch .)' $tmpfile >> $log
	local hits=$(jq -r '.hits.hits|length' $tmpfile)
	if [[ $hits > 0 ]] ; then
	    tot=$((tot+hits))
	    curl -X POST -s -H 'accept: application/json' -H 'content-type: application/json' \
		 "$ES_ENDPOINT/_search/scroll" -d"{\"scroll\": \"1m\", \"scroll_id\": \"$scrollid\"}" > $tmpfile
	else
	    finished=true
	fi
	echo -n "$prog: $tot"
    done
    echo
    touch sizes
    gsed -i -e "/^$prog/d" -e "/^program/d" -e "/^[-]/d" sizes
    local fmt="%-20s%'12d%'14d\n"
    printf $fmt $prog $(wcstat -l $log) $(wcstat -c $log) >> sizes
    sort -k 3nr sizes > sizes.sort
    printf "%-20s%12s%14s\n" program lines chars > sizes
    echo ------------------- ------------ ------------- >> sizes
    cat sizes.sort >> sizes
    rm sizes.sort
}

function wcstat() {
    local out=($(wc $1 $2))
    echo ${out[0]}
}

es_download "$@"
