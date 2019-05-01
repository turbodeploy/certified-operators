#!/bin/bash -e

order=(
    $(for x in *.log ; do echo $x $(wc -c $x); done \
	  | sort -k 2nr | cut -d' ' -f 1)
)

fmt="%-25s%'12d%'14d\n"
echo "program                         lines         chars" > sizes
echo "------------------------ ------------ -------------" >> sizes
for log in "${order[@]}"; do
    prog=$(basename $log .log)
    stat=($(wc -l -c $log))
    printf $fmt $prog ${stat[0]} ${stat[1]} >> sizes
done >> sizes


