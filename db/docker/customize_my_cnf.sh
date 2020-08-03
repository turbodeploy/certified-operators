#!/bin/bash

MYSQL_CONF=$1
TOTAL_MEM_BYTES=$(cat /sys/fs/cgroup/memory/memory.limit_in_bytes)
if [ -z "$DB_MEM_PCT_FOR_BUFFER_POOL" ]; then
    DB_MEM_PCT_FOR_BUFFER_POOL=75
fi
#bashd doesn't support FP arithmetic
BUFFER_POOL_SIZE_MB=$(( (TOTAL_MEM_BYTES * DB_MEM_PCT_FOR_BUFFER_POOL) / (100*1024*1024) ))

echo "Total Memory: $(( (TOTAL_MEM_BYTES)/(1024*1024) )) MB"
echo "Percentage of total memory allocated to DB buffer pool: ${DB_MEM_PCT_FOR_BUFFER_POOL}%"
echo "Changing Innodb buffer pool size to: ${BUFFER_POOL_SIZE_MB} MB"

sed -i 's/innodb_buffer_pool_size.*/innodb_buffer_pool_size = '$BUFFER_POOL_SIZE_MB'M/' $MYSQL_CONF

if [ -n "$DB_MAX_ALLOWED_PACKET" ]; then
    sed -i 's/max_allowed_packet.*/max_allowed_packet      = '$DB_MAX_ALLOWED_PACKET'/' $MYSQL_CONF
fi

if [ -n "$DB_KEY_BUFFER_SIZE" ]; then
    sed -i 's/key_buffer_size.*/key_buffer_size         = '$DB_KEY_BUFFER_SIZE'/' $MYSQL_CONF
fi

if [ -n "$DB_MYISAM_SORT_BUFFER_SIZE" ]; then
    sed -i 's/myisam_sort_buffer_size.*/myisam_sort_buffer_size = '$DB_MYISAM_SORT_BUFFER_SIZE'/' $MYSQL_CONF
fi
