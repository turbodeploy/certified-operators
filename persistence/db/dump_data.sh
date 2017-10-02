#!/bin/bash

DUMP_FILE_NAME=${1-vmtdb_dump}

if command -v xz >/dev/null; then 
	COMPRESSOR=xz
	FILE_EXT=xz
else 
	COMPRESSOR=gzip
	FILE_EXT=gz
fi

mysqldump -f -u root -pvmturbo vmtdb --extended_insert=true --routines | $COMPRESSOR > /tmp/$DUMP_FILE_NAME.sql.$FILE_EXT