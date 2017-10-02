#!/bin/bash
user=$1
passw=$2
addr=$3
# Wait for all the images to be available
while true
do
    if [[ ! -d "/usr/lib/vmt_images" ]]; then
        sleep 5
        continue
    fi

    NON_EMPTY=$(ls -l /usr/lib/vmt_images/*.tgz | wc -l)
    if [ $NON_EMPTY -gt 1 ]; then
        break
    fi
    sleep 5
done
# Add node
exec /usr/local/bin/vmtctl addnode "${addr}" "${user}" "${passw}"


