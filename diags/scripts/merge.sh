#!/bin/bash
# Check the arguments.
if [[ $# -lt 3 ]]; then
	echo "merge.sh component_name log gc_log"
	exit 1
fi

# Check whether component name has been specified.
if [[ ! -n "${1}" ]]; then
    echo "No component has been specified"
    exit 1
fi

# Set the prefix and the logs, so that we have an easier time using them later.
PREFIX="${1}-1:"
LOG="${2}"
GCLOG="${3}"

# Check for the log file existence
if [[ ! -f "${LOG}" ]]; then
    echo "The log file does not exist"
    exit 2
fi

# Check for the GC log file existence
if [[ ! -f "${GCLOG}" ]]; then
    echo "The GC log file does not exist"
    exit 2
fi

# Filter the lines with the component name, then remove the component name so that we can merge with GC log.
# Every line (including multi-line log entries) will be prefixed with the component name.
PREF_LEN=$(( ${#PREFIX} + 2 ))
egrep "^${PREFIX}" "${LOG}" | cut -c ${PREF_LEN}- > "${LOG}_post.txt"

# GC log convert the file
# The GC log files have a different date/time format, so we need to convert it to the one used by the main log files.
# Shift the lines belonging to the thread dump one character, so that the merge will not stick the lines from the log file starting with the
# today's timestamp in between thread dump. That would otherwise happen if the first 4 digits of the thread dump denoting the rank match the current year,
# since sort will stop comparison at the first non-digit at one of the key positions.
sed -E 's/^([0-9\-]{10})T([0-9\:]{8}).([0-9]{3})\+([0-9]{4}:)(.*)$/\1 \2,\3 \5/' "${GCLOG}" | sed -E 's/^([0-9]{4}:)(.*)/ \1\2/'> "${GCLOG}_post.txt"

# Merge
# Use numeric sort on the date fields, don't sort and stabilize the output.
sort -nms -k1.1,1.4 -k1.6,1.7 -k1.9,1.10 -k1.12,1.13 -k1.15,1.16 -k1.18,1.19 -k1.21,1.23 "${LOG}_post.txt" "${GCLOG}_post.txt" > log_merged.txt

# Remove artifacts
rm -f "${LOG}_post.txt" "${GCLOG}_post.txt"
