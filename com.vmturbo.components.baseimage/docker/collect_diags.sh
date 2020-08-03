#!/bin/bash
#
# This is for Eclipse OpenJ9 JVM.
# Populate a temporary directory with diagnostic text files.
# The directory to use is passed as the first argument.
#
diag_directory="/tmp/diags/system-data"
rm -rf "${diag_directory}" >/dev/null 2>&1
mkdir -p "${diag_directory}"

# The diags_done indicates that the diagnostics has been collected.
# The overall process might be rather long
rm -rf "/tmp/diags_done" >/dev/null 2>&1

# In case column is not present in PATH, default to cat (i.e. pass it as is without formatting)
function column() {
    column_bin=`which column 2>/dev/null`
    if [[ $? == 0 ]]; then
        "$column_bin" "$@"
    else
        cat "$@"
    fi
}

# Determine the process id of the VMT Component
COMPONENT_PID=`ps -e -o pid,args | grep "[j]ava" | awk '{print $1}'`
if [[ ! -z "$COMPONENT_PID" ]]; then
    # trigger a stack dump
    # Do this first, as the results may take a while to get collected
    kill -3 ${COMPONENT_PID}

    echo "Java threads with non-0 PCPU" >>${diag_directory}/top.txt
    echo "----------------------------" >>${diag_directory}/top.txt
    top -H -b -n 1 -p ${COMPONENT_PID} | egrep "^[ \t]*[0-9]" | awk '{if ($9>0) printf "nid=0x%x pcpu=%f\n", $1, $9}' >>${diag_directory}/top.txt

    # Please consult the http://linux.die.net/man/5/proc for further information.
    # Get /proc/<java_pid>/smaps. Contains the information that is not in the pmap.
    cat /proc/${COMPONENT_PID}/smaps >${diag_directory}/smaps.txt

    # Get the statm for the memory usage summary. We will need that along with everything else memory related.
    # The metrics are in 4K pages, so we convert them to KB.
    # size       (1) total program size (same as VmSize in /proc/[pid]/status)
    # resident   (2) resident set size (same as VmRSS in /proc/[pid]/status)
    # share      (3) shared pages (i.e., backed by a file)
    # text       (4) text (code)
    # lib        (5) library (unused in Linux 2.6)
    # data       (6) data + stack
    # dt         (7) dirty pages (unused in Linux 2.6)
    echo "size rss share text lib data dt" >${diag_directory}/statm.tmp
    cat /proc/${COMPONENT_PID}/statm | awk '{printf("%dk %dk %dk %dk %dk %dk %dk\n", $1*4, $2*4, $3*4, $4*4, $5*4b, $6*4b, $7*4)}' >>/${diag_directory}/statm.tmp
    column -t ${diag_directory}/statm.tmp >${diag_directory}/statm.txt
    rm ${diag_directory}/statm.tmp

    # Gather the pmap for process details
    pmap -XX ${COMPONENT_PID} | column -t >${diag_directory}/pmap.txt

    # List of all fds.
    ls -la /proc/${COMPONENT_PID}/fd >${diag_directory}/fds.txt
else
  echo "ERROR: Cannot determine component pid"
fi

# Gather VM and Java Component stats
top -b -n 1 >${diag_directory}/top.txt
echo "" >>${diag_directory}/top.txt

ps -m -e -o pcpu,pid,lwp,vsz,rss,command --sort vsz | awk '{if ($3 != "-") {printf("  => pcpu=%.1f%% nid=0x%x\n",$1,$3)} else {printf("pcpu=%.1f%% pid=%05d vsz=%08dk rss=%08dk cmd=%s\n", $1, $2, $4, $5, substr($0,index($0,$6)))}}' | tail -n +2 >${diag_directory}/ps.txt

# OS Release.
cat /etc/os-release >${diag_directory}/os_release.txt

# Time zone info and time
date >${diag_directory}/date_and_tz.txt

# Full netstat.
netstat -anvepo --tcp --udp >${diag_directory}/netstat.txt

# Memory information
cat /proc/meminfo >${diag_directory}/vmeminfo.txt

# CPU utilization
# The way this works is:
# We collect two distinct measurements, 1 second apart.
# As the result, we have:
# cpu0 <num01> <num02> <num03> ...
# cpu1 <num11> <num12> <num13> ...
# cpu0 <num21> <num22> <num23> ...
# cpu1 <num31> <num32> <num33> ...
# Then we collate it (sort takes care of that), and apply the following awk script on it:
# 'p2 {print $2-p2}{p2=$2}'.
# What this does is: First time the p2 is not set, so the {print $2-p2} is not executed, but {p2=$2} is.
# For the second row and onwards, we print the difference between the row[n+1] and row[n], where n > 1.
# Please note that in awk, both fields and row numbers are 1-based, as opposed to C and Java.
# Now, we have the following:
# <num11-num01> <num12-num02> <num13-num03> ...
# <num21-num11> <num22-num12> <num23-num13> ...
# <num31-num21> <num32-num22> <num33-num23> ...
# Which means we require only odd lines (in awk-speak). The awk 'NR % 2 != 0' takes care of that one.
# The NR is a built-in awk variable that contains the current 1-based line number of the input.
# The columns represent the following (in order)
# cpu user system nice idle iowait hardirq softirq hyperv_steal
(cat /proc/stat | egrep ".*cpu[0-9]+.*";sleep 1;cat /proc/stat | egrep ".*cpu[0-9]+.*") | \
    sort | awk 'p2 p3 p4 p5 p6 p7 p8 {print $2-p2,$3-p3,$4-p4,$5-p5,$6-p6,$7-p7,$8-p8}{p2=$2;p3=$3;p4=$4;p5=$5;p6=$6;p7=$7;p8=$8}' | awk 'NR % 2 != 0' | \
    awk '{total=100./($1+$2+$3+$4+$5+$6+$7+$8)} {printf("%%Cpu%d: us=%.1f sy=%.1f ni=%.1f id=%.1f wa=%.1f hi=%.1f si=%.1f st=%.1f\n", NR-1, $1*total, $3*total, $2*total, $4*total, $5*total, $6*total, $7*total, $8*total)}' | column -t >${diag_directory}/cpu_stat.txt

# File systems utilization.
echo "File Systems" >${diag_directory}/df.txt
echo "------------" >>${diag_directory}/df.txt
df -h >>${diag_directory}/df.txt
echo "" >>${diag_directory}/df.txt
echo "iNodes" >>${diag_directory}/df.txt
echo "------" >>${diag_directory}/df.txt
df -ih >>${diag_directory}/df.txt

# Check disk space
df -k > ${diag_directory}/diskspace.txt

# Check server configured Memory and CPU
echo "XL Instance VMem Capacity: $(free -m | grep Mem | awk '{print $2 " MB"}')" > ${diag_directory}/server_mem_cpu_capacity.txt
echo "XL Instance Number VCPUs: $(nproc)" >> ${diag_directory}/server_mem_cpu_capacity.txt

# Done
echo "1" > /tmp/diags_done
