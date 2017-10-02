package com.vmturbo.api.component.external.api.util;

import java.util.Set;

import com.google.common.collect.Sets;

/**
 * TemplatesUtils defined all allowed template field names which will be use to validate input
 * template information.
 */
public class TemplatesUtils {
    public static final String PROFILE = "Profile";
    // compute
    public static final String CPU_CONSUMED_FACTOR = "cpuConsumedFactor";
    public static final String CPU_SPEED = "cpuSpeed";
    public static final String IO_THROUGHPUT = "ioThroughput";
    public static final String IO_THROUGHPUT_SIZE = "ioThroughputSize";
    public static final String MEMORY_CONSUMED_FACTOR = "memoryConsumedFactor";
    public static final String MEMORY_SIZE = "memorySize";
    public static final String NUM_OF_CPU = "numOfCpu";
    public static final String NUM_OF_CORES = "numOfCores";
    public static final String NETWORK_THROUGHPUT = "networkThroughput";
    public static final String NETWORK_THROUGHPUT_SIZE = "networkThroughputSize";
    // storage
    public static final String DISK_IOPS = "diskIops";
    public static final String DISK_SIZE = "diskSize";
    public static final String DISK_CONSUMED_FACTOR = "diskConsumedFactor";

    public static Set<String> allowedComputeStats = Sets
        .newHashSet(NUM_OF_CPU, NUM_OF_CORES, CPU_SPEED, CPU_CONSUMED_FACTOR, MEMORY_SIZE,
            MEMORY_CONSUMED_FACTOR, IO_THROUGHPUT, IO_THROUGHPUT_SIZE,
            NETWORK_THROUGHPUT, NETWORK_THROUGHPUT_SIZE);
    public static Set<String> allowedStorageStats = Sets
        .newHashSet(DISK_SIZE, DISK_CONSUMED_FACTOR, DISK_IOPS);
    public static Set<String> allowedStorageTypes = Sets
        .newHashSet("disk", "rdm");
}