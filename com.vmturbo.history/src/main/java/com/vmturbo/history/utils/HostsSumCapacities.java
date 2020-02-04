package com.vmturbo.history.utils;

import static gnu.trove.impl.Constants.DEFAULT_CAPACITY;
import static gnu.trove.impl.Constants.DEFAULT_LOAD_FACTOR;

import gnu.trove.map.TLongDoubleMap;
import gnu.trove.map.hash.TLongDoubleHashMap;

/**
 * The class HostsSumCapacities stores the sums of capacities per slice for the commodities
 * sold by hosts.
 */
public class HostsSumCapacities {
    private TLongDoubleMap cpu = new TLongDoubleHashMap(
            DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, -1L, Double.NaN);
    private TLongDoubleMap mem = new TLongDoubleHashMap(
            DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, -1L, Double.NaN);
    private TLongDoubleMap io_throughput = new TLongDoubleHashMap(
            DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, -1L, Double.NaN);
    private TLongDoubleMap net_throughput = new TLongDoubleHashMap(
            DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, -1L, Double.NaN);
    private TLongDoubleMap cpu_provisioned = new TLongDoubleHashMap(
            DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, -1L, Double.NaN);
    private TLongDoubleMap mem_provisioned = new TLongDoubleHashMap(
            DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, -1L, Double.NaN);

    public TLongDoubleMap getCpu() {
        return cpu;
    }

    public TLongDoubleMap getMem() {
        return mem;
    }

    public TLongDoubleMap getIoThroughput() {
        return io_throughput;
    }

    public TLongDoubleMap getNetThroughput() {
        return net_throughput;
    }

    public TLongDoubleMap getCpuProvisioned() {
        return cpu_provisioned;
    }

    public TLongDoubleMap getMemProvisioned() {
        return mem_provisioned;
    }
}
