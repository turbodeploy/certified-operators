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

    /**
     * Get the CPU capacity value for the given slice.
     *
     * @param slice slice id
     * @return capacity value, or null if missing
     */
    public Double getCpu(long slice) {
        return nullIfMissing(cpu.get(slice));
    }

    public TLongDoubleMap getMem() {
        return mem;
    }

    /**
     * Get the MEM capacity value for the given slice.
     *
     * @param slice slice id
     * @return capacity value, or null if missing
     */
    public Double getMem(long slice) {
        return nullIfMissing(mem.get(slice));
    }

    public TLongDoubleMap getIoThroughput() {
        return io_throughput;
    }

    /**
     * Get the IO THROUGHPUT capacity value for the given slice.
     *
     * @param slice slice id
     * @return capacity value, or null if missing
     */
    public Double getIoThroughput(long slice) {
        return nullIfMissing(io_throughput.get(slice));
    }

    public TLongDoubleMap getNetThroughput() {
        return net_throughput;
    }

    /**
     * Get the NET THROUGHPUT capacity value for the given slice.
     *
     * @param slice slice id
     * @return capacity value, or null if missing
     */
    public Double getNetThroughput(long slice) {
        return nullIfMissing(net_throughput.get(slice));
    }

    public TLongDoubleMap getCpuProvisioned() {
        return cpu_provisioned;
    }

    /**
     * Get the CPU PROVISIONED capacity value for the given slice.
     *
     * @param slice slice id
     * @return capacity value, or null if missing
     */
    public Double getCpuProvisioned(long slice) {
        return nullIfMissing(cpu_provisioned.get(slice));
    }

    public TLongDoubleMap getMemProvisioned() {
        return mem_provisioned;
    }

    /**
     * Get the MEM PROVISIONED capacity value for the given slice.
     *
     * @param slice slice id
     * @return capacity value, or null if missing
     */
    public Double getMemProvisioned(long slice) {
        return nullIfMissing(mem_provisioned.get(slice));
    }

    private Double nullIfMissing(double value) {
        return Double.isNaN(value) ? null : value;
    }
}
