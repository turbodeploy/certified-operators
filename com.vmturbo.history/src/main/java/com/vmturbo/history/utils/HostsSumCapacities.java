package com.vmturbo.history.utils;

import java.util.Map;

import com.google.common.collect.Maps;

/**
 * The class HostsSumCapacities stores the sums of capacities per slice for the commodities
 * sold by hosts.
 */
// TODO should this really be a collection of static maps, with no coordiantion of read/write access?
public class HostsSumCapacities {
    private static Map<String, Double> cpu = Maps.newHashMap();
    private static Map<String, Double> mem = Maps.newHashMap();
    private static Map<String, Double> io_throughput = Maps.newHashMap();
    private static Map<String, Double> net_throughput = Maps.newHashMap();
    private static Map<String, Double> cpu_provisioned = Maps.newHashMap();
    private static Map<String, Double> mem_provisioned = Maps.newHashMap();

    public static Map<String, Double> getCpu() { return cpu; }
    public static Map<String, Double> getMem() { return mem; }
    public static Map<String, Double> getIoThroughput() { return io_throughput; }
    public static Map<String, Double> getNetThroughput() { return net_throughput; }
    public static Map<String, Double> getCpuProvisioned() { return cpu_provisioned; }
    public static Map<String, Double> getMemProvisioned() { return mem_provisioned; }

    public static void setCpu(Map<String, Double> cpu) { HostsSumCapacities.cpu = cpu; }
    public static void setMem(Map<String, Double> mem) { HostsSumCapacities.mem = mem; }
    public static void setIoThroughput(Map<String, Double> io_throughput) { HostsSumCapacities.io_throughput = io_throughput; }
    public static void setNetThroughput(Map<String, Double> net_throughput) { HostsSumCapacities.net_throughput = net_throughput; }
    public static void setCpuProvisioned(Map<String, Double> cpu_provisioned) { HostsSumCapacities.cpu_provisioned = cpu_provisioned; }
    public static void setMemProvisioned(Map<String, Double> mem_provisioned) { HostsSumCapacities.mem_provisioned = mem_provisioned; }

    public static void init() {
        cpu.clear();
        mem.clear();
        io_throughput.clear();
        net_throughput.clear();
        cpu_provisioned.clear();
        mem_provisioned.clear();
    }
}
