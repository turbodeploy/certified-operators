package com.vmturbo.history.utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import javax.annotation.Nullable;


import com.google.common.collect.Maps;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * The system load commodities are 12 commodities chosen to participate in the system load
 * calculations.
 */
public enum SystemLoadCommodities {
    /** CPU commodity. */
    CPU(0),
    /** Memory commodity. */
    MEM(1),
    /** Storage access commodity. */
    STORAGE_ACCESS(2),
    /** IO throughput commodity. */
    IO_THROUGHPUT(3),
    /** Net throughput commodity. */
    NET_THROUGHPUT(4),
    /** CPU provisioned commodity. */
    CPU_PROVISIONED(5),
    /** Memory provisioned commodity. */
    MEM_PROVISIONED(6),
    /** Storage provisioned commodity. */
    STORAGE_PROVISIONED(7),
    /** Virtual memory commodity. */
    VMEM(8),
    /** Virtual CPU commodity. */
    VCPU(9),
    /** Virtual storage commodity. */
    VSTORAGE(10),
    /** Storage amount commodity. */
    STORAGE_AMOUNT(11);

    public static final int SIZE = values().length;

    public final int idx;

    SystemLoadCommodities(int value) {
        this.idx = value;
    }

    private static final Map<Integer, SystemLoadCommodities> IDX_MAP =
            Maps.uniqueIndex(Arrays.asList(values()), commodities -> commodities.idx);

    public static SystemLoadCommodities get(int idx) { return IDX_MAP.get(idx); }

    public static final Set<Integer> hashSetComm = new HashSet<>(Arrays.asList(
            CommodityType.CPU.ordinal(),
            CommodityType.MEM.ordinal(),
            CommodityType.STORAGE_ACCESS.ordinal(),
            CommodityType.IO_THROUGHPUT.ordinal(),
            CommodityType.NET_THROUGHPUT.ordinal(),
            CommodityType.CPU_PROVISIONED.ordinal(),
            CommodityType.MEM_PROVISIONED.ordinal(),
            CommodityType.STORAGE_PROVISIONED.ordinal(),
            CommodityType.VMEM.ordinal(),
            CommodityType.VCPU.ordinal(),
            CommodityType.VSTORAGE.ordinal(),
            CommodityType.STORAGE_AMOUNT.ordinal()
    ));

    public static final HashMap<Integer, SystemLoadCommodities> hashMapComm = new HashMap<Integer, SystemLoadCommodities>() {{
        put(CommodityType.CPU.ordinal(), SystemLoadCommodities.CPU);
        put(CommodityType.MEM.ordinal(), SystemLoadCommodities.MEM);
        put(CommodityType.STORAGE_ACCESS.ordinal(), SystemLoadCommodities.STORAGE_ACCESS);
        put(CommodityType.IO_THROUGHPUT.ordinal(), SystemLoadCommodities.IO_THROUGHPUT);
        put(CommodityType.NET_THROUGHPUT.ordinal(), SystemLoadCommodities.NET_THROUGHPUT);
        put(CommodityType.CPU_PROVISIONED.ordinal(), SystemLoadCommodities.CPU_PROVISIONED);
        put(CommodityType.MEM_PROVISIONED.ordinal(), SystemLoadCommodities.MEM_PROVISIONED);
        put(CommodityType.STORAGE_PROVISIONED.ordinal(), SystemLoadCommodities.STORAGE_PROVISIONED);
        put(CommodityType.VMEM.ordinal(), SystemLoadCommodities.VMEM);
        put(CommodityType.VCPU.ordinal(), SystemLoadCommodities.VCPU);
        put(CommodityType.VSTORAGE.ordinal(), SystemLoadCommodities.VSTORAGE);
        put(CommodityType.STORAGE_AMOUNT.ordinal(), SystemLoadCommodities.STORAGE_AMOUNT);
    }};

    /**
     * The method takes as input an integer representing a
     * commodity type according to the CommodityType enumeration and checks if it is
     * a system load commodity.
     *
     * @param commType The commodity type in CommodityType enumeration.
     * @return True if it is a system load commodity, false otherwise.
     */
    public static boolean isSystemLoadCommodity(int commType) {
        return hashSetComm.contains(commType);
    }

    /**
     * The method takes as input an integer representing commodity type according to the
     * CommodityType enumeration and converts it to a system load commodity.
     *
     * @param commType The commodity type in CommodityType enumeration.
     * @return The respective system load commodity.
     */
    @Nullable
    public static SystemLoadCommodities toSystemLoadCommodity(int commType) {
        return hashMapComm.get(commType);
    }

    /**
     * The method takes as input a string with the name of the commodity and converts it
     * to a system load commodity.
     *
     * @param comm The name of the commodity.
     * @return The respective system load commodity.
     */
    public static SystemLoadCommodities toSystemLoadCommodity(String comm) {
        switch (comm) {
            case "CPU":
                return SystemLoadCommodities.CPU;
            case "MEM":
                return SystemLoadCommodities.MEM;
            case "STORAGE_ACCESS":
                return SystemLoadCommodities.STORAGE_ACCESS;
            case "IO_THROUGHPUT":
                return SystemLoadCommodities.IO_THROUGHPUT;
            case "NET_THROUGHPUT":
                return SystemLoadCommodities.NET_THROUGHPUT;
            case "CPU_PROVISIONED":
                return SystemLoadCommodities.CPU_PROVISIONED;
            case "MEM_PROVISIONED":
                return SystemLoadCommodities.MEM_PROVISIONED;
            case "STORAGE_PROVISIONED":
                return SystemLoadCommodities.STORAGE_PROVISIONED;
            case "VMEM":
                return SystemLoadCommodities.VMEM;
            case "VCPU":
                return SystemLoadCommodities.VCPU;
            case "VSTORAGE":
                return SystemLoadCommodities.VSTORAGE;
            case "STORAGE_AMOUNT":
                return SystemLoadCommodities.STORAGE_AMOUNT;
            default:
                return null;
        }
    }

    /**
     * The method checks if a commodity is sold by a host.
     *
     * @param commType The commodity to be checked.
     * @return True if it is sold by a host, otherwise false.
     */
    public static boolean isHostCommodity(int commType) {
        if ((commType == CommodityType.CPU.ordinal())
                || (commType == CommodityType.MEM.ordinal())
                || (commType == CommodityType.IO_THROUGHPUT.ordinal())
                || (commType == CommodityType.NET_THROUGHPUT.ordinal())
                || (commType == CommodityType.CPU_PROVISIONED.ordinal())
                || (commType == CommodityType.MEM_PROVISIONED.ordinal())) {
            return true;
        }
        return false;
    }

    /**
     * The method checks if a commodity is sold by a storage.
     *
     * @param commType The commodity to be checked.
     * @return True if it is sold by a storage, otherwise false.
     */
    public static boolean isStorageCommodity(int commType) {
        if ((commType == CommodityType.STORAGE_ACCESS.ordinal())
                || (commType == CommodityType.STORAGE_PROVISIONED.ordinal())
                || (commType == CommodityType.STORAGE_AMOUNT.ordinal())) {
            return true;
        }
        return false;
    }

}
