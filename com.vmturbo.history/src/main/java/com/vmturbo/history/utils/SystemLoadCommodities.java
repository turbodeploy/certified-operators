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
 * The system load commodities are 11 commodities chosen to participate in the system load
 * calculations.
 */
public enum SystemLoadCommodities {
    CPU(0),
    MEM(1),
    STORAGE_ACCESS(2),
    IO_THROUGHPUT(3),
    NET_THROUGHPUT(4),
    CPU_PROVISIONED(5),
    MEM_PROVISIONED(6),
    STORAGE_PROVISIONED(7),
    VMEM(8),
    VCPU(9),
    VSTORAGE(10);

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
            CommodityType.VSTORAGE.ordinal()
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
        if (comm.equals("CPU"))
            return SystemLoadCommodities.CPU;
        if (comm.equals("MEM"))
            return SystemLoadCommodities.MEM;
        if (comm.equals("STORAGE_ACCESS"))
            return SystemLoadCommodities.STORAGE_ACCESS;
        if (comm.equals("IO_THROUGHPUT"))
            return SystemLoadCommodities.IO_THROUGHPUT;
        if (comm.equals("NET_THROUGHPUT"))
            return SystemLoadCommodities.NET_THROUGHPUT;
        if (comm.equals("CPU_PROVISIONED"))
            return SystemLoadCommodities.CPU_PROVISIONED;
        if (comm.equals("MEM_PROVISIONED"))
            return SystemLoadCommodities.MEM_PROVISIONED;
        if (comm.equals("STORAGE_PROVISIONED"))
            return SystemLoadCommodities.STORAGE_PROVISIONED;
        if (comm.equals("VMEM"))
            return SystemLoadCommodities.VMEM;
        if (comm.equals("VCPU"))
            return SystemLoadCommodities.VCPU;
        if (comm.equals("VSTORAGE"))
            return SystemLoadCommodities.VSTORAGE;
        return null;
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
                || (commType == CommodityType.STORAGE_PROVISIONED.ordinal())) {
            return true;
        }
        return false;
    }

}
