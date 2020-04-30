package com.vmturbo.history.ingesters.live.writers;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Commodities that participate in system load calculation.
 */
public enum SystemLoadCommodity {
    /** CPU commodity. */
    CPU(CommodityType.CPU),
    /** Memory commodity. */
    MEM(CommodityType.MEM),
    /** Storage access commodity. */
    STORAGE_ACCESS(CommodityType.STORAGE_ACCESS),
    /** IO throughput commodity. */
    IO_THROUGHPUT(CommodityType.IO_THROUGHPUT),
    /** Network throughput commodity. */
    NET_THROUGHPUT(CommodityType.NET_THROUGHPUT),
    /** Provisioned CPU commodity. */
    CPU_PROVISIONED(CommodityType.CPU_PROVISIONED),
    /** Provisioned memory commodity. */
    MEM_PROVISIONED(CommodityType.MEM_PROVISIONED),
    /** Provisioned storage commodity. */
    STORAGE_PROVISIONED(CommodityType.STORAGE_PROVISIONED),
    /** Virtual memory commodity. */
    VMEM(CommodityType.VMEM),
    /** Virtual CPU commodity. */
    VCPU(CommodityType.VCPU),
    /** Storage amount commodity. */
    STORAGE_AMOUNT(CommodityType.STORAGE_AMOUNT);

    private static final Map<Integer, SystemLoadCommodity> SDK_TO_SYSLOAD_COMMODITY_TYPE_MAP =
            Arrays.stream(values()).collect(ImmutableMap.toImmutableMap(
                    type -> type.sdkType.getNumber(), Function.identity()));

    private final CommodityType sdkType;

    SystemLoadCommodity(CommodityType commodityType) {
        this.sdkType = commodityType;
    }

    /**
     * Get the system load commodity corresponding to the given SDK commodity type, if there
     * is one.
     *
     * @param sdkCommodityTypeNo SDK commodity type number
     * @return correspoding system load commodity, if any
     */
    public static Optional<SystemLoadCommodity> fromSdkCommodityType(int sdkCommodityTypeNo) {
        return Optional.ofNullable(SDK_TO_SYSLOAD_COMMODITY_TYPE_MAP.get(sdkCommodityTypeNo));
    }

    public CommodityType getSdkType() {
        return sdkType;
    }
}

