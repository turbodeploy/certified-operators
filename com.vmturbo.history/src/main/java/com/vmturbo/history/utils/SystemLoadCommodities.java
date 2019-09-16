package com.vmturbo.history.utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * The system load commodities are commodities chosen to participate in the system load
 * calculations.
 *
 * <p>The member names in this enum must be identical to those appearing in the
 * {@link CommodityType} enum. (The class will fail initializaiton if that that is not the case,
 * because the expression computing static field hashMapComm will throw IllegalArgumentException.)
 * </p>
 */
public enum SystemLoadCommodities {
    /** CPU commodity. */
    CPU,
    /** Memory commodity. */
    MEM,
    /** Storage access commodity. */
    STORAGE_ACCESS,
    /** IO throughput commodity. */
    IO_THROUGHPUT,
    /** Network throughput commodity. */
    NET_THROUGHPUT,
    /** Provisioned CPU commodity. */
    CPU_PROVISIONED,
    /** Provisioned memory commodity. */
    MEM_PROVISIONED,
    /** Provisioned storage commodity. */
    STORAGE_PROVISIONED,
    /** Virtual memory commodity. */
    VMEM,
    /** Virtual CPU commodity. */
    VCPU,
    /** Storage amount commodity. */
    STORAGE_AMOUNT;

    private static final Logger logger = LogManager.getLogger();

    /** Number of system load commodities. */
    public static final int SIZE = values().length;

    /**
     * Map to translate an enum ordinal to the enum value.
     */
    private static final Map<Integer, SystemLoadCommodities> IDX_MAP =
        Maps.uniqueIndex(Arrays.asList(values()), Enum::ordinal);

    /**
     * Commodity types that have been presented as system load commodities but are not.
     *
     * <p>This typically happens when loading system load data from the database, if the
     * list of system load commodities has changed since the data was saved.</p>
     *
     * <p>We use this list to avoid logging the same message more than once.</p>
     */
    private static Set<String> unknownCommodities = new HashSet<>();

    /**
     * Get the system load commodity that belongs at a given position in an array of commodity
     * values.
     *
     * @param idx position in array
     * @return commodity at that position, or none if the position is not used.
     */
    public static SystemLoadCommodities get(int idx) {
        return IDX_MAP.get(idx);
    }

    /**
     * A map linking {@link CommodityType} type values to {@link SystemLoadCommodities} members.
     */
    private static final Map<Integer, SystemLoadCommodities> hashMapComm =
        Stream.of(SystemLoadCommodities.values())
            .collect(Collectors.toMap(
                c -> CommodityType.valueOf(c.name()).ordinal(),
                Functions.identity()));

    /**
     * The method takes as input an integer representing a
     * commodity type according to the CommodityType enumeration and checks if it is
     * a system load commodity.
     *
     * @param commType The commodity type in CommodityType enumeration.
     * @return True if it is a system load commodity, false otherwise.
     */
    public static boolean isSystemLoadCommodity(int commType) {
        return hashMapComm.containsKey(commType);
    }

    /**
     * This method indicates whether the given name is the name of system load commodity.
     *
     * @param name name to test
     * @return true if it is the name of a system load commodity
     */
    public static boolean isSystemLoadCommodity(String name) {
        return toSystemLoadCommodity(name) != null;
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
     * @return The respective system load commodity, or null if this is not a system load commodity
     */
    public static Optional<SystemLoadCommodities> toSystemLoadCommodity(String comm) {
        try {
            return Optional.of(SystemLoadCommodities.valueOf(comm));
        } catch (IllegalArgumentException e) {
            if (!unknownCommodities.contains(comm)) {
                logger.warn("Ignoring unknown system load commodity {}", comm);
                unknownCommodities.add(comm);
            }
            return Optional.empty();
        }
    }

    private static final Set<Integer> HOST_SOLD_COMMODITIES = ImmutableSet.<Integer>builder()
        .add(CommodityType.CPU.ordinal())
        .add(CommodityType.MEM.ordinal())
        .add(CommodityType.IO_THROUGHPUT.ordinal())
        .add(CommodityType.NET_THROUGHPUT.ordinal())
        .add(CommodityType.CPU_PROVISIONED.ordinal())
        .add(CommodityType.MEM_PROVISIONED.ordinal())
        .build();
    /**
     * The method checks if a commodity is sold by a host.
     *
     * @param commType The commodity to be checked.
     * @return True if it is sold by a host, otherwise false.
     */
    public static boolean isHostCommodity(int commType) {
        return HOST_SOLD_COMMODITIES.contains(commType);
    }


    private static final Set<Integer> STORAGE_SOLD_COMMODITIES = ImmutableSet.<Integer>builder()
        .add(CommodityType.STORAGE_ACCESS.ordinal())
        .add(CommodityType.STORAGE_PROVISIONED.ordinal())
        .add(CommodityType.STORAGE_AMOUNT.ordinal())
        .build();
    /**
     * The method checks if a commodity is sold by a storage.
     *
     * @param commType The commodity to be checked.
     * @return True if it is sold by a storage, otherwise false.
     */
    public static boolean isStorageCommodity(int commType) {
        return STORAGE_SOLD_COMMODITIES.contains(commType);
    }
}
