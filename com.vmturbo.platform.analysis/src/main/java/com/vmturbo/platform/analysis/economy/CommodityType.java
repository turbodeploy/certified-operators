package com.vmturbo.platform.analysis.economy;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * The type of a commodity. e.g. CPU or Memory.
 *
 * <p>
 *  Together with the quality, this uniquely identifies a commodity sold from a single trader.
 * </p>
 */
public enum CommodityType implements Comparable<CommodityType> {
    // Enumerators
    CPU("MHz"),
    MEMORY("MB"),
    STORAGE("MiB"),
    VCPU("MHz"),
    VMEMORY("MB"),
    VSTORAGE("MiB"),

    ACCESS(""),
    CLUSTER(""),
    SEGMENT("");

    // Fields
    private final String unitOfMeasurement_;

    // Constructors
    private CommodityType(String unitOfMeasurement) {
        unitOfMeasurement_ = unitOfMeasurement;
    }

    // Methods

    /**
     * Returns the unit of measurement used to express amounts and capacities for commodities of this
     * type. e.g. MHz or MB
     */
    public @NonNull String getUnit() {
        return unitOfMeasurement_;
    }

} // end CommodityType enumeration
