package com.vmturbo.history.utils;

import static gnu.trove.impl.Constants.DEFAULT_CAPACITY;
import static gnu.trove.impl.Constants.DEFAULT_LOAD_FACTOR;

import gnu.trove.map.TLongDoubleMap;
import gnu.trove.map.hash.TLongDoubleHashMap;

/**
 * The class StoragesSumCapacities stores the sums of capacities per slice for the commodities
 * sold by storages.
 */
public class StoragesSumCapacities {
    private TLongDoubleMap storage_access = new TLongDoubleHashMap(
            DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, -1L, Double.NaN);
    private TLongDoubleMap storage_provisioned = new TLongDoubleHashMap(
            DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, -1L, Double.NaN);
    private TLongDoubleMap storage_amount = new TLongDoubleHashMap(
            DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, -1L, Double.NaN);

    public TLongDoubleMap getStorageAccess() {
        return storage_access;
    }

    /**
     * Get the STORAGE ACCESS capacity value for the given slice.
     *
     * @param slice slice id
     * @return capacity value, or null if missing
     */
    public Double getStorageAccess(long slice) {
        return nullIfMissing(storage_access.get(slice));
    }

    public TLongDoubleMap getStorageProvisioned() {
        return storage_provisioned;
    }

    /**
     * Get the STORAGE PROVISIONED capacity value for the given slice.
     *
     * @param slice slice id
     * @return capacity value, or null if missing
     */
    public Double getStorageProvisioned(long slice) {
        return nullIfMissing(storage_provisioned.get(slice));
    }

    public TLongDoubleMap getStorageAmount() {
        return storage_amount;
    }

    /**
     * Get the STORAGE AMOUNT capacity value for the given slice.
     *
     * @param slice slice id
     * @return capacity value, or null if missing
     */
    public Double getStorageAmount(long slice) {
        return nullIfMissing(storage_amount.get(slice));
    }

    private Double nullIfMissing(double value) {
        return Double.isNaN(value) ? null : value;
    }
}
