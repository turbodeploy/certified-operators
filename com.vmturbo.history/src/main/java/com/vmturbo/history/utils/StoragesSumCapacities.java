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

    public TLongDoubleMap getStorageProvisioned() {
        return storage_provisioned;
    }

    public TLongDoubleMap getStorageAmount() {
        return storage_amount;
    }
}
