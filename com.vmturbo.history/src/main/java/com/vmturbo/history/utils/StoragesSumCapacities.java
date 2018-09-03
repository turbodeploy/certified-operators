package com.vmturbo.history.utils;

import java.util.Map;
import com.google.common.collect.Maps;

/**
 * The class StoragesSumCapacities stores the sums of capacities per slice for the commodities
 * sold by storages.
 */
public class StoragesSumCapacities {
    private static Map<String, Double> storage_access = Maps.newHashMap();
    private static Map<String, Double> storage_provisioned = Maps.newHashMap();

    public static Map<String, Double> getStorageAccess() { return storage_access; }
    public static Map<String, Double> getStorageProvisioned() { return storage_provisioned; }

    public static void setStorageAccess(Map<String, Double> storage_access) { StoragesSumCapacities.storage_access = storage_access; }
    public static void setStorageProvisioned(Map<String, Double> storage_provisioned) { StoragesSumCapacities.storage_provisioned = storage_provisioned; }

    public static void init() {
        storage_access.clear();
        storage_provisioned.clear();
    }
}
