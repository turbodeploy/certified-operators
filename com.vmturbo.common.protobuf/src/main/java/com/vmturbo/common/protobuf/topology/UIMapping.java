package com.vmturbo.common.protobuf.topology;

import com.google.common.collect.ImmutableBiMap;

/**
 * Backend to UI mapping.
 */
public class UIMapping {

    private UIMapping() {}

    /**
     * This is currently required because the SDK probes have the category field inconsistently cased.
     */
    private static final ImmutableBiMap<String, String> USER_FACING_CATEGORY_MAP = ImmutableBiMap
        .<String, String>builder()
        .put("STORAGE", "Storage")
        .put("HYPERVISOR", "Hypervisor")
        .put("FABRIC", "Fabric")
        .put("ORCHESTRATOR", "Orchestrator")
        .put("HYPERCONVERGED", "Hyperconverged")
        .build();

    /**
     * Probe category strings as defined by the difference probe_conf.xml are not consistent
     * regarding uppercase/lowercase, etc. This maps the known problem category strings into
     * more user-friendly names.
     *
     * @param category the probe category
     * @return a user-friendly string for the probe category (for the problem categories we know of).
     */
    public static String getUserFacingCategoryString(final String category) {
        return USER_FACING_CATEGORY_MAP.getOrDefault(category, category);
    }
}
