package com.vmturbo.topology.processor.ncm;

/**
 * The {@link FlowsCommonUtils} contains common data structures and methods.
 */
class FlowsCommonUtils {
    /**
     * The flow key prefix.
     */
    static final String KEY_PREFIX = "FLOW-";

    /**
     * The flow keys.
     */
    static final String[] FLOW_KEYS = new String[]{KEY_PREFIX + "0", KEY_PREFIX + "1",
                                                   KEY_PREFIX + "2", KEY_PREFIX + "3"};
}
