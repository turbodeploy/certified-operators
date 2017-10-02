package com.vmturbo.topology.processor.identity;

/**
 * The HeuristicsDescriptor implements service entity heuristics.
 */
public interface HeuristicsDescriptor {

    /**
     * Policy to use when matching heuristic properties.
     */
    enum POLICY {
        /**
         * Use the heuristics threshold provided by the probe.
         */
        AMOUNT_MATCH_DEFAULT
    }

    /**
     * Returns the heuristics policy.
     *
     * @return The heuristics policy.
     */
    POLICY getPolicy();
}
