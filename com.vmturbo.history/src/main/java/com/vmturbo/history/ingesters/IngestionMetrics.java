package com.vmturbo.history.ingesters;

import com.google.common.base.CaseFormat;

import com.vmturbo.proactivesupport.DataMetricCounter;

/**
 * Metrics specifically related to ingestion and rollup processing in history component.
 */
public class IngestionMetrics {

    /**
     * This class should not be instantiated.
     */
    private IngestionMetrics() {
    }

    /** Safety valve metrics label for valve name. */
    public static final String VALVE_NAME_LABEL = "valve_name";

    /** Safety valve metrics label for tag. */
    public static final String TAG_LABEL = "tag";

    /**
     * Counter to track instances where "safety valves" have been activated in order to mitigate
     * damage from some problematic situation that may arise in ingestion/rollup processing.
     *
     * <p>Any single occurrence of any of these safety valves is cause for concern. A scenario
     * where activations are being recorded repeatedly is especailly worrying.</p>
     *
     * <p>A useful metaphor is water dripping from the ceiling. There could be a variety of
     * root causes, and a single unexplained drip is worth attention. A steady drip is
     * especially deserving of attention, and you probably ought to put a bucket under it
     * to contain the potential damage to the floor, furniture, etc. The safety valves
     * built into history component spot "leaks" and put "buckets" under them, to limit damage
     * without diagnosing or addressing the actual root cause.</p>
     */
    public static final DataMetricCounter SAFETY_VALVE_ACTIVATION_COUNTER = DataMetricCounter.builder()
            .withName("health:history_safety_valves")
            .withHelp("Activation counts of history component safety valves")
            .withLabelNames(VALVE_NAME_LABEL, TAG_LABEL)
            .build()
            .register();

    /**
     * Enumeration of implemented safety valves.
     */
    public enum SafetyValve {
        /** Repartitioning not running frequently enough. */
        REPARTITION,
        /** Unresolved snapshot force-resolved to allow hourly rollups to proceed. */
        RESOLVE_SNAPSHOT,
        /** Received topology timed out waiting for a processing directive. */
        SKIP_TOPOLOGY;

        /**
         * Produce a suitable value for the VALVE_NAME label.
         *
         * @return valve name
         */
        public String getLabel() {
            return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_HYPHEN, name());
        }
    }
}
