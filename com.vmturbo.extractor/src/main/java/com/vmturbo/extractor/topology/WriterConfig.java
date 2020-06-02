package com.vmturbo.extractor.topology;

import java.util.Set;

import org.immutables.value.Value;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Configuration information needed by topology writers.
 */
@Value.Immutable
public interface WriterConfig {

    /**
     * Specifies how often the lastSeen values should be updated for entities that have appeared
     * in topologies processed since the last update.
     *
     * <p>LastSeen values for newly encountered (or changed) entities are set to the time at which
     * the next update is expected to occur, plus some fuzz.</p>
     *
     * @return update interval in minutes
     */
    int lastSeenUpdateIntervalMinutes();

    /**
     * Specifies an additional amount of time to add to the lastSeen values saved in entity records
     * written in a given cycle, to ensure the entities will be considered for later queries until
     * the next update occurs.
     *
     * @return additional time in minutes
     */
    int lastSeenAdditionalFuzzMinutes();

    /**
     * Specifies the max amount of time to wait for records insertion to complete after
     * the last record in a stream has been sent.
     *
     * @return time limit in seconds
     */
    int insertTimeoutSeconds();

    /**
     * Specifies the whitelist of commodities to write to db for reporting.
     *
     * @return set of {@link CommodityType}s in the number format
     */
    Set<Integer> reportingCommodityWhitelist();
}
