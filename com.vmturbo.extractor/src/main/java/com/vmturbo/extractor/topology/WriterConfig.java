package com.vmturbo.extractor.topology;

import java.util.Set;

import com.google.common.collect.Multimap;

import org.immutables.value.Value;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Configuration information needed by topology writers.
 */
@Value.Immutable
public interface WriterConfig {

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

    /**
     * Specifies the commodity/selling-entity type combos that should be left un-aggregated with
     * respect to commodity keys, when creating bought/sold commodity metric records.
     *
     * @return multimap of qualifying combos
     */
    Multimap<CommodityType, EntityType> unaggregatedCommodities();

    /**
     * Batch size for persisting search tables' temporary records when using JDBC batch inserts.
     *
     * @return batch size for search persistence
     */
    int searchBatchSize();
}
