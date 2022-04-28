package com.vmturbo.cost.component.topology;

import static com.vmturbo.cost.component.db.Tables.AGGREGATION_META_DATA;
import static com.vmturbo.cost.component.db.Tables.INGESTED_LIVE_TOPOLOGY;
import static org.jooq.impl.DSL.least;
import static org.jooq.impl.DSL.min;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.SelectJoinStep;
import org.springframework.scheduling.TaskScheduler;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;

/**
 * A store responsible for tracking fully-processed topologies.
 */
public class IngestedTopologyStore {

    private static final List<String> AGGREGATED_TABLE_LIST = ImmutableList.of(
            "entity_cost",
            "reserved_instance_coverage_latest",
            "reserved_instance_utilization_latest");

    private final Logger logger = LogManager.getLogger();

    private final DSLContext dslContext;

    /**
     * Constructs a new {@link IngestedTopologyStore} instance.
     * @param taskScheduler The task scheduler, used to schedule a cleanup procedure.
     * @param cleanupInterval The cleanup interval.
     * @param dslContext The {@link DSLContext}.
     */
    public IngestedTopologyStore(@Nonnull TaskScheduler taskScheduler,
                                 @Nonnull Duration cleanupInterval,
                                 @Nonnull DSLContext dslContext) {

        this.dslContext = Objects.requireNonNull(dslContext);

        taskScheduler.scheduleWithFixedDelay(this::cleanup, cleanupInterval);
    }

    /**
     * Get CreationTime since last topology rollup.
     *
     * @param lastRollupTimes last roll up time from metadata table
     * @return list of CreationTime since last topology rollup
     */
    public List<LocalDateTime> getCreationTimeSinceLastTopologyRollup(
            @Nonnull final Optional<Long> lastRollupTimes) {
        final SelectJoinStep<Record1<LocalDateTime>> step = dslContext.select(
                INGESTED_LIVE_TOPOLOGY.CREATION_TIME).from(INGESTED_LIVE_TOPOLOGY);
        // Using ZoneOffset.UTC to be consistent with `recordIngestedTopology` method
        return lastRollupTimes.map(
                time -> step.where(INGESTED_LIVE_TOPOLOGY.CREATION_TIME.gt(Instant.ofEpochMilli(
                        time).atZone(ZoneOffset.UTC).toLocalDateTime())).fetch()).orElse(
                step.fetch()).stream().map(Record1::value1).collect(Collectors.toList());
    }

    /**
     * Records the ingested topology.
     * @param topologyInfo The topology info.
     */
    public void recordIngestedTopology(@Nonnull TopologyInfo topologyInfo) {

        final LocalDateTime topologyCreationTime = Instant.ofEpochMilli(topologyInfo.getCreationTime())
                .atZone(ZoneOffset.UTC)
                .toLocalDateTime();

        if (topologyInfo.getTopologyType() == TopologyType.REALTIME) {

            dslContext.insertInto(INGESTED_LIVE_TOPOLOGY)
                    .set(INGESTED_LIVE_TOPOLOGY.TOPOLOGY_ID, topologyInfo.getTopologyId())
                    .set(INGESTED_LIVE_TOPOLOGY.CREATION_TIME, topologyCreationTime)
                    .onDuplicateKeyUpdate()
                    .set(INGESTED_LIVE_TOPOLOGY.CREATION_TIME, topologyCreationTime)
                    .execute();

            logger.info("Persisted realtime topology (ID={}, Creation Time={})",
                    topologyInfo.getTopologyId(), topologyCreationTime);
        } else {
            logger.warn("Plan topology not persisted (ID={})", topologyInfo.getTopologyId());
        }
    }

    /**
     * Clean the records in ingested_live_topology table.
     */
    @VisibleForTesting
    public void cleanup() {

        final Optional<LocalDateTime> earliestAggregatedTime = dslContext
                .select(
                        least(
                                min(AGGREGATION_META_DATA.LAST_AGGREGATED_BY_HOUR),
                                min(AGGREGATION_META_DATA.LAST_AGGREGATED_BY_DAY),
                                min(AGGREGATION_META_DATA.LAST_AGGREGATED_BY_MONTH)))
                .from(AGGREGATION_META_DATA)
                .where(AGGREGATION_META_DATA.AGGREGATE_TABLE.in(AGGREGATED_TABLE_LIST))
                .fetchOptional()
                .map(Record1::value1)
                .map(Timestamp::toLocalDateTime);

        if (!earliestAggregatedTime.isPresent()) {
            logger.info("Skipping cleaning ingested live topology record since no aggregation was done");
            return;
        }

        final int numRecordsRemoved = dslContext.deleteFrom(INGESTED_LIVE_TOPOLOGY)
                // While it would be valid to remove any records equal to earliestAggregatedTime,
                // that record is intentionally left in place to aid in debugging
                .where(INGESTED_LIVE_TOPOLOGY.CREATION_TIME.lessThan(earliestAggregatedTime.get()))
                .execute();

        logger.info("Deleted {} ingested live topology record (Earliest Aggregation Time={})",
                numRecordsRemoved, earliestAggregatedTime);
    }
}
