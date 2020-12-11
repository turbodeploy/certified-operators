package com.vmturbo.history.ingesters.live.writers;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.MemReporter;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.RetentionPolicy;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.ingesters.common.IChunkProcessor;
import com.vmturbo.history.ingesters.common.TopologyIngesterBase.IngesterState;
import com.vmturbo.history.ingesters.common.writers.TopologyWriterBase;
import com.vmturbo.history.schema.HistoryVariety;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.tables.records.AvailableTimestampsRecord;
import com.vmturbo.history.stats.live.LiveStatsAggregator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Writer to record entity commodity properties from live topology to stats tables.
 */
public class EntityStatsWriter extends TopologyWriterBase implements MemReporter {
    private static final Logger logger = LogManager.getLogger(EntityStatsWriter.class);

    private final TopologyInfo topologyInfo;
    private final Set<CommodityType> excludedCommodityTypes;
    private final HistorydbIO historydbIO;
    private final IngesterState state;
    private LiveStatsAggregator aggregator;

    /**
     * Create a new writer instance.
     *
     * @param topologyInfo           topology info
     * @param excludedCommodityTypes set of commodities that are not recorded in stats table
     * @param historydbIO            database utils
     * @param state                  shared ingester state
     * @param entitiesFilter         entities filter
     */
    private EntityStatsWriter(TopologyInfo topologyInfo,
            Set<CommodityType> excludedCommodityTypes,
            HistorydbIO historydbIO,
            IngesterState state,
            Predicate<TopologyEntityDTO> entitiesFilter) {
        super(entitiesFilter);
        this.topologyInfo = topologyInfo;
        this.excludedCommodityTypes = excludedCommodityTypes;
        this.historydbIO = historydbIO;
        this.state = state;
    }

    @VisibleForTesting
    LiveStatsAggregator getAggregator() {
        if (aggregator == null) {
            this.aggregator = new LiveStatsAggregator(
                    historydbIO, topologyInfo, excludedCommodityTypes, state);
        }
        return aggregator;
    }

    @Override
    public ChunkDisposition processEntities(@Nonnull final Collection<TopologyEntityDTO> entities,
                                            @Nonnull final String infoSummary)
        throws InterruptedException {

        final Map<Long, TopologyEntityDTO> entityByOid = entities.stream()
            .collect(Collectors.toMap(TopologyEntityDTO::getOid, Functions.identity()));
        for (TopologyEntityDTO entity : entities) {
            getAggregator().aggregateEntity(entity, entityByOid);
        }
        return ChunkDisposition.SUCCESS;
    }

    @Override
    public void finish(int entityCount, boolean expedite, String infoSummary)
        throws InterruptedException {

        if (!expedite) {
            try {
                getAggregator().writeFinalStats();
                getAggregator().logShortenedCommodityKeys();
            } catch (VmtDbException e) {
                logger.warn("EntityStatsWriter failed to record final stats for topology {}",
                    infoSummary);
            }
            // assuming we wrote any records to entity_stats tables, record this topology's snapshot_time in
            // available_timestamps table
            if (state.getLoaders().getStats().getOutTables().stream().anyMatch(t -> EntityType.fromTable(t).isPresent())) {
                AvailableTimestampsRecord record = Tables.AVAILABLE_TIMESTAMPS.newRecord();
                Timestamp snapshot_time = new Timestamp(topologyInfo.getCreationTime());
                record.setTimeStamp(snapshot_time);
                record.setTimeFrame(TimeFrame.LATEST.name());
                record.setHistoryVariety(HistoryVariety.ENTITY_STATS.name());
                record.setExpiresAt(
                    Timestamp.from(RetentionPolicy.LATEST_STATS.getExpiration(snapshot_time.toInstant())));
                state.getLoaders().getLoader(Tables.AVAILABLE_TIMESTAMPS).insert(record);
            }
        }
    }

    /**
     * Factory to create a new {@link EntityStatsWriter}.
     */
    public static class Factory extends TopologyWriterBase.Factory {
        private final HistorydbIO historydbIO;
        private final Set<CommodityType> excludedCommodityTypes;
        private final Predicate<TopologyEntityDTO> entitiesFilter;

        /**
         * Create a new factory instance.
         *
         * @param historydbIO            database utils
         * @param excludedCommodityTypes commodities to be excluded from stats tables
         * @param entitiesFilter         entities filter
         */
        public Factory(HistorydbIO historydbIO, Set<CommodityType> excludedCommodityTypes,
                Predicate<TopologyEntityDTO> entitiesFilter) {
            this.historydbIO = historydbIO;
            this.excludedCommodityTypes = excludedCommodityTypes;
            this.entitiesFilter = entitiesFilter;
        }

        @Override
        public Optional<IChunkProcessor<Topology.DataSegment>>
        getChunkProcessor(final TopologyInfo topologyInfo,
                          IngesterState state) {
            return Optional.of(new EntityStatsWriter(
                    topologyInfo, excludedCommodityTypes, historydbIO, state, entitiesFilter));
        }
    }

    @Override
    public Long getMemSize() {
        return null;
    }

    @Override
    public List<MemReporter> getNestedMemReporters() {
        return Arrays.asList(
                aggregator
        );
    }
}
