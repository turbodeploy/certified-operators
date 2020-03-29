package com.vmturbo.history.ingesters.live.writers;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Functions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.RetentionPolicy;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.IChunkProcessor;
import com.vmturbo.history.ingesters.common.writers.TopologyWriterBase;
import com.vmturbo.history.schema.HistoryVariety;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.tables.records.AvailableTimestampsRecord;
import com.vmturbo.history.stats.live.LiveStatsAggregator;

/**
 * Writer to record entity commodity properties from live topology to stats tables.
 */
public class EntityStatsWriter extends TopologyWriterBase {
    private static Logger logger = LogManager.getLogger(EntityStatsWriter.class);

    private final LiveStatsAggregator aggregator;
    private final SimpleBulkLoaderFactory loaders;
    private final TopologyInfo topologyInfo;

    /**
     * Create a new writer instance.
     *
     * @param topologyInfo         topology info
     * @param commoditiesToExclude set of commodities that are not recorded in stats table
     * @param historydbIO          database utils
     * @param loaders              bulk loader factory
     */
    private EntityStatsWriter(TopologyInfo topologyInfo,
                              Set<String> commoditiesToExclude,
                              HistorydbIO historydbIO,
                              SimpleBulkLoaderFactory loaders) {
        this.loaders = loaders;
        this.topologyInfo = topologyInfo;
        // create an aggregator to do all the real work of matching buyers and sellers and
        // recording stats
        this.aggregator = new LiveStatsAggregator(
            historydbIO, topologyInfo, commoditiesToExclude, loaders);
    }

    @Override
    public ChunkDisposition processEntities(@Nonnull final Collection<TopologyEntityDTO> entities,
                                            @Nonnull final String infoSummary)
        throws InterruptedException {

        final Map<Long, TopologyEntityDTO> entityByOid = entities.stream()
            .collect(Collectors.toMap(TopologyEntityDTO::getOid, Functions.identity()));
        for (TopologyEntityDTO entity : entities) {
            aggregator.aggregateEntity(entity, entityByOid);
        }
        return ChunkDisposition.SUCCESS;
    }

    @Override
    public void finish(int entityCount, boolean expedite, String infoSummary)
        throws InterruptedException {

        if (!expedite) {
            try {
                aggregator.writeFinalStats();
            } catch (VmtDbException e) {
                logger.warn("EntityStatsWriter failed to record final stats for topology {}",
                    infoSummary);
            }
            // assuming we wrote any records to entity_stats tables, record this topology's snapshot_time in
            // available_timestamps table
            if (loaders.getStats().getOutTables().stream().anyMatch(t -> EntityType.fromTable(t) != null)) {
                AvailableTimestampsRecord record = Tables.AVAILABLE_TIMESTAMPS.newRecord();
                Timestamp snapshot_time = new Timestamp(topologyInfo.getCreationTime());
                record.setTimeStamp(snapshot_time);
                record.setTimeFrame(TimeFrame.LATEST.name());
                record.setHistoryVariety(HistoryVariety.ENTITY_STATS.name());
                record.setExpiresAt(
                    Timestamp.from(RetentionPolicy.LATEST_STATS.getExpiration(snapshot_time.toInstant())));
                loaders.getLoader(Tables.AVAILABLE_TIMESTAMPS).insert(record);
            }
        }
    }

    /**
     * Factory to create a new {@link EntityStatsWriter}.
     */
    public static class Factory extends TopologyWriterBase.Factory {
        private final HistorydbIO historydbIO;
        private final Set<String> commoditiesToExclude;

        /**
         * Create a new factory instance.
         *
         * @param historydbIO          database utils
         * @param commoditiesToExclude commodities to be excluded from stats tables
         */
        public Factory(HistorydbIO historydbIO, Set<String> commoditiesToExclude) {
            this.historydbIO = historydbIO;
            this.commoditiesToExclude = commoditiesToExclude;
        }

        @Override
        public Optional<IChunkProcessor<Topology.DataSegment>>
        getChunkProcessor(final TopologyInfo topologyInfo,
                          SimpleBulkLoaderFactory loaders) {
            return Optional.of(new EntityStatsWriter(
                topologyInfo, commoditiesToExclude, historydbIO, loaders));
        }
    }
}
