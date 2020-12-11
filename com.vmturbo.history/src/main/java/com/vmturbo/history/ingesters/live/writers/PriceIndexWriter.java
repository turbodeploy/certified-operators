package com.vmturbo.history.ingesters.live.writers;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.MemReporter;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.RetentionPolicy;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.IChunkProcessor;
import com.vmturbo.history.ingesters.common.TopologyIngesterBase.IngesterState;
import com.vmturbo.history.ingesters.common.writers.ProjectedTopologyWriterBase;
import com.vmturbo.history.schema.HistoryVariety;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.tables.records.AvailableTimestampsRecord;
import com.vmturbo.history.stats.priceindex.DBPriceIndexVisitor.DBPriceIndexVisitorFactory;
import com.vmturbo.history.stats.priceindex.TopologyPriceIndices;

/**
 * Writer that writes records price index data from a projected topology.
 */
public class PriceIndexWriter extends ProjectedTopologyWriterBase implements MemReporter {
    private static final Logger logger = LogManager.getLogger(PriceIndexWriter.class);

    private final TopologyInfo topologyInfo;
    private final IngesterState state;
    private final DBPriceIndexVisitorFactory visitorFactory;
    private final TopologyPriceIndices.Builder indicesBuilder;

    /**
     * Create a new index.
     *  @param topologyInfo   topology info
     * @param state        bulk loader factory
     * @param visitorFactory factory to create visitors over price index data
     * @param entitiesFilter entities filter
     */
    public PriceIndexWriter(@Nonnull TopologyInfo topologyInfo,
            @Nonnull IngesterState state,
            @Nonnull DBPriceIndexVisitorFactory visitorFactory,
            @Nonnull Predicate<TopologyEntityDTO> entitiesFilter) {
        super(entitiesFilter);
        this.topologyInfo = topologyInfo;
        this.state = state;
        this.visitorFactory = visitorFactory;
        this.indicesBuilder = TopologyPriceIndices.builder(topologyInfo);
    }

    @Override
    public ChunkDisposition processEntities(@Nonnull Collection<ProjectedTopologyEntity> chunk,
            @Nonnull String infoSummary) {
        chunk.forEach(indicesBuilder::addEntity);
        return ChunkDisposition.SUCCESS;
    }

    @Override
    public void finish(int entityCount, final boolean expedite, String infoSummary)
            throws InterruptedException {

        if (!expedite) {
            final SimpleBulkLoaderFactory loaders = state.getLoaders();
            final TopologyPriceIndices priceIndices = indicesBuilder.build();
            priceIndices.visit(visitorFactory.newVisitor(topologyInfo, loaders));
            // assuming we wrote any records to entity_stats tables, record this topology's snapshot_time in
            // available_timestamps table for PRICE_DATA
            if (loaders.getStats().getOutTables().stream().anyMatch(t -> EntityType.fromTable(t).isPresent())) {
                AvailableTimestampsRecord record = Tables.AVAILABLE_TIMESTAMPS.newRecord();
                Timestamp snapshot_time = new Timestamp(topologyInfo.getCreationTime());
                record.setTimeStamp(snapshot_time);
                record.setTimeFrame(TimeFrame.LATEST.name());
                record.setHistoryVariety(HistoryVariety.PRICE_DATA.name());
                record.setExpiresAt(
                        Timestamp.from(RetentionPolicy.LATEST_STATS.getExpiration(snapshot_time.toInstant())));
                loaders.getLoader(Tables.AVAILABLE_TIMESTAMPS).insert(record);
            }
        } else {
            logger.warn("Not saving price indexes to allow expedited shutdown");

        }
    }

    /**
     * Factory to create a new price index writer.
     */
    public static class Factory extends ProjectedTopologyWriterBase.Factory {

        private final DBPriceIndexVisitorFactory visitorFactory;
        private final Predicate<TopologyDTO.TopologyEntityDTO> entitiesFilter;

        /**
         * Create a new instance.
         *
         * @param visitorFactory factory to create visitors for price index data
         * @param entitiesFilter entities filter
         */
        public Factory(@Nonnull DBPriceIndexVisitorFactory visitorFactory,
                       @Nonnull Predicate<TopologyDTO.TopologyEntityDTO> entitiesFilter) {
            this.visitorFactory = visitorFactory;
            this.entitiesFilter = entitiesFilter;
        }

        @Override
        public Optional<IChunkProcessor<ProjectedTopologyEntity>>
        getChunkProcessor(@Nonnull final TopologyInfo topologyInfo,
                @Nonnull final IngesterState state) {
            return Optional.of(new PriceIndexWriter(topologyInfo, state, visitorFactory, entitiesFilter));
        }
    }

    @Override
    public Long getMemSize() {
        return null;
    }

    @Override
    public List<MemReporter> getNestedMemReporters() {
        return Arrays.asList(
                new SimpleMemReporter("indicesBuilder", indicesBuilder)
        );
    }
}
