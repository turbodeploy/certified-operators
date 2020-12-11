package com.vmturbo.history.ingesters.plan.writers;

import java.util.Collection;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.collect.Collections2;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.BulkLoader;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.IChunkProcessor;
import com.vmturbo.history.ingesters.common.writers.ProjectedTopologyWriterBase;
import com.vmturbo.history.schema.abstraction.tables.MktSnapshotsStats;
import com.vmturbo.history.schema.abstraction.tables.records.MktSnapshotsStatsRecord;
import com.vmturbo.history.schema.abstraction.tables.records.ScenariosRecord;
import com.vmturbo.history.stats.PlanStatsAggregator;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Class to write stats data while ingesting a projected plan topology broadcast by market.
 */
public class ProjectedPlanStatsWriter extends ProjectedTopologyWriterBase {
    private static Logger logger = LogManager.getLogger();

    private final HistorydbIO historydbIO;
    private final SimpleBulkLoaderFactory loaders;
    private MktSnapshotsStatsRecord currentPriceIndexRecord;
    private MktSnapshotsStatsRecord projectedPriceIndexRecord;
    private PlanStatsAggregator aggregator;
    private boolean initFailed = false;
    private int numOriginalPriceIndex = 0;

    /**
     * Create a new writer instance.
     *
     * @param topologyInfo topology info
     * @param historydbIO  database utils
     * @param loaders      bulk loader factory
     */
    private ProjectedPlanStatsWriter(@Nonnull TopologyInfo topologyInfo,
                                     @Nonnull HistorydbIO historydbIO,
                                     @Nonnull SimpleBulkLoaderFactory loaders) {
        super();
        this.historydbIO = historydbIO;
        this.loaders = loaders;

        try {
            final ScenariosRecord scenarioInfo = historydbIO.getOrAddScenariosRecord(topologyInfo);
            // prepare records to write to the mkt_snapshots_stats table for current and projected
            this.currentPriceIndexRecord = buildPriceIndexRecord(scenarioInfo, "currentPriceIndex");
            this.projectedPriceIndexRecord = buildPriceIndexRecord(scenarioInfo, "priceIndex");
            this.aggregator = new PlanStatsAggregator(loaders, historydbIO, topologyInfo, false);
        } catch (VmtDbException e) {
            throw new IllegalStateException("Failed to prepare for projected plan ingestion", e);
        }
    }

    @Override
    public ChunkDisposition processEntities(@Nonnull final Collection<ProjectedTopologyEntity> chunk,
                                            @Nonnull final String infoSummary) {
        aggregator.handleChunk(Collections2.transform(chunk, ProjectedTopologyEntity::getEntity));

        for (final ProjectedTopologyEntity entity : chunk) {
            if (entity.hasOriginalPriceIndex()) {
                numOriginalPriceIndex++;
                tabulateCapacityMinMax(currentPriceIndexRecord, entity.getOriginalPriceIndex());
            }
            tabulateCapacityMinMax(projectedPriceIndexRecord, entity.getProjectedPriceIndex());
        }
        return ChunkDisposition.SUCCESS;
    }

    @Override
    public void finish(final int entityCount, final boolean expedite, final String infoSummary)
        throws InterruptedException {

        aggregator.writeAggregates();
        if (entityCount != 0 && numOriginalPriceIndex != 0) {
            // add the priceIndex current and projected values
            currentPriceIndexRecord.setAvgValue(historydbIO.clipValue(currentPriceIndexRecord.getCapacity()
                / numOriginalPriceIndex));
            projectedPriceIndexRecord.setAvgValue(historydbIO.clipValue(projectedPriceIndexRecord.getCapacity()
                / entityCount));
            final BulkLoader<MktSnapshotsStatsRecord> loader
                = loaders.getLoader(MktSnapshotsStats.MKT_SNAPSHOTS_STATS);
            loader.insert(currentPriceIndexRecord);
            loader.insert(projectedPriceIndexRecord);
        } else {
            logger.warn("numberOfEntities: " + entityCount
                + " and/or numOriginalPriceIndex: " + numOriginalPriceIndex + " are 0.");
        }
    }

    /**
     * Create an initialized snapshots stats record ({@link MktSnapshotsStatsRecord}
     * to populate with priceIndex information.
     * Set up for calculating min, max, and sum of priceIndex values.
     *
     * @param scenarioInfo the information about the current scenario to initialize the
     *                     priceIndex information from, e.g.
     * @param propertyType the property to record, e.g. "priceIndex"
     * @return an initalized DB record {@link MktSnapshotsStatsRecord}
     */
    private MktSnapshotsStatsRecord buildPriceIndexRecord(ScenariosRecord scenarioInfo, String propertyType) {
        MktSnapshotsStatsRecord commodityRecord = new MktSnapshotsStatsRecord();
        commodityRecord.setRecordedOn(scenarioInfo.getCreateTime());
        commodityRecord.setMktSnapshotId(scenarioInfo.getId());
        commodityRecord.setPropertyType(propertyType);
        commodityRecord.setMinValue(Double.MAX_VALUE);
        commodityRecord.setMaxValue(Double.MIN_VALUE);
        commodityRecord.setCapacity(0D);
        commodityRecord.setProjectionTime(scenarioInfo.getCreateTime());
        // This priceIndex represents the entire plan market, so no single entity type applies
        commodityRecord.setEntityType((short)EntityType.UNKNOWN_VALUE);
        return commodityRecord;
    }

    /**
     * Calculate the sum, min, and max of priceIndex values, and store back into the
     * priceIndexRecord DB object.
     *
     * @param priceIndexRecord the stats record to update
     * @param value            the value to update with
     */
    private void tabulateCapacityMinMax(MktSnapshotsStatsRecord priceIndexRecord, double value) {
        priceIndexRecord.setCapacity(historydbIO.clipValue(priceIndexRecord.getCapacity() + value));
        priceIndexRecord.setMinValue(historydbIO.clipValue(Math.min(priceIndexRecord.getMinValue(), value)));
        priceIndexRecord.setMaxValue(historydbIO.clipValue(Math.max(priceIndexRecord.getMaxValue(), value)));
    }

    /**
     * Factory to create new writer instance.
     */
    public static class Factory extends ProjectedTopologyWriterBase.Factory {
        private final HistorydbIO historydbIO;

        /**
         * Create a new factory instance.
         *
         * @param historydbIO database utils
         */
        public Factory(@Nonnull HistorydbIO historydbIO) {
            this.historydbIO = historydbIO;
        }

        @Override
        public Optional<IChunkProcessor<ProjectedTopologyEntity>>
        getChunkProcessor(final TopologyInfo topologyInfo, final SimpleBulkLoaderFactory loaders) {
            return Optional.of(new ProjectedPlanStatsWriter(topologyInfo, historydbIO, loaders));
        }
    }
}
