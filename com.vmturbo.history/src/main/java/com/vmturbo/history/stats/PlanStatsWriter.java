package com.vmturbo.history.stats;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Collections2;

import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.abstraction.tables.MktSnapshotsStats;
import com.vmturbo.history.schema.abstraction.tables.records.MktSnapshotsStatsRecord;
import com.vmturbo.history.schema.abstraction.tables.records.ScenariosRecord;
import com.vmturbo.history.utils.HistoryStatsUtils;
import com.vmturbo.history.utils.TopologyOrganizer;

/**
 * Persist stats from a plan topology to the mkt_snapshots tables in the relational database.
 * <ul>
 *     <li>mkt_snapshots - one row for each planning topology snapshot
 *     <li>mkt_snapshot_stats - one row for each stat for each planning topology snapshot
 * </ul>
 **/
public class PlanStatsWriter {

    private static final Logger logger = LogManager.getLogger();

    private final HistorydbIO historydbIO;

    public PlanStatsWriter(HistorydbIO historydbIO) {
        this.historydbIO = historydbIO;
    }

    /**
     * Calculate the sum, min, and max of priceIndex values, and store back into the
     * priceIndexRecord DB object.
     *
     * @param priceIndexRecord the stats record to update
     * @param value the value to update with
     */
    private void tabulateCapacityMinMax(MktSnapshotsStatsRecord priceIndexRecord, double value) {
        priceIndexRecord.setCapacity(priceIndexRecord.getCapacity() + value);
        priceIndexRecord.setMinValue(Math.min(priceIndexRecord.getMinValue(), value));
        priceIndexRecord.setMaxValue(Math.max(priceIndexRecord.getMaxValue(), value));
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
        return commodityRecord;
    }

    /**
     * Process message with chunks of projected plan topology DTOs.
     *
     * @param topologyOrganizer the topology organizer for this topology
     * @param dtosIterator an iterator of chunks
     * @throws CommunicationException if there is a problem getting the next chunk
     * @throws TimeoutException if there is a timeout getting the next chunk
     * @throws InterruptedException if getting the next chunk was interrupted
     * @throws VmtDbException if there is a problem writing to the DB
     * @return The number of entities processed.
     */
    public int processProjectedChunks(@Nonnull final TopologyOrganizer topologyOrganizer,
                @Nonnull final Set<Long> skippedEntities,
                @Nonnull final RemoteIterator<ProjectedTopologyEntity> dtosIterator)
            throws CommunicationException, TimeoutException, InterruptedException, VmtDbException {
        final ScenariosRecord scenarioInfo = historydbIO.getOrAddScenariosRecord(
            topologyOrganizer.getTopologyContextId(), topologyOrganizer.getTopologyId(),
            topologyOrganizer.getSnapshotTime());

        // prepare records to write to the mkt_snapshots_stats table for current and projected
        final MktSnapshotsStatsRecord currentPriceIndexRecord = buildPriceIndexRecord(scenarioInfo,
                "currentPriceIndex");
        final MktSnapshotsStatsRecord projectedPriceIndexRecord = buildPriceIndexRecord(scenarioInfo,
                "priceIndex");

        int numOriginalPriceIndex = skippedEntities.size();
        skippedEntities.forEach(skippedEntityOid ->
            tabulateCapacityMinMax(currentPriceIndexRecord, HistoryStatsUtils.DEFAULT_PRICE_IDX));

        int numberOfEntities = 0;
        historydbIO.addMktSnapshotRecord(topologyOrganizer);
        final PlanStatsAggregator aggregator
                = new PlanStatsAggregator(historydbIO, topologyOrganizer, false);
        while (dtosIterator.hasNext()) {
            final Collection<ProjectedTopologyEntity> chunk = dtosIterator.nextChunk();
            aggregator.handleChunk(Collections2.transform(chunk, ProjectedTopologyEntity::getEntity));

            for (final ProjectedTopologyEntity entity : chunk) {
                if (entity.hasOriginalPriceIndex()) {
                    numOriginalPriceIndex++;
                    tabulateCapacityMinMax(currentPriceIndexRecord, entity.getOriginalPriceIndex());
                }
                tabulateCapacityMinMax(projectedPriceIndexRecord, entity.getProjectedPriceIndex());
            }

            numberOfEntities += chunk.size();
        }
        logger.debug("Writing aggregates for topology {} and context {}",
                topologyOrganizer.getTopologyId(), topologyOrganizer.getTopologyContextId());
        aggregator.writeAggregates();

        // add the priceIndex current and projected values
        currentPriceIndexRecord.setAvgValue(currentPriceIndexRecord.getCapacity() / numOriginalPriceIndex);
        projectedPriceIndexRecord.setAvgValue(projectedPriceIndexRecord.getCapacity() / numberOfEntities);
        historydbIO.execute(HistorydbIO.getJooqBuilder()
                .insertInto(MktSnapshotsStats.MKT_SNAPSHOTS_STATS)
                .set(currentPriceIndexRecord)
                .newRecord()
                .set(projectedPriceIndexRecord));

        logger.debug("Done handling topology notification for projected topology {} in context {}."
                        + " Number of entities: {}", topologyOrganizer.getTopologyId(),
                topologyOrganizer.getTopologyContextId(), numberOfEntities);

        return numberOfEntities;
    }

    /**
     * Process message with chunks of plan topology DTOs.
     *
     * @param topologyOrganizer the topology organizer for this topology
     * @param dtosIterator an iterator of chunks
     * @throws CommunicationException if there is a problem getting the next chunk
     * @throws TimeoutException if there is a timeout getting the next chunk
     * @throws InterruptedException if getting the next chunk was interrupted
     * @throws VmtDbException if there is a problem writing to the DB
     * @return The number of entities processed.
     */
    public int processChunks(@Nonnull final TopologyOrganizer topologyOrganizer,
                             @Nonnull final RemoteIterator<TopologyEntityDTO> dtosIterator)
            throws CommunicationException, TimeoutException, InterruptedException, VmtDbException {
        int numberOfEntities = 0;
        historydbIO.addMktSnapshotRecord(topologyOrganizer);
        final PlanStatsAggregator aggregator
                = new PlanStatsAggregator(historydbIO, topologyOrganizer, true);
        while (dtosIterator.hasNext()) {
            final Collection<TopologyEntityDTO> chunk = dtosIterator.nextChunk();
            aggregator.handleChunk(chunk);
            numberOfEntities += chunk.size();
        }
        aggregator.writeAggregates();
        logger.debug("Done handling topology notification for source topology {} in context {}."
                        + " Number of entities: {}", topologyOrganizer.getTopologyId(),
                topologyOrganizer.getTopologyContextId(), numberOfEntities);

        return numberOfEntities;
    }
}
