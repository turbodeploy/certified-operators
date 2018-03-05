package com.vmturbo.history.stats;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.utils.TopologyOrganizer;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessagePayload;
import com.vmturbo.reports.db.VmtDbException;
import com.vmturbo.reports.db.abstraction.tables.MktSnapshotsStats;
import com.vmturbo.reports.db.abstraction.tables.records.MktSnapshotsStatsRecord;
import com.vmturbo.reports.db.abstraction.tables.records.ScenariosRecord;

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
     * Persist priceIndex information for a plan analysis result. This includes both the current
     * and projecte priceIndex for each of a collection of ServiceEntities.
     * @param priceIndexMessage a list of priceIndex information records
     *                          ({@link PriceIndexMessagePayload})
     */
    public void persistPlanPriceIndexInfo(PriceIndexDTOs.PriceIndexMessage priceIndexMessage) {

        final long topologyContextId = priceIndexMessage.getTopologyContextId();
        final long topologyId = priceIndexMessage.getTopologyId();
        final long snapshotTime = priceIndexMessage.getSourceTopologyCreationTime();
        final List<PriceIndexMessagePayload> payloadList = priceIndexMessage.getPayloadList();

        if (payloadList.isEmpty()) {
            // no priceIndex items, no work to do
            logger.debug("Topology context {}, topology {} has empty priceIndex payload.",
                    topologyContextId, topologyId);
            return;
        }

        SharedMetrics.UPDATE_PRICE_INDEX_DURATION_SUMMARY
            .labels(SharedMetrics.PLAN_CONTEXT_TYPE_LABEL)
            .time(
                    () -> persistPlanPriceIndexInternal(priceIndexMessage, topologyContextId,
                            topologyId, snapshotTime, payloadList));
    }

    private void persistPlanPriceIndexInternal(PriceIndexMessage priceIndexMessage, long topologyContextId,
                                               long topologyId, long snapshotTime,
                                               List<PriceIndexMessagePayload> payloadList) {
        try {
            ScenariosRecord scenarioInfo = historydbIO.getOrAddScenariosRecord(topologyContextId,
                    topologyId, snapshotTime);

            // prepare records to write to the mkt_snapshots_stats table for current and projected
            MktSnapshotsStatsRecord currentPriceIndexRecord = buildPriceIndexRecord(scenarioInfo,
                    "currentPriceIndex");
            MktSnapshotsStatsRecord projectedPriceIndexRecord = buildPriceIndexRecord(scenarioInfo,
                    "priceIndex");

            // tabulate min, max, and total (capacity) priceIndex for the SE's given.
            for (PriceIndexMessagePayload priceIndexInfo : payloadList) {
                double current = priceIndexInfo.getPriceindexCurrent();
                tabulateCapacityMinMax(currentPriceIndexRecord, current);
                double projected = priceIndexInfo.getPriceindexProjected();
                tabulateCapacityMinMax(projectedPriceIndexRecord, projected);
            }

            // add the priceIndex current and projected values
            currentPriceIndexRecord.setAvgValue(currentPriceIndexRecord.getCapacity() /
                    payloadList.size());
            projectedPriceIndexRecord.setAvgValue(projectedPriceIndexRecord.getCapacity() /
                    payloadList.size());

            historydbIO.execute(HistorydbIO.getJooqBuilder()
                    .insertInto(MktSnapshotsStats.MKT_SNAPSHOTS_STATS)
                    .set(currentPriceIndexRecord)
                    .newRecord()
                    .set(projectedPriceIndexRecord));

        } catch (VmtDbException e) {
            throw new RuntimeException(new VmtDbException(VmtDbException.INSERT_ERR,
                    "Error persisting priceIndex for context " + topologyContextId +
                    ", topology ID " + priceIndexMessage.getTopologyId() +
                    ", market id " + priceIndexMessage.getMarketId(), e));
        }
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
    public int processProjectedChunks(TopologyOrganizer topologyOrganizer,
        RemoteIterator<TopologyEntityDTO> dtosIterator)
            throws CommunicationException, TimeoutException, InterruptedException, VmtDbException {
        return internalProcessChunks(topologyOrganizer, dtosIterator, false);
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
    public int processChunks(TopologyOrganizer topologyOrganizer,
        RemoteIterator<TopologyEntityDTO> dtosIterator)
            throws CommunicationException, TimeoutException, InterruptedException, VmtDbException {
        return internalProcessChunks(topologyOrganizer, dtosIterator, true);
    }

    private int internalProcessChunks(TopologyOrganizer topologyOrganizer,
        RemoteIterator<TopologyEntityDTO> dtosIterator, boolean isProcessingCurrent)
            throws CommunicationException, TimeoutException, InterruptedException, VmtDbException {
        int numberOfEntities = 0;
        int chunkNumber = 0;
        historydbIO.addMktSnapshotRecord(topologyOrganizer);
        PlanStatsAggregator aggregator
            = new PlanStatsAggregator(historydbIO, topologyOrganizer, isProcessingCurrent);
        while (dtosIterator.hasNext()) {
            Collection<TopologyEntityDTO> chunk = dtosIterator.nextChunk();
            logger.debug("Received chunk #{} of size {} for topology {} and context {}",
                ++chunkNumber, chunk.size(), topologyOrganizer.getTopologyId(),
                topologyOrganizer.getTopologyContextId());
            aggregator.handleChunk(chunk);
            numberOfEntities += chunk.size();
        }
        logger.debug("Writing aggregates for topology {} and context {}",
            topologyOrganizer.getTopologyId(), topologyOrganizer.getTopologyContextId());
        aggregator.writeAggregates();
        logger.debug("Done handling topology notification for topology {} and context {}."
                        + " Number of entities: {}", topologyOrganizer.getTopologyId(),
                        topologyOrganizer.getTopologyContextId(), numberOfEntities);

        return numberOfEntities;
    }
}
