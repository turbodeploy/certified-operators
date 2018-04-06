package com.vmturbo.history.stats;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.InsertSetMoreStep;
import org.jooq.Query;
import org.jooq.Table;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.db.BasedbIO;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.StringConstants;
import com.vmturbo.history.topology.TopologySnapshotRegistry;
import com.vmturbo.history.utils.TopologyOrganizer;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs;

/**
 * Write stats derived from a PriceIndex message to the RDB.
 */
public class PriceIndexWriter {

    private static final Logger logger = LogManager.getLogger();

    private final HistorydbIO historydbIO;

    // the number of entities for which stats are persisted in a single DB Insert operations
    private final int writeTopologyChunkSize;

    /**
     * a utility class to coordinate async receipt of Topology and PriceIndex messages for the
     * same topologyContext.
     */
    private final TopologySnapshotRegistry snapshotRegistry;


    public PriceIndexWriter(TopologySnapshotRegistry topologySnapshotRegistry,
                            HistorydbIO historydbIO, int writeTopologyChunkSize) {
        this.historydbIO = historydbIO;
        this.snapshotRegistry = topologySnapshotRegistry;
        this.writeTopologyChunkSize = writeTopologyChunkSize;
    }

    /**
     * Persist priceIndex values to the DB, once the corresponding topology has been received.
     * The {@link TopologySnapshotRegistry} tracks the two components, managing the async
     * delivery.
     *
     * @param topologyContextId the long-running topology context to which this priceIndex info
     *                          belongs
     * @param topologyId        id for this specific topology
     * @param payloadList       a list of {@link PriceIndexDTOs.PriceIndexMessagePayload} elements
     */
    public void persistPriceIndexInfo(long topologyContextId,
                                      long topologyId,
                                      List<PriceIndexDTOs.PriceIndexMessagePayload> payloadList) {
        // use the snapshotRegistry to sequence the processing, ensuring the topology is
        // processed before the priceIndex
        snapshotRegistry.registerPriceIndexInfo(topologyContextId, topologyId, topologyOrganizer -> {
            // this block will run when the full topology is available; might already be, but
            // ... might not yet be
            logger.debug("persisting live priceIndex, contextId: {}, topo: {}, items: {}",
                    topologyContextId, topologyId, payloadList.size());
            SharedMetrics.UPDATE_PRICE_INDEX_DURATION_SUMMARY
                    .labels(SharedMetrics.LIVE_CONTEXT_TYPE_LABEL)
                    .time(() -> persistPriceIndexInfoInternal(payloadList, topologyOrganizer));
        });
    }

    /**
     * Persist "priceIndex" info, generated by the Market, to the _stats_latest database
     * for each entity in the payloadList.
     * <p>
     * <p><p>The records are inserted, nothing is updated, and so we rely on "auto-commit" for each
     * operation.
     * <p>
     * <p><p>The connection will auto-close.
     * <p>
     * <p>The database is determined based on the Entity Type, so we use the snapshotRegistry to
     * provide that information.
     *
     * @param priceIndexPayloadList the priceIndex information to persist - entityId -> priceIndex
     * @param topologyOrganizer     the snapshot map  used to look up {@link TopologyDTO.TopologyEntityDTO} by oid
     */
    private void persistPriceIndexInfoInternal(List<PriceIndexDTOs.PriceIndexMessagePayload> priceIndexPayloadList,
                                       TopologyOrganizer topologyOrganizer) {
        logger.debug("Persisting priceIndex info for context: {}, topology: {}, count: {}",
                topologyOrganizer.getTopologyContextId(),
                topologyOrganizer.getTopologyId(),
                priceIndexPayloadList.size());
        Instant start = Instant.now();
        try {

            // accumulate a batch of SQL statements to insert the commodity rows; execute in batches
            List<Query> commodityInsertStatements = Lists.newArrayList();

            final Set<Long> missingEntityOids = Sets.newHashSet();

            for (PriceIndexDTOs.PriceIndexMessagePayload priceIndexInfo : priceIndexPayloadList) {
                final long entityId = priceIndexInfo.getOid();
                final Optional<Integer> entityTypeId = topologyOrganizer.getEntityType(entityId);
                if (!entityTypeId.isPresent()) {
                    missingEntityOids.add(entityId);
                    continue;
                }

                // note that getPriceIndexCurrent() will never return null
                final double priceIndexCurrent = historydbIO.clipValue(
                        priceIndexInfo.getPriceindexCurrent());
                // todo: Handle the project priceIndex:  priceIndexInfo.getPriceindexProjected();
                final long snapshotTime = topologyOrganizer.getSnapshotTime();

                Table<?> dbTable = historydbIO.getLatestDbTableForEntityType(entityTypeId.get());
                if (dbTable == null) {
                    // no table found for the entity type, meaning the entity cannot be persisted
                    continue;
                }
                InsertSetMoreStep<?> insertStmt = historydbIO.getCommodityInsertStatement(dbTable);
                historydbIO.initializeCommodityInsert(StringConstants.PRICE_INDEX, snapshotTime,
                        entityId, RelationType.METRICS, null, null,
                        null, insertStmt, dbTable);
                // set the values specific to used component of commodity and write
                historydbIO.setCommodityValues(StringConstants.PRICE_INDEX, priceIndexCurrent,
                        insertStmt, dbTable);
                commodityInsertStatements.add(insertStmt);
                if (commodityInsertStatements.size() > writeTopologyChunkSize) {
                    // execute a batch of updates - FORCED implies repeat until successful
                    historydbIO.execute(BasedbIO.Style.FORCED, commodityInsertStatements);
                    commodityInsertStatements = new ArrayList<>(writeTopologyChunkSize);
                }
            }
            if (missingEntityOids.size() > 0) {
                logger.warn("missing entity DTOs from topology context {};  " +
                                "topology ID {};  skipping {}",
                        topologyOrganizer.getTopologyContextId(),
                        topologyOrganizer.getTopologyId(),
                        missingEntityOids);
            }
            // now execute the remaining batch of updates, if any
            if (commodityInsertStatements.size() > 0) {
                historydbIO.execute(BasedbIO.Style.FORCED, commodityInsertStatements);
            }
            Duration elapsed = Duration.between(start, Instant.now());
            logger.debug("Done persisting priceIndex info for context: {}, topology: {}, time {}",
                    topologyOrganizer.getTopologyContextId(), topologyOrganizer.getTopologyId(),
                    elapsed
            );
        } catch (VmtDbException e) {
            logger.warn("Error creating connection to persist PriceIndex information to DB", e);
        }
    }
}