package com.vmturbo.extractor.topology;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdate;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.StatusUpdateType;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.Detail;
import com.vmturbo.cost.api.CostNotificationListener;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.ModelDefinitions.EntityCost;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.topology.fetcher.BottomUpCostFetcherFactory.BottomUpCostData;
import com.vmturbo.extractor.topology.fetcher.BottomUpCostFetcherFactory.BottomUpCostData.BottomUpCostDataPoint;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * TopologyWriter that records per-entity cost data obtained from the cost compoennt.
 */
public class EntityCostListener implements CostNotificationListener {
    private static final Logger logger = LogManager.getLogger();

    private final DataProvider dataProvider;
    private final DbEndpoint dbEndpoint;
    private final ExecutorService pool;
    private final WriterConfig config;
    private final boolean reportingEnabled;
    private final long realtimeTopologyContextId;

    /**
     * Create a new instance.
     *
     * @param dataProvider {@link DataProvider} that will provide cost data
     * @param dbEndpoint   access to extractor database
     * @param pool         thread pool
     * @param config       writer config
     * @param reportingEnabled whether reporting is enabled
     * @param realtimeTopologyContextId id of real time topology context
     */
    public EntityCostListener(DataProvider dataProvider, final DbEndpoint dbEndpoint,
            final ExecutorService pool, WriterConfig config, boolean reportingEnabled,
            long realtimeTopologyContextId) {
        this.dataProvider = dataProvider;
        this.dbEndpoint = dbEndpoint;
        this.pool = pool;
        this.config = config;
        this.reportingEnabled = reportingEnabled;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    @Override
    public void onCostNotificationReceived(@Nonnull final CostNotification costNotification) {
        final MultiStageTimer timer = new MultiStageTimer(logger);
        if (costNotification.hasCloudCostStatsAvailable()) {
            final long snapshotTime = costNotification.getCloudCostStatsAvailable().getSnapshotDate();
            dataProvider.fetchBottomUpCostData(snapshotTime, timer);

            // only write to postgres if reporting is enabled
            if (reportingEnabled) {
                timer.start("Write entity costs");
                try {
                    long n = writeEntityCosts(snapshotTime);
                    timer.info(String.format("Wrote entity costs for %d entities", n), Detail.STAGE_DETAIL);
                } catch (UnsupportedDialectException | SQLException e) {
                    logger.error("Failed writing entity cost data", e);
                } catch (InterruptedException e) {
                    logger.error("Interrupted while writing entity cost data");
                    Thread.currentThread().interrupt();
                }
            }
        } else if (costNotification.hasStatusUpdate()) {
            // only process real time update, skip plan update
            if (costNotification.getStatusUpdate().getTopologyContextId() == realtimeTopologyContextId) {
                StatusUpdate statusUpdate = costNotification.getStatusUpdate();
                if (statusUpdate.getType() == StatusUpdateType.PROJECTED_COST_UPDATE) {
                    dataProvider.fetchProjectedBottomUpCostData(timer);
                } else if (statusUpdate.getType() == StatusUpdateType.SOURCE_RI_COVERAGE_UPDATE) {
                    dataProvider.fetchCurrentRICoverageData(timer);
                } else if (statusUpdate.getType() == StatusUpdateType.PROJECTED_RI_COVERAGE_UPDATE) {
                    dataProvider.fetchProjectedRICoverageData(timer,
                            costNotification.getStatusUpdate().getTopologyContextId());
                }
            }
        }
    }

    private long writeEntityCosts(long snapshotTime) throws UnsupportedDialectException, InterruptedException, SQLException {
        final BottomUpCostData entityCosts = dataProvider.getBottomUpCostData();
        if (entityCosts == null) {
            logger.warn("No entity cost data available, so no costs can be persisted");
            return 0;
        }
        if (entityCosts.getSnapshotTime() != snapshotTime) {
            logger.error("Latest entity cost data is not for snapshot specified in notification; "
                            + "expected {}, got {}",
                    new Timestamp(snapshotTime), new Timestamp(entityCosts.getSnapshotTime()));
            return 0;
        }
        final DslRecordSink sink = getEntityCostInserterSink();
        final Timestamp snapshotTimestamp = new Timestamp(snapshotTime);
        try (TableWriter writer = EntityCost.TABLE.open(sink, "entity_cost inserter", logger)) {
            final int[] entityCount = new int[]{0};
            for (long oid : entityCosts.getEntityOids()) {
                entityCount[0] += 1;
                for (BottomUpCostDataPoint dataPoint : entityCosts.getEntityCostDataPoints(oid)) {
                    writer.accept(entityCostRecord(snapshotTimestamp, oid, dataPoint));
                }
            }
            return entityCount[0];
        }
    }

    @VisibleForTesting
    DslRecordSink getEntityCostInserterSink()
            throws UnsupportedDialectException, SQLException, InterruptedException {
        return new DslRecordSink(dbEndpoint.dslContext(), EntityCost.TABLE, config, pool);
    }

    private Record entityCostRecord(final Timestamp snapshotTime, final long oid,
            final BottomUpCostDataPoint datapoint) {
        final Record record = new Record(EntityCost.TABLE);
        record.set(EntityCost.TIME, snapshotTime);
        record.set(EntityCost.ENTITY_OID, oid);
        record.set(EntityCost.CATEGORY, datapoint.getCategory());
        record.set(EntityCost.SOURCE, datapoint.getSource());
        record.set(EntityCost.COST, datapoint.getCost());
        return record;
    }
}
