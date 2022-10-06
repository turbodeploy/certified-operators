package com.vmturbo.cost.component.billed.cost;

import static com.vmturbo.cost.component.billed.cost.BilledCostTableAccessor.CLOUD_COST_DAILY;
import static com.vmturbo.cost.component.billed.cost.BilledCostTableAccessor.CLOUD_COST_HOURLY;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.persistence.DataQueue;
import com.vmturbo.cloud.common.persistence.DataQueueConfiguration;
import com.vmturbo.cloud.common.persistence.DataQueueFactory;
import com.vmturbo.cloud.common.scope.IdentityOperationException;
import com.vmturbo.cloud.common.scope.IdentityUninitializedException;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostData;
import com.vmturbo.cost.component.billed.cost.CloudCostStore.BilledCostPersistenceSession;
import com.vmturbo.platform.sdk.common.CostBilling.CloudBillingData.CloudBillingBucket.Granularity;
import com.vmturbo.sql.utils.DbException;

/**
 * SQL implementation of {@link BilledCostPersistenceSession}.
 */
class SqlBilledCostPersistenceSession implements BilledCostPersistenceSession {

    private final Logger logger = LogManager.getLogger();

    private final BilledCostWriter costWriter;

    private final BilledCostPersistenceConfig persistenceConfig;

    private final Map<Granularity, DataQueue<BilledCostData, BilledCostPersistenceSummary>> granularDataQueues;

    protected SqlBilledCostPersistenceSession(@Nonnull BilledCostWriter costWriter,
                                              @Nonnull DataQueueFactory dataQueueFactory,
                                              @Nonnull BilledCostPersistenceConfig persistenceConfig) {

        this.costWriter = Objects.requireNonNull(costWriter);
        this.persistenceConfig = Objects.requireNonNull(persistenceConfig);

        /*
        We create a queue for each
         */
        final DataQueueConfiguration hourlyQueueConfig = DataQueueConfiguration.builder()
                .queueName(CLOUD_COST_HOURLY.table().getName())
                .concurrency(persistenceConfig.concurrency())
                .build();
        final DataQueueConfiguration dailyQueueConfig = DataQueueConfiguration.builder()
                .queueName(CLOUD_COST_DAILY.table().getName())
                .concurrency(persistenceConfig.concurrency())
                .build();
        this.granularDataQueues = ImmutableMap.of(
                Granularity.HOURLY, dataQueueFactory.createQueue(
                        BilledCostBatcher.create(),
                        this::processHourly,
                        BilledCostPersistenceSummary.Collector.create(),
                        hourlyQueueConfig),
                Granularity.DAILY, dataQueueFactory.createQueue(
                        BilledCostBatcher.create(),
                        this::processDaily,
                        BilledCostPersistenceSummary.Collector.create(),
                        dailyQueueConfig));
    }

    @Override
    public void storeCostDataAsync(@Nonnull BilledCostData billedCostData) {

        if (granularDataQueues.containsKey(billedCostData.getGranularity())) {
            granularDataQueues.get(billedCostData.getGranularity()).addData(billedCostData);
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unsupported granularity: %s", billedCostData.getGranularity()));
        }
    }

    @Override
    public void commitSession() throws Exception {

        logger.info("Committing cloud cost persistence session");

        final Instant timeBarrier = Instant.now().plus(persistenceConfig.persistenceTimeout());

        try {
            for (DataQueue<?, ?> dataQueue : granularDataQueues.values()) {
                final Duration remainingTime = Duration.between(Instant.now(), timeBarrier);
                dataQueue.drainAndClose(remainingTime);
            }
        } catch (Exception e) {
            granularDataQueues.values().forEach(DataQueue::forceClose);
            throw e;
        }
    }

    private BilledCostPersistenceStats processHourly(@Nonnull List<BilledCostData> billedCostDataList)
            throws IdentityOperationException, IdentityUninitializedException, DbException {

        logger.info("Processing {} hourly cost data instances with size {}",
                billedCostDataList::size, () -> billedCostDataList.stream().mapToLong(BilledCostData::getSerializedSize).sum());

        return costWriter.persistCostData(billedCostDataList, Granularity.HOURLY);
    }

    private BilledCostPersistenceStats processDaily(@Nonnull List<BilledCostData> billedCostDataList)
            throws IdentityOperationException, IdentityUninitializedException, DbException {

        logger.info("Processing {} daily cost data instances with size {}",
                billedCostDataList::size, () -> billedCostDataList.stream().mapToLong(BilledCostData::getSerializedSize).sum());

        return costWriter.persistCostData(billedCostDataList, Granularity.DAILY);
    }

    /**
     * Factory class for constructing {@link SqlBilledCostPersistenceSession} instances.
     */
    static class Factory {

        private final BilledCostWriter costWriter;

        private final DataQueueFactory dataQueueFactory;

        private final BilledCostPersistenceConfig persistenceConfig;

        protected Factory(@Nonnull BilledCostWriter costWriter,
                          @Nonnull DataQueueFactory dataQueueFactory,
                          @Nonnull BilledCostPersistenceConfig persistenceConfig) {

            this.costWriter = Objects.requireNonNull(costWriter);
            this.dataQueueFactory = Objects.requireNonNull(dataQueueFactory);
            this.persistenceConfig = Objects.requireNonNull(persistenceConfig);
        }

        @Nonnull
        protected SqlBilledCostPersistenceSession createPersistenceSession() {
            return new SqlBilledCostPersistenceSession(costWriter, dataQueueFactory, persistenceConfig);
        }
    }
}