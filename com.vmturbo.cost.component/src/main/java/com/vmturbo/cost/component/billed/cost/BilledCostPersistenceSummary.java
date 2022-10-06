package com.vmturbo.cost.component.billed.cost;

import java.util.LongSummaryStatistics;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;
import org.jetbrains.annotations.NotNull;

import com.vmturbo.cloud.common.data.stats.LongStatistics;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.cloud.common.persistence.DataQueueStats.DataSpecificStatsCollector;

/**
 * A summary of {@link BilledCostPersistenceStats}.
 */
@HiddenImmutableImplementation
@Immutable
public interface BilledCostPersistenceSummary {

    /**
     * Stats for {@link BilledCostPersistenceStats#tagGroupCount()}.
     * @return Stats for {@link BilledCostPersistenceStats#tagGroupCount()}.
     */
    @Nonnull
    LongStatistics tagGroupCount();

    /**
     * Stats for {@link BilledCostPersistenceStats#billingBucketCount()}.
     * @return Stats for {@link BilledCostPersistenceStats#billingBucketCount()}.
     */
    @Nonnull
    LongStatistics billingBucketCount();

    /**
     * Stats for {@link BilledCostPersistenceStats#cloudScopeCount()}.
     * @return Stats for {@link BilledCostPersistenceStats#cloudScopeCount()}.
     */
    @Nonnull
    LongStatistics cloudScopeCount();

    /**
     * Stats for {@link BilledCostPersistenceStats#billingItemCount()}.
     * @return Stats for {@link BilledCostPersistenceStats#billingItemCount()}.
     */
    @Nonnull
    LongStatistics billingItemCount();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed {@link Builder} instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing immutable {@link BilledCostPersistenceSummary} instances.
     */
    class Builder extends ImmutableBilledCostPersistenceSummary.Builder {}

    /**
     * A collector of {@link BilledCostPersistenceStats} into a {@link BilledCostPersistenceSummary}.
     */
    class Collector implements DataSpecificStatsCollector<BilledCostPersistenceStats, BilledCostPersistenceSummary> {

        private final ReadWriteLock collectionLock = new ReentrantReadWriteLock();

        private final LongSummaryStatistics tagGroupCount = new LongSummaryStatistics();

        private final LongSummaryStatistics billingBucketCount = new LongSummaryStatistics();

        private final LongSummaryStatistics cloudScopeCount = new LongSummaryStatistics();

        private final LongSummaryStatistics billingItemCount = new LongSummaryStatistics();

        private Collector() {}

        /**
         * Creates a new collector.
         * @return The newly created collector.
         */
        @Nonnull
        public static Collector create() {
            return new Collector();
        }

        @Override
        public void collect(@NotNull BilledCostPersistenceStats operationStats) {

            // Read/write lock is intentionally inverted to allow concurrent collection
            // while blocking collection while reading the stats
            collectionLock.writeLock().lock();
            try {
                tagGroupCount.accept(operationStats.tagGroupCount());
                billingBucketCount.accept(operationStats.billingBucketCount());
                cloudScopeCount.accept(operationStats.cloudScopeCount());
                billingItemCount.accept(operationStats.billingItemCount());
            } finally {
                collectionLock.writeLock().unlock();
            }
        }

        @Override
        public BilledCostPersistenceSummary toSummary() {

            collectionLock.readLock().lock();
            try {
                return BilledCostPersistenceSummary.builder()
                        .cloudScopeCount(LongStatistics.fromLongSummary(cloudScopeCount))
                        .billingBucketCount(LongStatistics.fromLongSummary(billingBucketCount))
                        .billingItemCount(LongStatistics.fromLongSummary(billingItemCount))
                        .tagGroupCount(LongStatistics.fromLongSummary(tagGroupCount))
                        .build();
            } finally {
                collectionLock.readLock().unlock();
            }
        }
    }
}
