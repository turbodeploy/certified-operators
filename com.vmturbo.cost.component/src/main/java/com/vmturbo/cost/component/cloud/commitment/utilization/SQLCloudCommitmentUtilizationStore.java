package com.vmturbo.cost.component.cloud.commitment.utilization;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentData.CloudCommitmentDataBucket;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord;
import com.vmturbo.common.protobuf.cloud.CloudCommon.CloudStatGranularity;

/**
 * A SQL implementation of {@link CloudCommitmentUtilizationStore}.
 */
public class SQLCloudCommitmentUtilizationStore implements CloudCommitmentUtilizationStore {

    private static final Logger logger = LogManager.getLogger();

    private final DSLContext dslContext;

    /**
     * Constructs a new {@link SQLCloudCommitmentUtilizationStore} instance.
     * @param dslContext The {@link DSLContext} to use in queries.
     */
    public SQLCloudCommitmentUtilizationStore(@Nonnull DSLContext dslContext) {
        this.dslContext = Objects.requireNonNull(dslContext);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public void persistUtilizationSamples(@Nonnull final Collection<CloudCommitmentDataBucket> utilizationBuckets) {

        Preconditions.checkNotNull(utilizationBuckets, "Utilization buckets collection cannot be null");

        final Stopwatch stopwatch = Stopwatch.createStarted();
        final ListMultimap<CloudStatGranularity, CloudCommitmentDataBucket> utilizationBucketsByGranularity =
                utilizationBuckets.stream()
                        .collect(ImmutableListMultimap.toImmutableListMultimap(
                                CloudCommitmentDataBucket::getGranularity,
                                Function.identity()));

        utilizationBucketsByGranularity.asMap().forEach((granularity, buckets) -> {

            logger.info("Persisting {} utilization buckets to the {} table", buckets::size, granularity::toString);

            final GranularTableStore<?, ?> granularTableStore =
                    GranularTableStore.createTableStore(granularity, dslContext);

            buckets.forEach(granularTableStore::persistDataBucket);

        });

        logger.info("Persisted {} utilization buckets in {}", utilizationBuckets.size(), stopwatch);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public List<CloudCommitmentDataBucket> getUtilizationBuckets(@Nonnull final CloudCommitmentUtilizationFilter filter) {

        Preconditions.checkNotNull(filter, "Utilization filter cannot be null");

        final GranularTableStore<?, ?> granularTableStore =
                GranularTableStore.createTableStore(filter.granularity(), dslContext);

        return granularTableStore.getUtilizationBuckets(filter);
    }

    /**
     * {@inheritDoc}.
     */
    @Override
    public Stream<CloudCommitmentStatRecord> streamUtilizationStats(@Nonnull final CloudCommitmentUtilizationStatsFilter filter) {

        Preconditions.checkNotNull(filter, "Utilization stats filter cannot be null");

        final GranularTableStore<?, ?> granularTableStore =
                GranularTableStore.createTableStore(filter.granularity(), dslContext);

        return granularTableStore.streamUtilizationStats(filter);
    }
}
