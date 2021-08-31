package com.vmturbo.cost.component.cloud.commitment.utilization;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentData.CloudCommitmentDataBucket;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCloudCommitmentUtilizationRequest.GroupByCondition;
import com.vmturbo.common.protobuf.cloud.CloudCommon.AccountFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.CloudCommitmentFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.CloudStatGranularity;
import com.vmturbo.common.protobuf.cloud.CloudCommon.RegionFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.ServiceProviderFilter;

/**
 * A store of historical utilization statistics for cloud commitments.
 */
public interface CloudCommitmentUtilizationStore {

    /**
     * Persists the utilization data contained with {@code utilizationBuckets}. Any conflicts with
     * previously persisted data will update to the values contained with {@code utilizationBuckets}.
     * Any references to coverage attributes within each {@link CloudCommitmentDataBucket} (e.g. an entity
     * ID) will be ignored.
     * @param utilizationBuckets The utilization buckets to persist.
     */
    void persistUtilizationSamples(@Nonnull Collection<CloudCommitmentDataBucket> utilizationBuckets);

    /**
     * Gets the utilization {@link CloudCommitmentDataBucket} matching the {@code filter}.
     * @param filter The filter of utilization data.
     * @return A list of data buckets containing utilization data matching the {@code filter}.
     */
    @Nonnull
    List<CloudCommitmentDataBucket> getUtilizationBuckets(@Nonnull CloudCommitmentUtilizationFilter filter);

    /**
     * Streams the {@link CloudCommitmentStatRecord} matching {@code filter}. Stats will always be grouped
     * by the sample timestamp (each record is analogous to {@link CloudCommitmentDataBucket}).
     *
     * <p>NOTE: This stream should be treated as a closeable resource, as it may represent underlying
     * database connections.
     * @param filter The filter for stats data.
     * @return A stream of stats records matching the filter.
     */
    @Nonnull
    Stream<CloudCommitmentStatRecord> streamUtilizationStats(@Nonnull CloudCommitmentUtilizationStatsFilter filter);

    /**
     * A filter for utilization data. Any sub-filter contained within a {@link CloudCommitmentUtilizationFilter}
     * will be AND'd as part of the query.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface CloudCommitmentUtilizationFilter {

        /**
         * The (optional) start time to filter by (inclusive).
         * @return The start time.
         */
        @Nonnull
        Optional<Instant> startTime();

        /**
         * The (optional) end time to filter by (exclusive).
         * @return The start time.
         */
        @Nonnull
        Optional<Instant> endTime();

        /**
         * The granularity of data to fetch.
         * @return The granularity of data to fetch.
         */
        @Default
        @Nonnull
        default CloudStatGranularity granularity() {
            return CloudStatGranularity.HOURLY;
        }

        /**
         * A filter of utilization by regions.
         * @return A filter of utilization by regions.
         */
        @Default
        @Nonnull
        default RegionFilter regionFilter() {
            return RegionFilter.getDefaultInstance();
        }

        /**
         * A filter of utilization by purchasing accounts.
         * @return A filter of utilization by purchasing accounts.
         */
        @Default
        @Nonnull
        default AccountFilter accountFilter() {
            return AccountFilter.getDefaultInstance();
        }

        /**
         * A filter of utilization by cloud commitments.
         * @return A filter of utilization by cloud commitments.
         */
        @Default
        @Nonnull
        default CloudCommitmentFilter cloudCommitmentFilter() {
            return CloudCommitmentFilter.getDefaultInstance();
        }

        /**
         * A filter of utilization by service provider.
         * @return A filter of utilization by service provider.
         */
        @Default
        @Nonnull
        default ServiceProviderFilter serviceProviderFilter() {
            return ServiceProviderFilter.getDefaultInstance();
        }

        /**
         * Constructs and returns a new builder instance.
         * @return The newly constructed builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing {@link CloudCommitmentUtilizationFilter} instances.
         */
        class Builder extends ImmutableCloudCommitmentUtilizationFilter.Builder {}
    }

    /**
     * A utilization stat filter, adding support for grouping utilization data points. Note that all
     * request stats will always be grouped by sample time, coverage type, and coverage subtype.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface CloudCommitmentUtilizationStatsFilter extends CloudCommitmentUtilizationFilter {

        /**
         * The group by conditions for the stats query. Note that all request stats will always be
         * grouped by sample time, coverage type, and coverage subtype.
         * @return The list of group by conditions.
         */
        @Nonnull
        List<GroupByCondition> groupByList();

        /**
         * Constructs and returns a new builder instance.
         * @return The newly constructed builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing {@link CloudCommitmentUtilizationStatsFilter} instances.
         */
        class Builder extends ImmutableCloudCommitmentUtilizationStatsFilter.Builder {}
    }
}
