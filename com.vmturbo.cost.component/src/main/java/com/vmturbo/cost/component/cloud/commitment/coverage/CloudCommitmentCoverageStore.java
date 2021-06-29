package com.vmturbo.cost.component.cloud.commitment.coverage;

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
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCommitmentCoverageStatsRequest.GroupByCondition;
import com.vmturbo.common.protobuf.cloud.CloudCommon.AccountFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.CloudStatGranularity;
import com.vmturbo.common.protobuf.cloud.CloudCommon.RegionFilter;
import com.vmturbo.common.protobuf.cloud.CloudCommon.ServiceProviderFilter;

/**
 * A store for cloud commitment coverage. Coverage will only provide the covered entity, without a
 * relationship to the cloud commitment.
 */
public interface CloudCommitmentCoverageStore {

    /**
     * Persists the coverage buckets provided. Any samples that conflict with samples already persisted
     * will be updated.
     * @param coverageBuckets The coverage buckets to persist.
     */
    void persistCoverageSamples(@Nonnull Collection<CloudCommitmentDataBucket> coverageBuckets);

    /**
     * Queries the coverage samples with account aggregation, based on the {@code accountCoverageFilter}.
     * @param accountCoverageFilter The {@link AccountCoverageFilter}.
     * @return The list of coverage data buckets, sorted by earliest sample time first.
     */
    @Nonnull
    List<CloudCommitmentDataBucket> getCoverageBuckets(@Nonnull AccountCoverageFilter accountCoverageFilter);

    /**
     * Streams the account aggregated coverage stat records, based on the provided {@code coverageFilter}.
     * @param coverageFilter The {@link AccountCoverageStatsFilter}.
     * @return A stream of stat records for the matching data samples, sorted by earliest sample time first.
     */
    @Nonnull
    Stream<CloudCommitmentStatRecord> streamCoverageStats(@Nonnull AccountCoverageStatsFilter coverageFilter);

    /**
     * A filter for account-aggregated coverage data.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface AccountCoverageFilter {

        /**
         * The (optional) start time to filter by (inclusive).
         *
         * @return The start time.
         */
        @Nonnull
        Optional<Instant> startTime();

        /**
         * The (optional) end time to filter by (exclusive).
         *
         * @return The start time.
         */
        @Nonnull
        Optional<Instant> endTime();

        /**
         * The granularity of data to fetch. If a granularity is not specified, one will be derived
         * from the {@link #startTime()} and retention periods set for coverage data.
         *
         * @return The granularity of data to fetch.
         */
        @Nonnull
        Optional<CloudStatGranularity> granularity();

        /**
         * A filter of coverage by regions.
         *
         * @return A filter of coverage by regions.
         */
        @Default
        @Nonnull
        default RegionFilter regionFilter() {
            return RegionFilter.getDefaultInstance();
        }

        /**
         * A filter of coverage by purchasing accounts.
         *
         * @return A filter of coverage by purchasing accounts.
         */
        @Default
        @Nonnull
        default AccountFilter accountFilter() {
            return AccountFilter.getDefaultInstance();
        }

        /**
         * A filter of coverage by service provider.
         *
         * @return A filter of coverage by service provider.
         */
        @Default
        @Nonnull
        default ServiceProviderFilter serviceProviderFilter() {
            return ServiceProviderFilter.getDefaultInstance();
        }

        /**
         * Constructs and returns a new builder instance.
         *
         * @return The newly constructed builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for creating {@link AccountCoverageFilter} instances.
         */
        class Builder extends ImmutableAccountCoverageFilter.Builder {}
    }

    /**
     * A filter for account-aggregated coverage stats.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface AccountCoverageStatsFilter extends AccountCoverageFilter {

        /**
         * The group by conditions for the stats query. Note that all request stats will always be
         * grouped by sample time, coverage type, and coverage subtype.
         * @return The list of group by conditions.
         */
        @Nonnull
        List<GroupByCondition> groupByList();

        /**
         * Constructs and returns a new builder instance.
         *
         * @return The newly constructed builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for creating {@link AccountCoverageStatsFilter} instances.
         */
        class Builder extends ImmutableAccountCoverageStatsFilter.Builder {}
    }
}
