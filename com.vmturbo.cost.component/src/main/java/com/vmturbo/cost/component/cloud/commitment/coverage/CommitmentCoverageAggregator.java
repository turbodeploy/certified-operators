package com.vmturbo.cost.component.cloud.commitment.coverage;

import java.util.Collection;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.cloud.common.immutable.HiddenImmutableTupleImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageGroupBy;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageVector;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.ScopedCommitmentCoverage;

/**
 * An aggregator of cloud commitment coverage, based on group-by requirements.
 */
public class CommitmentCoverageAggregator {

    /**
     * Groups the {@link ScopedCommitmentCoverage} collection, based on the provided group-by collection. If the
     * group-by collection is empty, the commitment coverage will be grouped based on the coverage vector info.
     * @param coverageCollection The commitment coverage collection.
     * @param groupByCollection THe group-by collection.
     * @return The coverage grouping list.
     */
    @Nonnull
    public List<CommitmentCoverageGrouping> groupCoverageCollection(
            @Nonnull Collection<ScopedCommitmentCoverage> coverageCollection,
            @Nonnull Collection<CloudCommitmentCoverageGroupBy> groupByCollection) {

        final Set<CloudCommitmentCoverageGroupBy> groupBySet = ImmutableSet.copyOf(groupByCollection);

        final ListMultimap<CoverageGroupKey, CloudCommitmentCoverageVector> coverageGroupMap = coverageCollection.stream()
                .flatMap(scopedCoverage -> scopedCoverage.getCoverageVectorList()
                        .stream()
                        .map(vectoredCoverage -> ImmutablePair.of(scopedCoverage, vectoredCoverage)))
                .collect(ImmutableListMultimap.toImmutableListMultimap(
                        coveragePair -> createGroupKeyFromCoverage(coveragePair.getLeft(), coveragePair.getRight(), groupBySet),
                        coveragePair -> coveragePair.getRight()));

        return coverageGroupMap.asMap().entrySet()
                .stream()
                .map(coverageGroupEntry -> CommitmentCoverageGrouping.of(coverageGroupEntry.getKey(), coverageGroupEntry.getValue()))
                .collect(ImmutableList.toImmutableList());
    }

    private CoverageGroupKey createGroupKeyFromCoverage(@Nonnull ScopedCommitmentCoverage scopedCoverage,
                                                        @Nonnull CloudCommitmentCoverageVector coverageVector,
                                                        @Nonnull Set<CloudCommitmentCoverageGroupBy> groupBySet) {

        final CoverageGroupKey.Builder coverageKey = CoverageGroupKey.builder()
                .coverageTypeInfo(coverageVector.getVectorType());

        groupBySet.forEach(groupBy -> {

            switch (groupBy) {

                case COMMITMENT_COVERAGE_GROUP_BY_REGION:
                    coverageKey.regionOid(scopedCoverage.getRegionOid());
                    coverageKey.serviceProviderOid(scopedCoverage.getServiceProviderOid());
                    break;
                case COMMITMENT_COVERAGE_GROUP_BY_ACCOUNT:
                    coverageKey.accountOid(scopedCoverage.getAccountOid());
                    coverageKey.serviceProviderOid(scopedCoverage.getServiceProviderOid());
                    break;
                case COMMITMENT_COVERAGE_GROUP_BY_CLOUD_SERVICE:
                    coverageKey.cloudServiceOid(scopedCoverage.getCloudServiceOid());
                    coverageKey.serviceProviderOid(scopedCoverage.getServiceProviderOid());
                    break;
                case COMMITMENT_COVERAGE_GROUP_BY_SERVICE_PROVIDER:
                    coverageKey.serviceProviderOid(scopedCoverage.getServiceProviderOid());
                    break;
                default:
                    throw new UnsupportedOperationException(
                            String.format("Group by condition is not supported: %s", groupBy));
            }
        });

        return coverageKey.build();
    }

    /**
     * A key for commitment coverage, containing the shared attributes of a {@link CommitmentCoverageGrouping}.
     */
    @HiddenImmutableImplementation
    @Immutable(prehash = true)
    public interface CoverageGroupKey {

        /**
         * The coverage type info.
         * @return The coverage type info.
         */
        @Nonnull
        CloudCommitmentCoverageTypeInfo coverageTypeInfo();

        /**
         * The optional account OID.
         * @return The optional account OID.
         */
        @Nonnull
        OptionalLong accountOid();

        /**
         * The optional region OID.
         * @return The optional region OID.
         */
        @Nonnull
        OptionalLong regionOid();

        /**
         * The optional cloud service OID.
         * @return The optional cloud service OID.
         */
        @Nonnull
        OptionalLong cloudServiceOid();

        /**
         * The optional service provider OID.
         * @return The optional service provider OID.
         */
        @Nonnull
        OptionalLong serviceProviderOid();

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed {@link Builder} instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing immutable {@link CoverageGroupKey} instances.
         */
        class Builder extends ImmutableCoverageGroupKey.Builder {}
    }

    /**
     * A grouping of {@link CloudCommitmentCoverageVector} instances, based on criteria
     * contained in {@link #key()}.
     */
    @HiddenImmutableTupleImplementation
    @Immutable
    public interface CommitmentCoverageGrouping {

        /**
         * The coverage group key.
         * @return The coverage group key.
         */
        @Nonnull
        CoverageGroupKey key();

        /**
         * The coverage list.
         * @return The coverage list.
         */
        @Nonnull
        List<CloudCommitmentCoverageVector> coverageList();

        /**
         * Constructs a new {@link CommitmentCoverageGrouping} instance.
         * @param key The coverage key.
         * @param coverageCollection The coverage collection.
         * @return The newly created {@link CommitmentCoverageGrouping} instance.
         */
        @Nonnull
        static CommitmentCoverageGrouping of(@Nonnull CoverageGroupKey key,
                                             @Nonnull Collection<CloudCommitmentCoverageVector> coverageCollection) {
            return CommitmentCoverageGroupingTuple.of(key, coverageCollection);
        }
    }
}
