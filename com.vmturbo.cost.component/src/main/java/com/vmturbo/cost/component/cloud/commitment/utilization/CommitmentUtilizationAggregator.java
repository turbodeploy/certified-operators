package com.vmturbo.cost.component.cloud.commitment.utilization;

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
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentUtilizationGroupBy;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentUtilizationVector;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.ScopedCommitmentUtilization;

/**
 * An aggregator of {@link ScopedCommitmentUtilization} instances to {@link CommitmentUtilizationGrouping}.
 */
public class CommitmentUtilizationAggregator {

    /**
     * Groups the {@code utilizationCollection} to {@link CommitmentUtilizationGrouping} instances, based
     * on {@code groupByCollection}.
     * @param utilizationCollection The commitment utilization colleciton.
     * @param groupByCollection The collection of group by requirements.
     * @return The utilization groups.
     */
    @Nonnull
    public List<CommitmentUtilizationGrouping> groupUtilizationCollection(
            @Nonnull Collection<ScopedCommitmentUtilization> utilizationCollection,
            @Nonnull Collection<CloudCommitmentUtilizationGroupBy> groupByCollection) {

        final Set<CloudCommitmentUtilizationGroupBy> groupBySet = ImmutableSet.copyOf(groupByCollection);

        final ListMultimap<UtilizationGroupKey, CloudCommitmentUtilizationVector> utilizationGroupMap = utilizationCollection.stream()
                .flatMap(scopedUtilization -> scopedUtilization.getUtilizationVectorList()
                        .stream()
                        .map(vectoredUtilization -> ImmutablePair.of(scopedUtilization, vectoredUtilization)))
                .collect(ImmutableListMultimap.toImmutableListMultimap(
                        utilizationPair -> createGroupKeyFromUtilization(utilizationPair.getLeft(), utilizationPair.getRight(), groupBySet),
                        utilizationPair -> utilizationPair.getRight()));

        return utilizationGroupMap.asMap().entrySet()
                .stream()
                .map(utilGroupEntry -> CommitmentUtilizationGrouping.of(utilGroupEntry.getKey(), utilGroupEntry.getValue()))
                .collect(ImmutableList.toImmutableList());
    }

    private UtilizationGroupKey createGroupKeyFromUtilization(@Nonnull ScopedCommitmentUtilization utilizationScope,
                                                              @Nonnull CloudCommitmentUtilizationVector utilizationVector,
                                                              @Nonnull Set<CloudCommitmentUtilizationGroupBy> groupBySet) {

        final UtilizationGroupKey.Builder utilizationKey = UtilizationGroupKey.builder()
                .coverageTypeInfo(utilizationVector.getVectorType());

        groupBySet.forEach(groupBy -> {

            switch (groupBy) {

                case COMMITMENT_UTILIZATION_GROUP_BY_COMMITMENT:
                    utilizationKey.commitmentOid(utilizationScope.getCloudCommitmentOid());
                    utilizationKey.accountOid(utilizationScope.getAccountOid());
                    utilizationKey.regionOid(utilizationScope.getRegionOid());
                    utilizationKey.serviceProviderOid(utilizationScope.getServiceProviderOid());
                    break;
                case COMMITMENT_UTILIZATION_GROUP_BY_REGION:
                    utilizationKey.regionOid(utilizationScope.getRegionOid());
                    utilizationKey.serviceProviderOid(utilizationScope.getServiceProviderOid());
                    break;
                case COMMITMENT_UTILIZATION_GROUP_BY_ACCOUNT:
                    utilizationKey.accountOid(utilizationScope.getAccountOid());
                    utilizationKey.serviceProviderOid(utilizationScope.getServiceProviderOid());
                    break;
                case COMMITMENT_UTILIZATION_GROUP_BY_SERVICE_PROVIDER:
                    utilizationKey.serviceProviderOid(utilizationScope.getServiceProviderOid());
                    break;
                default:
                    throw new UnsupportedOperationException(
                            String.format("Group by condition is not supported: %s", groupBy));
            }
        });

        return utilizationKey.build();
    }

    /**
     * A utilization group key, containing the shared attributes across
     * the elements in the {@link CommitmentUtilizationGrouping}.
     */
    @HiddenImmutableImplementation
    @Immutable(prehash = true)
    public interface UtilizationGroupKey {

        /**
         * The coverage type vector.
         * @return The coverage type vector.
         */
        @Nonnull
        CloudCommitmentCoverageTypeInfo coverageTypeInfo();

        /**
         * The optional commitment OID.
         * @return The optional commitment OID.
         */
        @Nonnull
        OptionalLong commitmentOid();

        /**
         * The optional account OID.
         * @return THe optional account OID.
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
         * A builder class for constructing immutable {@link UtilizationGroupKey} instances.
         */
        class Builder extends ImmutableUtilizationGroupKey.Builder {}
    }

    /**
     * A grouping of {@link CloudCommitmentUtilizationVector} instances based on the attributes
     * contained within {@link #key()}.
     */
    @HiddenImmutableTupleImplementation
    @Immutable(prehash = true)
    public interface CommitmentUtilizationGrouping {

        /**
         * The utilization group key.
         * @return The utilization group key.
         */
        @Nonnull
        UtilizationGroupKey key();

        /**
         * The utilization vector list.
         * @return The utilization vector list.
         */
        @Nonnull
        List<CloudCommitmentUtilizationVector> utilizationList();

        /**
         * Constructs a new {@link CommitmentUtilizationGrouping} instance.
         * @param key The utilization group key.
         * @param utilizationCollection The utilization vector collection.
         * @return The new {@link CommitmentUtilizationGrouping} instance.
         */
        @Nonnull
        static CommitmentUtilizationGrouping of(@Nonnull UtilizationGroupKey key,
                                                @Nonnull Collection<CloudCommitmentUtilizationVector> utilizationCollection) {
            return CommitmentUtilizationGroupingTuple.of(key, utilizationCollection);
        }
    }
}
