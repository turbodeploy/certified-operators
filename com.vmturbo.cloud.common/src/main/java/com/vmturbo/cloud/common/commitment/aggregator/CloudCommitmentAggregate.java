package com.vmturbo.cloud.common.commitment.aggregator;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.immutables.value.Value.Auxiliary;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Lazy;

import com.vmturbo.cloud.common.commitment.CloudCommitmentData;
import com.vmturbo.cloud.common.commitment.CommitmentAmountCalculator;
import com.vmturbo.cloud.common.commitment.CommitmentAmountUtils;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageTypeInfo;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentType;

/**
 * A container for the aggregation of a set of cloud commitments, based on some aggregation logic. The
 * aggregation is designed to contain only a single cloud commitment type.
 */
public interface CloudCommitmentAggregate {

    /**
     * An ID assigned to the aggregate during aggregation. In the case of identity aggregation, this
     * ID will mirror the underlying commitment ID.
     * @return The aggregate ID.
     */
    long aggregateId();

    /**
     * The info used to aggregate the commitments.
     * @return The info used to aggregate the commitments.
     */
    AggregateInfo aggregateInfo();

    /**
     * The set of commitments contained within this aggregate. In the case of identity aggregation,
     * this will always contain a single commitment.
     * @return The set of commitments contained within this aggregate.
     */
    @Auxiliary
    Set<? extends CloudCommitmentData> commitments();

    /**
     * The commitment capacity of this aggregate.
     * @return The total commitment capacity of this aggregate.
     */
    @Derived
    @Nonnull
    default CloudCommitmentAmount commitmentCapacity() {
        return CommitmentAmountCalculator.sum(
                commitments().stream()
                        .map(CloudCommitmentData::capacity)
                        .collect(ImmutableList.toImmutableList()));
    }

    /**
     * The immutable map of commitment capacity, broken down by coverage type.
     * @return The immutable map of commitment capacity, broken down by coverage type.
     */
    @Lazy
    @Nonnull
    default Map<CloudCommitmentCoverageTypeInfo, CloudCommitmentAmount> capacityTypeMap() {
        return CommitmentAmountUtils.groupByKey(commitmentCapacity());
    }

    /**
     * The immutable set of coverage types supported by this commitment aggregate.
     * @return The immutable set of coverage types.
     */
    @Lazy
    @Nonnull
    default Set<CloudCommitmentCoverageTypeInfo> coverageTypeInfoSet() {
        return ImmutableSet.copyOf(capacityTypeMap().keySet());
    }

    /**
     * Returns the commitment capacity for {@code coverageTypeInfo}. If this commitment aggregate does not
     * have any capacity of the specified type, {@link CommitmentAmountUtils#EMPTY_COMMITMENT_AMOUNT} will
     * be returned.
     * @param coverageTypeInfo The coverage type info.
     * @return The commitment capacity for the specified coverage type.
     */
    @Nonnull
    default CloudCommitmentAmount capacityByType(@Nonnull CloudCommitmentCoverageTypeInfo coverageTypeInfo) {
        return capacityTypeMap().getOrDefault(coverageTypeInfo, CommitmentAmountUtils.EMPTY_COMMITMENT_AMOUNT);
    }

    /**
     * The cloud commitment type of this aggregate.
     * @return The cloud commitment type of this aggregate.
     */
    @Derived
    default CloudCommitmentType commitmentType() {
        return aggregateInfo().commitmentType();
    }

    /**
     * Determines whether the set of commitments stored within {@link #commitments()} are reserved
     * instances.
     * @return True, if this aggregation represents a set of reserved instances.
     */
    @Derived
    default boolean isReservedInstance() {
        return commitmentType() == CloudCommitmentType.RESERVED_INSTANCE;
    }

    /**
     * Converts this aggregate to a {@link ReservedInstanceAggregate}.
     * @return This aggregate, as a {@link ReservedInstanceAggregate} instance.
     */
    @Auxiliary
    @Derived
    default ReservedInstanceAggregate asReservedInstanceAggregate() {
        return (ReservedInstanceAggregate)this;
    }
}
