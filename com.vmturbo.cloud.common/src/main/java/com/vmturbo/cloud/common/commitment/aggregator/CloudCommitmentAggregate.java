package com.vmturbo.cloud.common.commitment.aggregator;

import java.util.Set;

import org.immutables.value.Value.Auxiliary;
import org.immutables.value.Value.Derived;

import com.vmturbo.cloud.common.commitment.CloudCommitmentData;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;

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
