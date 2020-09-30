package com.vmturbo.cloud.common.commitment.aggregator;

import java.util.OptionalLong;

import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;

/**
 * Represents the base information required for the aggregation of cloud commitments .
 */
public interface AggregateInfo {

    /**
     * The cloud commitment type.
     * @return The cloud commitment type.
     */
    CloudCommitmentType commitmentType();

    /**
     * The billing family ID. This will be set as long as the commitment is associated with a billing family.
     * @return The billing family ID of the cloud commitment.
     */
    OptionalLong billingFamilyId();

    /**
     * The purchasing account ID of the commitment. This will be set if the commitment is shared across
     * a billing family or it is scoped to a set of accounts that includes the purchasing account
     * (allowing for potential priority in covering entities within the purchasing account. This will
     * not be set if the commitment is scoped to a set of accounts that do not include the purchasing
     * account.
     * @return The purchasing account ID of the commitment.
     */
    OptionalLong purchasingAccountOid();
}
