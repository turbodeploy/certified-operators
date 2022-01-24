package com.vmturbo.cloud.common.commitment.aggregator;

import java.util.OptionalLong;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentEntityScope;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentLocation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentStatus;

/**
 * Represents the base information required for the aggregation of cloud commitments .
 */
public interface AggregationInfo {

    /**
     * The service provider OID.
     * @return The service provider OID.
     */
    long serviceProviderOid();

    /**
     * The cloud commitment type.
     * @return The cloud commitment type.
     */
    CloudCommitmentType commitmentType();

    /**
     * The purchasing account ID of the commitment. This will be set if the commitment is shared across
     * a billing family or it is scoped to a set of accounts that includes the purchasing account
     * (allowing for potential priority in covering entities within the purchasing account. This will
     * not be set if the commitment is scoped to a set of accounts that do not include the purchasing
     * account.
     * @return The purchasing account ID of the commitment.
     */
    OptionalLong purchasingAccountOid();

    /**
     * The coverage type.
     * @return The coverage type.
     */
    @Nonnull
    CloudCommitmentCoverageType coverageType();

    /**
     * The entity scope of this aggregate. Entity scope encapsulates the root of the coverable entities.
     * @return The entity scope of this aggregate.
     */
    @Nonnull
    CloudCommitmentEntityScope entityScope();

    /**
     * The location (global, regional, zonal) of the cloud commitment.
     * @return The cloud commitment location.
     */
    @Nonnull
    CloudCommitmentLocation location();

    /**
     * The cloud commitment status.
     * @return The cloud commitment status.
     */
    @Nonnull
    CloudCommitmentStatus status();
}
