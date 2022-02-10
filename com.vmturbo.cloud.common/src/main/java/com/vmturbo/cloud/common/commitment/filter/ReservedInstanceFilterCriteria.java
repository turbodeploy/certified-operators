package com.vmturbo.cloud.common.commitment.filter;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverageType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentType;

/**
 * A {@link CloudCommitmentFilterCriteria}, specific to filtering reserved instances.
 */
@HiddenImmutableImplementation
@Immutable
public interface ReservedInstanceFilterCriteria extends CloudCommitmentFilterCriteria {

    /**
     * {@inheritDoc}.
     */
    @Override
    @Derived
    default CloudCommitmentType type() {
        return CloudCommitmentType.RESERVED_INSTANCE;
    }

    @Derived
    @Override
    default CloudCommitmentCoverageType coverageType() {
        return CloudCommitmentCoverageType.COUPONS;
    }

    @Nonnull
    @Override
    ComputeScopeFilterCriteria resourceScopeCriteria();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link ReservedInstanceFilterCriteria} instances.
     */
    class Builder extends ImmutableReservedInstanceFilterCriteria.Builder {}
}
