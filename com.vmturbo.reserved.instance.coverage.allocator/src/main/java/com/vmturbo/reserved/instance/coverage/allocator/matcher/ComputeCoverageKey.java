package com.vmturbo.reserved.instance.coverage.allocator.matcher;

import java.util.OptionalLong;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.reserved.instance.coverage.allocator.matcher.CommitmentMatcherConfig.Builder;

/**
 * A {@link CoverageKey} implementation for matching compute-based commitments to coverage entities (e.g.
 * an EC2 RI to an AWS VM).
 */
@HiddenImmutableImplementation
@Immutable(lazyhash = true)
public interface ComputeCoverageKey extends CoverageKey {

    /**
     * The tier family.
     * @return The tier family. May be null, if matching based on size-inflexibility.
     */
    @Nullable
    String tierFamily();

    /**
     * The compute tier OID.
     * @return The compute tier OID. May be empty, if matching based on size-flexibility.
     */
    @Nonnull
    OptionalLong tierOid();

    /**
     * The tenancy of the entity/cloud commitment.
     * @return The tenancy of the entity/cloud commitment. May be null, if matching is not dependent
     * on tenancy.
     */
    @Nullable
    Tenancy tenancy();

    /**
     * The platform of the entity/cloud commitment.
     * @return The platform of the entity/cloud commitment. May be null, if matching is not dependent
     * on platform (commitment is platform-flexible).
     */
    @Nullable
    OSType platform();

    /**
     * Constructs and returns a new {@link CommitmentMatcherConfig.Builder} instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link ComputeCoverageKey} instances.
     */
    class Builder extends ImmutableComputeCoverageKey.Builder {}
}
