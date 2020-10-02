package com.vmturbo.reserved.instance.coverage.allocator.topology;

import java.util.Optional;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * A data class containing info related to a compute tier.
 */
@HiddenImmutableImplementation
@Immutable
public interface ComputeTierInfo {

    /**
     * The family of the compute tier.
     * @return The family of the compute tier. May be empty if the compute tier does not specify a
     * family or if the tier's coupon value is not positive.
     */
    @Nonnull
    Optional<String> family();

    /**
     * The tier OID.
     * @return The tier OID.
     */
    long tierOid();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link ComputeTierInfo} instances.
     */
    class Builder extends ImmutableComputeTierInfo.Builder {}
}
