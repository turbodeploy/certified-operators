package com.vmturbo.reserved.instance.coverage.allocator.matcher.entity;

import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * An {@link EntityMatcherConfig} implementation, specific to generating coverage keys for
 * virtual machines.
 */
@HiddenImmutableImplementation
@Immutable
public interface VirtualMachineMatcherConfig extends EntityMatcherConfig {

    /**
     * The set of tier matchers, indicating how a VM should be matched to a cloud commitment
     * through its compute tier.
     * @return The set of tier matchers.
     */
    @Nonnull
    Set<TierMatcher> tierMatchers();

    /**
     * Indicates whether the VM's tenancy should be included in generated coverage keys.
     * @return Whether the VM's tenancy should be included in generated coverage keys.
     */
    boolean includeTenancy();

    /**
     * Indicates whether the VM's platform should be included in generated coverage keys.
     * @return Whether the VM's platform should be included in generated coverage keys.
     */
    boolean includePlatform();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link VirtualMachineMatcherConfig}.
     */
    class Builder extends ImmutableVirtualMachineMatcherConfig.Builder {}

    /**
     * Represents the possible ways to match an entity's tier to a cloud commitment (through a coverage
     * key).
     */
    enum TierMatcher {
        FAMILY,
        TIER,
        NONE
    }
}
