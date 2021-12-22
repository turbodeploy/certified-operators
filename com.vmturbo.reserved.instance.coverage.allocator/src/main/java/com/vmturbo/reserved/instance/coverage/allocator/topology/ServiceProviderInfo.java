package com.vmturbo.reserved.instance.coverage.allocator.topology;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * Service provider information required for coverage allocation.
 */
@HiddenImmutableImplementation
@Immutable
public interface ServiceProviderInfo {

    /**
     * The service provider OID.
     * @return The service provider OID.
     */
    long oid();

    /**
     * The display name of the service provider.
     * @return The display name of the service provider.
     */
    @Nonnull
    String name();

    /**
     * The reference ID of the service provider, used as a constant reference across
     * topologies.
     * @return The reference ID of the service provider, used as a constant reference across
     * topologies.
     */
    @Derived
    default String referenceId() {
        return name().toLowerCase();
    }

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed {@link Builder} instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing immutable {@link ServiceProviderInfo} instances.
     */
    class Builder extends ImmutableServiceProviderInfo.Builder {}
}
