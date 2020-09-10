package com.vmturbo.cloud.commitment.analysis.demand;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Represents demand for a compute tier, detached from an entity.
 */
@Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Immutable(lazyhash = true)
public interface ComputeTierDemand extends CloudTierDemand {

    /**
     * The OS of the demand.
     *
     * @return The OS of the demand.
     */
    @Nonnull
    OSType osType();

    /**
     * The tenancy of the demand.
     *
     * @return The tenancy of the demand.
     */
    @Nonnull
    Tenancy tenancy();

    /**
     * Constructs and returns a new builder instance.
     * @return A newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link ComputeTierDemand}.
     */
    class Builder extends ImmutableComputeTierDemand.Builder {}
}
