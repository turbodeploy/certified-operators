package com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation;

import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * Info about the covered demand/entities for a cloud commitment recommendation.
 */
@HiddenImmutableImplementation
@Immutable
public interface CoveredDemandInfo {

    /**
     * The set of all {@link ScopedCloudTierInfo} in scope of the recommendation. This may include demand
     * which is entirely covered by the cloud commitment inventory.
     * @return The set of all {@link ScopedCloudTierInfo} in scope of the recommendation.
     */
    @Nonnull
    Set<ScopedCloudTierInfo> cloudTierSet();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing {@link CoveredDemandInfo} instances.
     */
    class Builder extends ImmutableCoveredDemandInfo.Builder{}
}
