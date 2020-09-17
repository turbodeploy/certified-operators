package com.vmturbo.cloud.commitment.analysis.spec;

import java.util.Set;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand;

/**
 * Interface representing aggregated cloud tier demand being matched with cloud commitment spec data.
 */
@Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Immutable
public interface CommitmentSpecDemand {

    /**
     * The info about the spec the demand is matched to.
     *
     * @return the Cloud Commitment Spec Data.
     */
    CloudCommitmentSpecData cloudCommitmentSpecData();

    /**
     * The set of aggregated cloud tier demand which matches to the cloud commitment spec.
     *
     * @return The set of aggregated cloud tier demand.
     */
    Set<AggregateCloudTierDemand> aggregateCloudTierDemandSet();

    /**
     * Returns a builder of the CommitmentSpecDemand class.
     *
     * @return The builder.
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * Static inner class for extending generated or yet to be generated builder.
     */
    class Builder extends ImmutableCommitmentSpecDemand.Builder {}
}

