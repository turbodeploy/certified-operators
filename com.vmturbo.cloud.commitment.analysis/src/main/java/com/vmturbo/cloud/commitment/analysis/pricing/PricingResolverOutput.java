package com.vmturbo.cloud.commitment.analysis.pricing;

import java.util.Set;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.runtime.data.AnalysisTopology;

/**
 * This class represents the output of the Pricing resolver stage.
 */
@Immutable
public interface PricingResolverOutput {

    /**
     * Set of rate annotated commitment context.
     *
     * @return The rate annotated commitment context set.
     */
    Set<RateAnnotatedCommitmentContext> rateAnnotatedCommitmentContextSet();

    /**
     * The analysis topology having segmented demand breakdown.
     *
     * @return The analysis topology.
     */
    AnalysisTopology analysisTopology();

    /**
     * Static inner class for extending generated or yet to be generated builder.
     */
    class Builder extends ImmutablePricingResolverOutput.Builder {}

    /**
     * Returns a builder of the PricingResolverOutput class.
     *
     * @return The builder.
     */
    static PricingResolverOutput.Builder builder() {
        return new PricingResolverOutput.Builder();
    }
}
