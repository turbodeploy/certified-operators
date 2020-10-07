package com.vmturbo.cloud.commitment.analysis.spec;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.runtime.data.AnalysisTopology;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * Represents the output of the CCARecommendationSpecMatcherStage.
 */
@HiddenImmutableImplementation
@Immutable
public interface SpecMatcherOutput {
    /**
     * The Commitment spec demand set representing the potential set of CCA specs to recommend.
     *
     * @return The commitment spec demand set.
     */
    CommitmentSpecDemandSet commitmentSpecDemandSet();

    /**
     * The analysis topology containing segment demand for cca.
     *
     * @return The Analysis topology.
     */
    AnalysisTopology analysisTopology();

    /**
     * Static inner class for extending generated or yet to be generated builder.
     */
    class Builder extends ImmutableSpecMatcherOutput.Builder {}

    /**
     * Returns a builder of the SpecMatcherOutput class.
     *
     * @return The builder.
     */
    static SpecMatcherOutput.Builder builder() {
        return new SpecMatcherOutput.Builder();
    }
}
