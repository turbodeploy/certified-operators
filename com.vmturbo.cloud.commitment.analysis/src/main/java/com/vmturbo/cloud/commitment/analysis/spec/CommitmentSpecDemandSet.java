package com.vmturbo.cloud.commitment.analysis.spec;

import java.util.Set;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

/**
 * Interface encapsulating the output of the CCA Recommendation Spec Matcher Stage. The output is a
 * set of CommitmentSpecDemand.
 */
@Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Immutable
public interface CommitmentSpecDemandSet {
    /**
     * A commitment spec demand set where each spec demand contains info about the demand and info about
     * the matched specs.
     *
     * @return A set of commitment spec demands.
     */
    Set<CommitmentSpecDemand> commitmentSpecDemand();

    /**
     * Returns a builder of the CommitmentSpecDemandSet class.
     *
     * @return The builder.
     */
    static CommitmentSpecDemandSet.Builder builder() {
        return new CommitmentSpecDemandSet.Builder();
    }

    /**
     *  Static inner class for extending generated or yet to be generated builder.
     */
    class Builder extends ImmutableCommitmentSpecDemandSet.Builder {}
}
