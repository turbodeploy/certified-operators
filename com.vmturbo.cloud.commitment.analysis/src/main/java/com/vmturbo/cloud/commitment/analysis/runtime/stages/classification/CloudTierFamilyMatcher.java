package com.vmturbo.cloud.commitment.analysis.runtime.stages.classification;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecMatcher;

/**
 * The matcher determines whether distinct instances of cloud tier demand are within the same
 * "family", meaning that the demand can be covered by a single recommendation. "Family" in this case
 * is synonymous with flexibility, given recommendations will generally be scoped to a single instance
 * family.
 */
public class CloudTierFamilyMatcher {

    private final CloudCommitmentSpecMatcher<?> cloudCommitmentSpecMatcher;

    /**
     * Constructs a new instance of the family matcher with the specified spec matcher, which is used
     * to determine its recommendation spec.
     * @param cloudCommitmentSpecMatcher The commitment spec matcher.
     */
    public CloudTierFamilyMatcher(@Nonnull CloudCommitmentSpecMatcher<?> cloudCommitmentSpecMatcher) {
        this.cloudCommitmentSpecMatcher = Objects.requireNonNull(cloudCommitmentSpecMatcher);
    }

    /**
     * Determines whether the two instances of {@link ScopedCloudTierInfo} can be covered by a single
     * recommendation (and therefore match).
     * @param demandA The first instance of {@link ScopedCloudTierInfo}.
     * @param demandB The second instance of {@link ScopedCloudTierInfo}.
     * @return True, if the two instances of demand can be covered by the same recommendation and should
     * therefore be considered within the "family". False if a recommendation spec can not be determined
     * for either of the demand instances or the recommendation specs do not match.
     */
    public boolean match(@Nonnull final ScopedCloudTierInfo demandA,
                         @Nonnull final ScopedCloudTierInfo demandB) {

        return cloudCommitmentSpecMatcher.matchDemandToSpecs(demandA)
                .map(matchingSpecDataA -> cloudCommitmentSpecMatcher.matchDemandToSpecs(demandB)
                        .map(matchingSpecDataB -> matchingSpecDataA.equals(matchingSpecDataB))
                        .orElse(false))
                .orElse(false);
    }

    /**
     * A factory class for creating instances of {@link CloudTierFamilyMatcher}.
     */
    public static class CloudTierFamilyMatcherFactory {

        /**
         * Constructs a new instance of {@link CloudTierFamilyMatcher} with the specified spec matcher.
         * @param cloudCommitmentSpecMatcher The {@link CloudCommitmentSpecMatcher} instance, used to
         *                                   determine the recommendation spec of cloud tier demand.
         * @return The newly created instance of {@link CloudTierFamilyMatcher}.
         */
        @Nonnull
        public CloudTierFamilyMatcher newFamilyMatcher(
                @Nonnull CloudCommitmentSpecMatcher<?> cloudCommitmentSpecMatcher) {

            return new CloudTierFamilyMatcher(cloudCommitmentSpecMatcher);
        }
    }
}
