package com.vmturbo.market.cloudvmscaling;

import javax.annotation.Nonnull;

import com.vmturbo.market.cloudvmscaling.entities.SMAInput;

/**
 * A factory class to create {@Link StableMarriageAlgorithmAnalysis} instances.
 *
 */
public interface StableMarriageAlgorithmAnalysisFactory {

    /**
     * Create a new {@Link StableMarriageAlgorithmAnalysis}.
     *
     * @param input the SMA input
     * @return output of SMA
     */
    @Nonnull
    StableMarriageAlgorithmAnalysis newStableMarriageAlgorithmAnalysis(
            @Nonnull final SMAInput input);

}
