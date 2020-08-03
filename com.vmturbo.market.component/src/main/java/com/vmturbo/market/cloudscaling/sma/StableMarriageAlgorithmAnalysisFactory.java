package com.vmturbo.market.cloudscaling.sma;

import javax.annotation.Nonnull;

import com.vmturbo.market.cloudscaling.sma.entities.SMAInput;

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
            @Nonnull SMAInput input);

}
