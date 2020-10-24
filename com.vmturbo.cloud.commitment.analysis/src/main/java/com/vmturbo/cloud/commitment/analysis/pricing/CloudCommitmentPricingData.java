package com.vmturbo.cloud.commitment.analysis.pricing;

/**
 * Interface representing the cloud commitment pricing data.
 */
public interface CloudCommitmentPricingData {

    /**
     * The amortized hourly rate for the cloud commitment.
     * @return The amortized hourly rate for the cloud commitment.
     */
    double amortizedHourlyRate();
}
