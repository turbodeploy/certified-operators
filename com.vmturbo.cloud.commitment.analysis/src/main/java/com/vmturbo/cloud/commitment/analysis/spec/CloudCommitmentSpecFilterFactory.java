package com.vmturbo.cloud.commitment.analysis.spec;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile;
import com.vmturbo.cost.calculation.integration.CloudTopology;

/**
 * Interface describing the cloud commitment spec filter factory.
 *
 * @param <SPEC_TYPE> The spec type.
 */
public interface CloudCommitmentSpecFilterFactory<SPEC_TYPE> {

    /**
     * Creates a cloud commitment spec filter based on a given purchase profile and cloud topology.
     *
     * @param commitmentPurchaseProfile The purchase profile.
     * @param cloudTopology The cloud topology.
     *
     * @return The Cloud commitment spec filter.
     */
    CloudCommitmentSpecFilter<SPEC_TYPE> createFilter(@Nonnull CommitmentPurchaseProfile commitmentPurchaseProfile,
            @Nonnull CloudTopology cloudTopology);
}
