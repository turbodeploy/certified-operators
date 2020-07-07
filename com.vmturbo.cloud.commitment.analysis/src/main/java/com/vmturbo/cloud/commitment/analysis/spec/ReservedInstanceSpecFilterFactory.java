package com.vmturbo.cloud.commitment.analysis.spec;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile;
import com.vmturbo.cost.calculation.integration.CloudTopology;

/**
 * The reserved instance spec filter factory is used to create a Cloud commitment spec filter.
 */
public class ReservedInstanceSpecFilterFactory implements CloudCommitmentSpecFilterFactory {
    private static final Logger logger = LogManager.getLogger();

    private final CloudCommitmentSpecResolver cloudCommitmentSpecResolver;

    /**
     * Constructor for the cloud commitment spec filter factory.
     *
     * @param cloudCommitmentSpecResolver The cloud commitment spec resolver.
     */
    public ReservedInstanceSpecFilterFactory(@Nonnull final CloudCommitmentSpecResolver cloudCommitmentSpecResolver) {
        this.cloudCommitmentSpecResolver = Objects.requireNonNull(cloudCommitmentSpecResolver);
    }

    @Override
    public ReservedInstanceSpecFilter createFilter(@Nonnull CommitmentPurchaseProfile commitmentPurchaseProfile,
                                                  @Nonnull CloudTopology cloudTopology) {
        return new ReservedInstanceSpecFilter(cloudTopology, commitmentPurchaseProfile.getRiPurchaseProfile().getRiTypeByRegionOidMap(), cloudCommitmentSpecResolver);
    }
}
