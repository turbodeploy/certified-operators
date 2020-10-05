package com.vmturbo.cloud.commitment.analysis.spec;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;

/**
 * The reserved instance spec filter factory is used to create a Cloud commitment spec filter.
 */
public class RISpecPurchaseFilterFactory {
    private static final Logger logger = LogManager.getLogger();

    private final CloudCommitmentSpecResolver<ReservedInstanceSpec> riSpecResolver;

    private final ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory;

    /**
     * Constructs a new factory instance.
     * @param riSpecResolver The RI spec resolver, used to query all specs in inventory.
     * @param computeTierFamilyResolverFactory A factory for creating {@link ComputeTierFamilyResolver} instances.
     */
    public RISpecPurchaseFilterFactory(@Nonnull final CloudCommitmentSpecResolver<ReservedInstanceSpec> riSpecResolver,
                                       @Nonnull ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory) {
        this.riSpecResolver = Objects.requireNonNull(riSpecResolver);
        this.computeTierFamilyResolverFactory = Objects.requireNonNull(computeTierFamilyResolverFactory);
    }

    /**
     * Constructs a new {@link RISpecPurchaseFilter} instance.
     * @param cloudTopology The {@link CloudTopology}, used to create a {@link ComputeTierFamilyResolver}
     *                      instance.
     * @param purchaseProfile The purchase profile to filter RI specs against.
     * @return The newly created {@link RISpecPurchaseFilter} instance.
     */
    public RISpecPurchaseFilter createFilter(
            @Nonnull final CloudTopology<TopologyEntityDTO> cloudTopology,
            final CommitmentPurchaseProfile purchaseProfile) {

        final ComputeTierFamilyResolver computeTierFamilyResolver =
                computeTierFamilyResolverFactory.createResolver(cloudTopology);

        return new RISpecPurchaseFilter(cloudTopology,
                riSpecResolver,
                computeTierFamilyResolver,
                purchaseProfile.getRiPurchaseProfile());
    }
}
