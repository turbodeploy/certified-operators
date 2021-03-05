package com.vmturbo.cloud.commitment.analysis.spec;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.spec.catalog.ReservedInstanceCatalog;
import com.vmturbo.cloud.commitment.analysis.spec.catalog.ReservedInstanceCatalog.ReservedInstanceCatalogFactory;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;

/**
 * The reserved instance spec filter factory is used to create a Cloud commitment spec filter.
 */
public class RISpecPurchaseFilterFactory {

    private static final Logger logger = LogManager.getLogger();

    private final ReservedInstanceCatalogFactory reservedInstanceCatalogFactory;

    private final ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory;

    /**
     * Constructs a new factory instance.
     * @param reservedInstanceCatalogFactory The RI catalog, used to query all specs available for purchase.
     * @param computeTierFamilyResolverFactory A factory for creating {@link ComputeTierFamilyResolver} instances.
     */
    public RISpecPurchaseFilterFactory(@Nonnull final ReservedInstanceCatalogFactory reservedInstanceCatalogFactory,
                                       @Nonnull ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory) {
        this.reservedInstanceCatalogFactory = Objects.requireNonNull(reservedInstanceCatalogFactory);
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

        final ReservedInstanceCatalog reservedInstanceCatalog =
                reservedInstanceCatalogFactory.createAccountRestrictedCatalog(purchaseProfile.getAccountPurchaseRestrictionsList());

        return new RISpecPurchaseFilter(cloudTopology,
                reservedInstanceCatalog,
                computeTierFamilyResolver,
                purchaseProfile.getRiPurchaseProfile());
    }
}
