package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.integration.CloudTopology;

/**
 * A factory class for creating instances of {@link RegionalRIMatcherCache}.
 */
public class RegionalRIMatcherCacheFactory {

    private final ReservedInstanceSpecMatcherFactory riSpecMatcherFactory;

    private final ReservedInstanceInventoryMatcherFactory riInventoryMatcherFactory;

    private final ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory;

    /**
     * Creates an instance of {@link RegionalRIMatcherCacheFactory}.
     *
     * @param riSpecMatcherFactory A factory for creating instances of RI spec matchers.
     * @param riInventoryMatcherFactory A factory for creating instances of RI inventory matchers.
     * @param computeTierFamilyResolverFactory The compute tier family resolver, used to resolve the family
     *                                         coverage for size flexible RIs.
     */
    public RegionalRIMatcherCacheFactory(@Nonnull ReservedInstanceSpecMatcherFactory riSpecMatcherFactory,
                                         @Nonnull ReservedInstanceInventoryMatcherFactory riInventoryMatcherFactory,
                                         @Nonnull ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory) {

        this.riSpecMatcherFactory = Objects.requireNonNull(riSpecMatcherFactory);
        this.riInventoryMatcherFactory = Objects.requireNonNull(riInventoryMatcherFactory);
        this.computeTierFamilyResolverFactory = Objects.requireNonNull(computeTierFamilyResolverFactory);
    }

    /**
     * Creates a new cached instance, in which the matcher instances will be based on the provided
     * {@code cloudTopology} and {@code purchaseConstraints}. The purchase constraints will be used
     * by the RI spec matcher in determining which RI specs to recommend for a set of demand clusters.
     *
     * @param cloudTopology The cloud topology associated with a round of analysis.
     * @param topologyInfo The info about the topology.
     * @return A newly created instance of {@link RegionalRIMatcherCache}.
     */
    public RegionalRIMatcherCache createNewCache(@Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                                 @Nonnull TopologyInfo topologyInfo) {

        final ComputeTierFamilyResolver computeTierFamilyResolver = computeTierFamilyResolverFactory.createResolver(cloudTopology);

        return new RegionalRIMatcherCache(
                riSpecMatcherFactory,
                riInventoryMatcherFactory,
                cloudTopology,
                computeTierFamilyResolver,
                topologyInfo);
    }
}
