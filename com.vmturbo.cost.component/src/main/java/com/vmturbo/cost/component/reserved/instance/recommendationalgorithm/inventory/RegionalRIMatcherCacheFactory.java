package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstancePurchaseConstraints;

/**
 * A factory class for creating instances of {@link RegionalRIMatcherCache}.
 */
public class RegionalRIMatcherCacheFactory {

    private final ReservedInstanceSpecMatcherFactory riSpecMatcherFactory;

    private final ReservedInstanceInventoryMatcherFactory riInventoryMatcherFactory;

    /**
     * Creates an instance of {@link RegionalRIMatcherCacheFactory}.
     *
     * @param riSpecMatcherFactory A factory for creating instances of RI spec matchers.
     * @param riInventoryMatcherFactory A factory for creating instances of RI inventory matchers.
     */
    public RegionalRIMatcherCacheFactory(@Nonnull ReservedInstanceSpecMatcherFactory riSpecMatcherFactory,
                                         @Nonnull ReservedInstanceInventoryMatcherFactory riInventoryMatcherFactory) {

        this.riSpecMatcherFactory = Objects.requireNonNull(riSpecMatcherFactory);
        this.riInventoryMatcherFactory = Objects.requireNonNull(riInventoryMatcherFactory);
    }

    /**
     * Creates a new cached instance, in which the matcher instances will be based on the provided
     * {@code cloudTopology} and {@code purchaseConstraints}. The purchase constraints will be used
     * by the RI spec matcher in determining which RI specs to recommend for a set of demand clusters.
     *
     * @param cloudTopology The cloud topology associated with a round of analysis.
     * @param purchaseConstraints The purchase constraints for the analysis.
     * @return A newly created instance of {@link RegionalRIMatcherCache}.
     */
    public RegionalRIMatcherCache createNewCache(@Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                                 @Nonnull ReservedInstancePurchaseConstraints purchaseConstraints) {
        return new RegionalRIMatcherCache(
            riSpecMatcherFactory,
            riInventoryMatcherFactory,
            cloudTopology,
            purchaseConstraints);
    }
}
