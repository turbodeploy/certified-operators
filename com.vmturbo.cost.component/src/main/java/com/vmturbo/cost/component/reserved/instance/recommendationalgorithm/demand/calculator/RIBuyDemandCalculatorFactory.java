package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.calculator;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceHistoricalDemandDataType;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyHistoricalDemandProvider;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.RegionalRIMatcherCache;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.RegionalRIMatcherCacheFactory;

/**
 * A factory class for creating instances of {@link RIBuyDemandCalculator}. The created demand calculator
 * is intended to be used across all demand contexts analyzed in a single round of RI buy analysis.
 */
public class RIBuyDemandCalculatorFactory {

    private final RegionalRIMatcherCacheFactory regionalRIMatcherCacheFactory;

    private final RIBuyHistoricalDemandProvider demandProvider;

    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private final float demandWeight;

    /**
     * Constructs a factory instance.
     *
     * @param regionalRIMatcherCacheFactory A {@link RegionalRIMatcherCacheFactory}, used to match RI
     *                                      inventory to analysis demand.
     * @param demandProvider The historical demand provider, providing access to recorded demand for
     *                       a target demand cluster.
     * @param reservedInstanceBoughtStore The {@link ReservedInstanceBoughtStore}.
     * @param demandWeight The demand weight used in recorded compute tier demand. This weight value
     *                     is necessary in order to apply recently purchased RIs to consumption demand.
     */
    public RIBuyDemandCalculatorFactory(@Nonnull RegionalRIMatcherCacheFactory regionalRIMatcherCacheFactory,
                                        @Nonnull RIBuyHistoricalDemandProvider demandProvider,
                                        @Nonnull ReservedInstanceBoughtStore reservedInstanceBoughtStore,
                                        float demandWeight) {

        this.regionalRIMatcherCacheFactory = Objects.requireNonNull(regionalRIMatcherCacheFactory);
        this.demandProvider = Objects.requireNonNull(demandProvider);
        this.reservedInstanceBoughtStore = Objects.requireNonNull(reservedInstanceBoughtStore);
        this.demandWeight = demandWeight;
    }


    /**
     * Creates a new demand calculator for the specified analysis info.
     *
     * @param topologyInfo The topology info.
     * @param cloudTopology The cloud topology.
     * @param demandDataType The demand data type to analyze in calculating uncovered demand.
     * @return A newly created calculator instance.
     */
    public RIBuyDemandCalculator newCalculator(@Nonnull TopologyInfo topologyInfo,
                                               @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                                               @Nonnull ReservedInstanceHistoricalDemandDataType demandDataType) {

        final RegionalRIMatcherCache riMatcherCache = regionalRIMatcherCacheFactory.createNewCache(
                cloudTopology, topologyInfo);

        return new RIBuyDemandCalculator(
                demandProvider,
                demandDataType,
                riMatcherCache,
                demandWeight,
                reservedInstanceBoughtStore);
    }


}
