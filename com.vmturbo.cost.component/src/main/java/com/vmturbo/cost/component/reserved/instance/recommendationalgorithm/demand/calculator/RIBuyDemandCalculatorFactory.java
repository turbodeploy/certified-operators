package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.calculator;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceHistoricalDemandDataType;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyHistoricalDemandProvider;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.RegionalRIMatcherCache;

/**
 * A factory class for creating instances of {@link RIBuyDemandCalculator}. The created demand calculator
 * is intended to be used across all demand contexts analyzed in a single round of RI buy analysis.
 */
public class RIBuyDemandCalculatorFactory {


    private final RIBuyHistoricalDemandProvider demandProvider;

    private final float demandWeight;

    /**
     * Constructs a factory instance.
     *
     * @param demandProvider The historical demand provider, providing access to recorded demand for
     *                       a target demand cluster.
     * @param demandWeight The demand weight used in recorded compute tier demand. This weight value
     *                     is necessary in order to apply recently purchased RIs to consumption demand.
     */
    public RIBuyDemandCalculatorFactory(@Nonnull RIBuyHistoricalDemandProvider demandProvider,
                                        float demandWeight) {
        this.demandProvider = Objects.requireNonNull(demandProvider);
        this.demandWeight = demandWeight;
    }


    /**
     * Creates a new demand calculator for the specified analysis info.
     *
     * @param regionalRIMatcherCache A cache providing access to RI spec matchers and RI inventory matchers
     *                               for a target region.
     * @param demandDataType The demand data type to analyze in calculating uncovered demand.
     * @param riBoughtStore The ReservedINstanceBoughtStore to query the initial
     *                     discovery time for each RI.
     * @return A newly created calculator instance.
     */
    public RIBuyDemandCalculator newCalculator(@Nonnull RegionalRIMatcherCache regionalRIMatcherCache,
                                               @Nonnull ReservedInstanceHistoricalDemandDataType demandDataType,
                                               @Nonnull ReservedInstanceBoughtStore riBoughtStore) {
        return new RIBuyDemandCalculator(
                demandProvider,
                demandDataType,
                regionalRIMatcherCache,
                demandWeight,
                riBoughtStore);
    }


}
