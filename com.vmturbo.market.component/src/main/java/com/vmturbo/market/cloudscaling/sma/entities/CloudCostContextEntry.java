package com.vmturbo.market.cloudscaling.sma.entities;

/**
 * Wrapper for storing the CostContext Float pair.
 */
public class CloudCostContextEntry {
    /**
     * the type assigned in the traderTO.
     */
    private final CostContext costContext;
    /**
     * the commodityType in the TopologyDTO.
     */
    private final Float costValue;

    /**
     * Constructor.
     * @param costContext the cost context
     * @param costValue the actual cost.
     */
    public CloudCostContextEntry(CostContext costContext, Float costValue) {
        this.costContext = costContext;
        this.costValue = costValue;
    }

    public CostContext getCostContext() {
        return costContext;
    }

    public Float getCostValue() {
        return costValue;
    }
}
