package com.vmturbo.topology.processor.history;

import java.util.function.Function;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.HistoricalValuesImpl;

/**
 * Define the target field of commodity DTO to be filled by historical aggregation.
 */
public enum CommodityField {
    /**
     * A "used" field description for identity purposes.
     */
    USED((sold) -> sold.hasUsed() ? sold.getUsed() : null,
         (bought) -> bought.hasUsed() ? bought.getUsed() : null,
         CommoditySoldImpl::getOrCreateHistoricalUsed,
         CommodityBoughtImpl::getOrCreateHistoricalUsed),
    /**
     * A "peak" field description for identity purposes.
     */
    PEAK(CommoditySoldImpl::getPeak,
         CommodityBoughtImpl::getPeak,
         CommoditySoldImpl::getOrCreateHistoricalPeak,
         CommodityBoughtImpl::getOrCreateHistoricalPeak);

    private final Function<CommoditySoldImpl, Double> soldValue;
    private final Function<CommodityBoughtImpl, Double> boughtValue;
    private final Function<CommoditySoldImpl, HistoricalValuesImpl> soldBuilder;
    private final Function<CommodityBoughtImpl, HistoricalValuesImpl> boughtBuilder;

    /**
     * Construct the field reference.
     *
     * @param soldValue how to get the running value from a sold commodity dto
     * @param boughtValue how to get the running value from a bought commodity dto
     * @param soldBuilder how to get the field dto builder from a sold commodity dto
     * @param boughtBuilder how to get the field dto builder from a bought commodity dto
     */
    private CommodityField(
                    @Nonnull Function<CommoditySoldImpl, Double> soldValue,
                    @Nonnull Function<CommodityBoughtImpl, Double> boughtValue,
                    @Nonnull Function<CommoditySoldImpl, HistoricalValuesImpl> soldBuilder,
                    @Nonnull Function<CommodityBoughtImpl, HistoricalValuesImpl> boughtBuilder) {
        this.soldValue = soldValue;
        this.boughtValue = boughtValue;
        this.soldBuilder = soldBuilder;
        this.boughtBuilder = boughtBuilder;
    }

    public Function<CommoditySoldImpl, HistoricalValuesImpl> getSoldBuilder() {
        return soldBuilder;
    }

    public Function<CommodityBoughtImpl, HistoricalValuesImpl> getBoughtBuilder() {
        return boughtBuilder;
    }

    public Function<CommoditySoldImpl, Double> getSoldValue() {
        return soldValue;
    }

    public Function<CommodityBoughtImpl, Double> getBoughtValue() {
        return boughtValue;
    }

}
