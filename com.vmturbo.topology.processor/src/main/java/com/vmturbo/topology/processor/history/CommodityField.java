package com.vmturbo.topology.processor.history;

import java.util.function.Function;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;

/**
 * Define the target field of commodity DTO to be filled by historical aggregation.
 */
public enum CommodityField {
    /**
     * A "used" field description for identity purposes.
     */
    USED((sold) -> sold.hasUsed() ? sold.getUsed() : null,
         (bought) -> bought.hasUsed() ? bought.getUsed() : null,
         TopologyDTO.CommoditySoldDTO.Builder::getHistoricalUsedBuilder,
         TopologyDTO.CommodityBoughtDTO.Builder::getHistoricalUsedBuilder),
    /**
     * A "peak" field description for identity purposes.
     */
    PEAK(TopologyDTO.CommoditySoldDTO.Builder::getPeak,
         TopologyDTO.CommodityBoughtDTO.Builder::getPeak,
         TopologyDTO.CommoditySoldDTO.Builder::getHistoricalPeakBuilder,
         TopologyDTO.CommodityBoughtDTO.Builder::getHistoricalPeakBuilder);

    private final Function<TopologyDTO.CommoditySoldDTO.Builder, Double> soldValue;
    private final Function<TopologyDTO.CommodityBoughtDTO.Builder, Double> boughtValue;
    private final Function<TopologyDTO.CommoditySoldDTO.Builder, HistoricalValues.Builder> soldBuilder;
    private final Function<TopologyDTO.CommodityBoughtDTO.Builder, HistoricalValues.Builder> boughtBuilder;

    /**
     * Construct the field reference.
     *
     * @param soldValue how to get the running value from a sold commodity dto
     * @param boughtValue how to get the running value from a bought commodity dto
     * @param soldBuilder how to get the field dto builder from a sold commodity dto
     * @param boughtBuilder how to get the field dto builder from a bought commodity dto
     */
    private CommodityField(
                    @Nonnull Function<TopologyDTO.CommoditySoldDTO.Builder, Double> soldValue,
                    @Nonnull Function<TopologyDTO.CommodityBoughtDTO.Builder, Double> boughtValue,
                    @Nonnull Function<TopologyDTO.CommoditySoldDTO.Builder, HistoricalValues.Builder> soldBuilder,
                    @Nonnull Function<TopologyDTO.CommodityBoughtDTO.Builder, HistoricalValues.Builder> boughtBuilder) {
        this.soldValue = soldValue;
        this.boughtValue = boughtValue;
        this.soldBuilder = soldBuilder;
        this.boughtBuilder = boughtBuilder;
    }

    public Function<TopologyDTO.CommoditySoldDTO.Builder, HistoricalValues.Builder> getSoldBuilder() {
        return soldBuilder;
    }

    public Function<TopologyDTO.CommodityBoughtDTO.Builder, HistoricalValues.Builder> getBoughtBuilder() {
        return boughtBuilder;
    }

    public Function<TopologyDTO.CommoditySoldDTO.Builder, Double> getSoldValue() {
        return soldValue;
    }

    public Function<TopologyDTO.CommodityBoughtDTO.Builder, Double> getBoughtValue() {
        return boughtValue;
    }

}
