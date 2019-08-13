package com.vmturbo.topology.processor.history;

import java.util.function.Consumer;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO;

/**
 * Define the target field of commodity DTO to be filled by historical aggregation.
 */
public enum CommodityField {
    USED(TopologyDTO.CommoditySoldDTO.Builder::getHistoricalUsedBuilder,
         TopologyDTO.CommodityBoughtDTO.Builder::getHistoricalUsedBuilder),
    PEAK(TopologyDTO.CommoditySoldDTO.Builder::getHistoricalPeakBuilder,
         TopologyDTO.CommodityBoughtDTO.Builder::getHistoricalPeakBuilder);

    private final Consumer<TopologyDTO.CommoditySoldDTO.Builder> soldBuilder;
    private final Consumer<TopologyDTO.CommodityBoughtDTO.Builder> boughtBuilder;

    /**
     * Construct the field reference.
     *
     * @param soldBuilder how to get the field dto builder from a sold commodity dto
     * @param boughtBuilder how to get the field dto builder from a bought commodity dto
     */
    private CommodityField(
                    @Nonnull Consumer<TopologyDTO.CommoditySoldDTO.Builder> soldBuilder,
                    @Nonnull Consumer<TopologyDTO.CommodityBoughtDTO.Builder> boughtBuilder) {
        this.soldBuilder = soldBuilder;
        this.boughtBuilder = boughtBuilder;
    }

    public Consumer<TopologyDTO.CommoditySoldDTO.Builder> getSoldBuilder() {
        return soldBuilder;
    }

    public Consumer<TopologyDTO.CommodityBoughtDTO.Builder> getBoughtBuilder() {
        return boughtBuilder;
    }

}
