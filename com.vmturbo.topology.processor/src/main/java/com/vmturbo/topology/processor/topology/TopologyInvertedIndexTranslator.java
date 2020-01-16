package com.vmturbo.topology.processor.topology;

import com.google.common.annotations.VisibleForTesting;

import java.io.Serializable;
import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.commons.analysis.InvertedIndexBaseTranslator;
import com.vmturbo.commons.analysis.NumericIDAllocator;
import com.vmturbo.stitching.TopologyEntity;

/**
 * A mapper that implements methods that are used by the InvertedIndex.
 *
 */
public class TopologyInvertedIndexTranslator implements InvertedIndexBaseTranslator<TopologyEntity,
        TopologyEntityDTO.CommoditiesBoughtFromProvider, List<TopologyDTO.CommoditySoldDTO>>, Serializable {

    private final NumericIDAllocator commodityTypeAllocator = new NumericIDAllocator();

    /**
     * Create a new inverted index translator.
     */
    public TopologyInvertedIndexTranslator() {}

    /**
     * return an array of type values sold by a seller.
     *
     * @param seller whose sold commodities are to be translated to an array of integers
     * @return array of integers representing the types in the basketSold
     */
    public int[] getCommoditySoldTypeArrayFromEntity(final TopologyEntity seller) {
        return getCommoditySoldTypeArray(seller.getTopologyEntityDtoBuilder().getCommoditySoldListList());
    }

    /**
     * return an array of type values present in basket.
     *
     * @param basket whose {@link TopologyEntityDTO.CommoditiesBoughtFromProvider}'s are converted to type integers
     * @return array of integers representing the types in the basket
     */
    public int[] getCommodityBoughtTypeArray(final TopologyEntityDTO.CommoditiesBoughtFromProvider basket) {
        return basket.getCommodityBoughtList().stream()
                .mapToInt(commBought -> toMarketCommodityId(commBought.getCommodityType()))
                .sorted().toArray();
    }

    /**
     * return an array of type values present in basket.
     *
     * @param basket whose list of {@link TopologyDTO.CommoditySoldDTO}'s are converted to type integers
     * @return array of integers representing the types in the basket
     */
    public int[] getCommoditySoldTypeArray(final List<TopologyDTO.CommoditySoldDTO> basket) {
        return basket.stream().mapToInt(commSold -> toMarketCommodityId(commSold.getCommodityType()))
                .sorted().toArray();
    }

    /**
     * Uses a {@link NumericIDAllocator} to construct an integer type to
     * each unique combination of numeric commodity type + string key.
     * @param commType a commodity description that contains the numeric type and the key
     * @return and integer identifying the type
     */
    @VisibleForTesting
    int toMarketCommodityId(@Nonnull final TopologyDTO.CommodityType commType) {
        return commodityTypeAllocator.allocate(commodityTypeToString(commType));
    }

    /**
     * Concatenates the type and the key of the {@link TopologyDTO.CommodityType}.
     *
     * @param commType the {@link TopologyDTO.CommodityType} for which string conversion is desired
     * @return string conversion of {@link TopologyDTO.CommodityType}
     */
    @Nonnull
    private String commodityTypeToString(@Nonnull final TopologyDTO.CommodityType commType) {
        int type = commType.getType();
        return type + (commType.hasKey() ?
                "|" + commType.getKey()
                : "");
    }
}
