package com.vmturbo.topology.processor.topology;

import java.io.Serializable;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang.StringUtils;

import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityTypeView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderView;
import com.vmturbo.commons.analysis.InvertedIndexBaseTranslator;
import com.vmturbo.commons.analysis.NumericIDAllocator;
import com.vmturbo.components.common.utils.CommodityTypeAllocatorConstants;
import com.vmturbo.stitching.TopologyEntity;

/**
 * A mapper that implements methods that are used by the InvertedIndex.
 *
 */
public class TopologyInvertedIndexTranslator implements InvertedIndexBaseTranslator<TopologyEntity,
        CommoditiesBoughtFromProviderView, List<CommoditySoldView>>, Serializable {

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
        return getCommoditySoldTypeArray(seller.getTopologyEntityImpl().getCommoditySoldListList());
    }

    /**
     * return an array of type values present in basket.
     *
     * @param basket whose {@link CommoditiesBoughtFromProviderView}'s are converted to type integers
     * @return array of integers representing the types in the basket
     */
    public int[] getCommodityBoughtTypeArray(final CommoditiesBoughtFromProviderView basket) {
        return basket.getCommodityBoughtList().stream()
            // When there is a basket with no accessCommodities, it is possible that it could bring in entities from
            // outside the scope. When we filter out non-accessComms and use just accessCommodities while determining
            // scope, restrict the scope to just entities that are within the specified constraints.
            // In the case of vSAN storages that buy no accessCommodities, we pull in the relevant hosts as providers
            // of the VMs in scope.
            .filter(commBought -> !StringUtils.isEmpty(commBought.getCommodityType().getKey()))
            .mapToInt(commBought -> toMarketCommodityId(commBought.getCommodityType()))
            .sorted().toArray();
    }

    /**
     * return an array of type values present in basket.
     *
     * @param basket whose list of {@link CommoditySoldView}'s are converted to type integers
     * @return array of integers representing the types in the basket
     */
    public int[] getCommoditySoldTypeArray(final List<CommoditySoldView> basket) {
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
    int toMarketCommodityId(@Nonnull final CommodityTypeView commType) {
        return commodityTypeAllocator.allocate(commodityTypeToString(commType), commType.hasKey()
                ? CommodityTypeAllocatorConstants.ACCESS_COMM_TYPE_START_COUNT : 0);
    }

    /**
     * Concatenates the type and the key of the {@link CommodityTypeView}.
     *
     * @param commType the {@link CommodityTypeView} for which string conversion is desired
     * @return string conversion of {@link CommodityTypeView}
     */
    @Nonnull
    private String commodityTypeToString(@Nonnull final CommodityTypeView commType) {
        int type = commType.getType();
        return type + (commType.hasKey() ?
                "|" + commType.getKey()
                : "");
    }
}
