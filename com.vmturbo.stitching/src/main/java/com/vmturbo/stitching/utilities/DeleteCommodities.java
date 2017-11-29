package com.vmturbo.stitching.utilities;

import java.util.List;
import java.util.Map;

import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.stitching.StitchingEntity;

/**
 * Utility class for deleting commodities during stitching.
 */
public class DeleteCommodities {

    private DeleteCommodities() {

    }

    /**
     * Remove a provider from the map of providers to commodities bought for a consumer.
     *
     * @param provider The provider to remove.
     * @param consumer The consumer to remove it from.
     */
    public static void deleteCommoditiesBoughtFromProvider(StitchingEntity provider,
                                                           StitchingEntity consumer) {
        final Map<StitchingEntity, List<CommodityDTO.Builder>> consumerBought =
                consumer.getCommoditiesBoughtByProvider();
        consumerBought.remove(provider);

    }
}
