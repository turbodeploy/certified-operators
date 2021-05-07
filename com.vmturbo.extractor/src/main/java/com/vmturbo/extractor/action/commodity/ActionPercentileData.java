package com.vmturbo.extractor.action.commodity;

import java.util.Map;

import javax.annotation.Nullable;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.extractor.schema.json.common.CommodityPercentileChange;

/**
 * Data object to hold percentile-related data for recommended or executed actions.
 */
public class ActionPercentileData {
    private final Long2ObjectMap<Map<CommodityType, CommodityPercentileChange>>
            changesByEntityAndCommodity;

    ActionPercentileData(Long2ObjectMap<Map<CommodityType, CommodityPercentileChange>> changesByEntityAndCommodity) {
        this.changesByEntityAndCommodity = changesByEntityAndCommodity;
    }

    /**
     * Get the percentile change for a certain commodity sold by a given entity.
     *
     * @param entityId The id of the entity.
     * @param commodityType The commodity type.
     * @return The associated {@link CommodityPercentileChange}, or null if none match.
     */
    @Nullable
    public CommodityPercentileChange getChange(long entityId, CommodityType commodityType) {
        Map<CommodityType, CommodityPercentileChange> entityPercentileChanges =
                changesByEntityAndCommodity.get(entityId);
        if (entityPercentileChanges != null) {
            return entityPercentileChanges.get(commodityType);
        } else {
            return null;
        }
    }
}
