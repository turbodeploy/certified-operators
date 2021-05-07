package com.vmturbo.extractor.action.commodity;

import java.util.Map;

import javax.annotation.Nullable;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.extractor.schema.json.common.ActionImpactedEntity.ImpactedMetric;
import com.vmturbo.extractor.schema.json.common.CommodityPercentileChange;

/**
 * Data object to hold commodity-related data (including percentile data) for recommended or
 * executed actions.
 */
public class ActionCommodityData {

    /**
     * entity oid -> commodity type -> {@link ImpactedMetric}.
     */
    private final Long2ObjectMap<Object2ObjectOpenHashMap<String, ImpactedMetric>> changesByEntityAndCommodity;

    private final ActionPercentileData percentileData;


    /**
     * Constructor.
     *  @param commodityChanges commodity changes data
     * @param actionPercentileData percentile data
     */
    public ActionCommodityData(Long2ObjectMap<Object2ObjectOpenHashMap<String, ImpactedMetric>> commodityChanges,
            ActionPercentileData actionPercentileData) {
        this.changesByEntityAndCommodity = commodityChanges;
        this.percentileData = actionPercentileData;
    }

    /**
     * Get the percentile change for a certain commodity sold by a given entity.
     *
     * @param id The id of the entity.
     * @param commodityType The commodity type.
     * @return The associated {@link CommodityPercentileChange}, or null if none match.
     */
    @Nullable
    public CommodityPercentileChange getPercentileChange(long id, CommodityType commodityType) {
        return percentileData.getChange(id, commodityType);
    }

    /**
     * Get the metric for a certain commodity sold by a given entity.
     *
     * @param entityId The id of the entity.
     * @return The associated {@link ImpactedMetric}s, arranged by commodity id, or null if none match.
     */
    @Nullable
    public Map<String, ImpactedMetric> getEntityImpact(long entityId) {
        return changesByEntityAndCommodity.get(entityId);
    }
}
