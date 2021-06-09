package com.vmturbo.extractor.export;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.extractor.schema.enums.CostCategory;
import com.vmturbo.extractor.schema.enums.CostSource;
import com.vmturbo.extractor.schema.json.export.EntityCost;
import com.vmturbo.extractor.topology.fetcher.BottomUpCostFetcherFactory.BottomUpCostData;
import com.vmturbo.extractor.topology.fetcher.BottomUpCostFetcherFactory.BottomUpCostData.BottomUpCostDataPoint;

/**
 * Extracts bottom up cost for an entity in a topology.
 */
public class BottomUpCostExtractor {

    private final BottomUpCostData bottomUpCostData;

    /**
     * Create a new extractor. There is a new one created every time we process a topology.
     *
     * @param bottomUpCostData The {@link BottomUpCostData}.
     */
    public BottomUpCostExtractor(@Nonnull final BottomUpCostData bottomUpCostData) {
        this.bottomUpCostData = bottomUpCostData;
    }

    /**
     * Get the bottom up cost associated with an entity.
     *
     * @param entityId The entity oid
     * @return {@link EntityCost} cost for given entity or null if not available
     */
    @Nullable
    public EntityCost getCost(final long entityId) {
        final List<BottomUpCostDataPoint> entityCostDataPoints =
                bottomUpCostData.getEntityCostDataPoints(entityId);
        if (entityCostDataPoints.isEmpty()) {
            return null;
        }

        final EntityCost entityCost = new EntityCost();
        entityCost.setUnit(StringConstants.DOLLARS_PER_HOUR);
        entityCostDataPoints.forEach(costDataPoint -> {
            if (costDataPoint.getSource() == CostSource.TOTAL) {
                if (costDataPoint.getCategory() == CostCategory.TOTAL) {
                    entityCost.setTotal(costDataPoint.getCost());
                } else {
                    entityCost.setCategoryCost(costDataPoint.getCategory().getLiteral(), costDataPoint.getCost());
                }
            } else {
                entityCost.setSourceCost(costDataPoint.getCategory().getLiteral(),
                        costDataPoint.getSource().getLiteral(), costDataPoint.getCost());
            }
        });
        return entityCost;
    }
}
