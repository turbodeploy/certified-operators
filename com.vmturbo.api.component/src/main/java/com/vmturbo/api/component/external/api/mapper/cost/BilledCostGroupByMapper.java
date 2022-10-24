package com.vmturbo.api.component.external.api.mapper.cost;

import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.enums.CostGroupBy;
import com.vmturbo.common.protobuf.cost.BilledCost.BilledCostStatsQuery.BilledCostGroupBy;

/**
 * Mapper of {@link CostGroupBy} API enum to {@link BilledCostGroupBy} for cost component queries.
 */
public class BilledCostGroupByMapper {

    private static final Map<CostGroupBy, BilledCostGroupBy> GROUP_BY_ENUM_MAP = ImmutableMap.<CostGroupBy, BilledCostGroupBy>builder()
            .put(CostGroupBy.TAG, BilledCostGroupBy.TAG)
            .put(CostGroupBy.TAG_GROUP, BilledCostGroupBy.TAG_GROUP)
            .put(CostGroupBy.ENTITY, BilledCostGroupBy.ENTITY)
            .put(CostGroupBy.ENTITY_TYPE, BilledCostGroupBy.ENTITY_TYPE)
            .put(CostGroupBy.ACCOUNT, BilledCostGroupBy.ACCOUNT)
            .put(CostGroupBy.REGION, BilledCostGroupBy.REGION)
            .put(CostGroupBy.CLOUD_SERVICE, BilledCostGroupBy.CLOUD_SERVICE)
            .put(CostGroupBy.COST_CATEGORY, BilledCostGroupBy.COST_CATEGORY)
            .put(CostGroupBy.PRICE_MODEL, BilledCostGroupBy.PRICE_MODEL)
            .put(CostGroupBy.SERVICE_PROVIDER, BilledCostGroupBy.SERVICE_PROVIDER)
            .build();

    private BilledCostGroupByMapper() {}

    /**
     * Convert API {@link CostGroupBy} enum to {@link BilledCostGroupBy} enum recognized by Cost component.
     *
     * @param costGroupBy {@code CostGroupBy} to convert (can be {@code null}).
     * @return Converted {@code GroupByType}.
     */
    @Nonnull
    public static BilledCostGroupBy toGroupByType(@Nonnull final CostGroupBy costGroupBy) {

        Preconditions.checkNotNull(costGroupBy, "CostGroupBy cannot be null");

        final BilledCostGroupBy result = GROUP_BY_ENUM_MAP.get(costGroupBy);
        if (result == null) {
            throw new IllegalArgumentException("Unsupported CostGroupBy: " + costGroupBy);
        }
        return result;
    }
}
