package com.vmturbo.api.component.external.api.mapper;

import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.enums.CostGroupBy;
import com.vmturbo.common.protobuf.cost.Cost.GetCloudBilledStatsRequest.GroupByType;

/**
 * Mapper for {@link CostGroupBy} enum.
 */
public final class CostGroupByMapper {
    private static final Map<CostGroupBy, GroupByType> GROUP_BY_ENUM_MAP = ImmutableMap.of(
            CostGroupBy.TAG, GroupByType.TAG);

    private CostGroupByMapper() {
    }

    /**
     * Convert API {@link CostGroupBy} enum to {@link GroupByType} enum recognized by Cost component.
     *
     * @param costGroupBy {@code CostGroupBy} to convert (can be {@code null}).
     * @return Converted {@code GroupByType} or {@code null} if argument is {@code null}.
     */
    @Nullable
    public static GroupByType toGroupByType(@Nullable final CostGroupBy costGroupBy) {
        if (costGroupBy == null) {
            return null;
        }
        final GroupByType result = GROUP_BY_ENUM_MAP.get(costGroupBy);
        if (result == null) {
            throw new IllegalArgumentException("Unsupported CostGroupBy: " + costGroupBy);
        }
        return result;
    }
}
