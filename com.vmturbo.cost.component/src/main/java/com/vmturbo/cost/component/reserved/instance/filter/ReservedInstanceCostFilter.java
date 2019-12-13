package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * Filter used to extract RI costs from the underlying tables.
 */
public class ReservedInstanceCostFilter extends ReservedInstanceBoughtTableFilter {

    /**
     * Constructor for ReservedInstanceCostFilter.
     *
     * @param scopeIds The scope(s) IDs.
     * @param scopeEntityType The scope(s) entity type.
     * @param cloudScopesTuple The Cloud Scopes Tuple for the topology scope.
     * @param joinWithSpecTable  True if any filter needs to get from reserved instance spec table.
     */
    private ReservedInstanceCostFilter(Set<Long> scopeIds,
                    final Optional<Integer> scopeEntityType,
                    @Nonnull final Map<CommonDTO.EntityDTO.EntityType, Set<Long>> cloudScopesTuple,
                    final boolean joinWithSpecTable) {
        super(scopeIds, scopeEntityType, cloudScopesTuple, joinWithSpecTable);
    }

    /**
     * Create a builder used to construct a filter.
     *
     * @return The builder object.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Create a builder used to construct a filter.
     *
     * @return The builder object.
     */
    public static class Builder extends AbstractBuilder<ReservedInstanceCostFilter, Builder> {
        private Builder() {

        }

        @Override
        public ReservedInstanceCostFilter build() {
            return new ReservedInstanceCostFilter(scopeIds, scopeEntityType, cloudScopesTuple,
                            joinWithSpecTable);
        }

        @Override
        Builder getThis() {
            return this;
        }
    }
}
