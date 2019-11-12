package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A filter to restrict the {@link ReservedInstanceBought} from the {@link ReservedInstanceBoughtStore}.
 * It provider a easier way to define simple search over reserved instances records in the tables.
 */
public class ReservedInstanceBoughtFilter extends ReservedInstanceBoughtTableFilter {

    public static final ReservedInstanceBoughtFilter SELECT_ALL_FILTER = newBuilder().build();

    /**
     * Constructor for ReservedInstanceBoughtFilter.
     *
     * @param scopeIds The scope(s) IDs.
     * @param scopeEntityType The scope(s) entity type.
     * @param cloudScopesTuple The Cloud Scopes Tuple for the topology scope.
     * @param joinWithSpecTable True if any filter needs to get from reserved instance spec table.
     */
    private ReservedInstanceBoughtFilter(@Nonnull final Set<Long> scopeIds,
                                         final int scopeEntityType,
                                         @Nonnull final Map<EntityType, Set<Long>> cloudScopesTuple,
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
    public static class Builder extends AbstractBuilder<ReservedInstanceBoughtFilter, Builder> {
        private Builder() {}

        @Override
        public ReservedInstanceBoughtFilter build() {
            return new ReservedInstanceBoughtFilter(scopeIds, scopeEntityType, cloudScopesTuple,
                                                    joinWithSpecTable);
        }

        @Override
        Builder getThis() {
            return this;
        }
    }
}
