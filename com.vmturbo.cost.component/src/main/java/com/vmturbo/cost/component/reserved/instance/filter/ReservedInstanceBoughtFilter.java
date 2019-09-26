package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jooq.Condition;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A filter to restrict the {@link ReservedInstanceBought} from the {@link ReservedInstanceBoughtStore}.
 * It provider a easier way to define simple search over reserved instances records in the tables.
 */
public class ReservedInstanceBoughtFilter extends ReservedInstanceFilter {

    public static final ReservedInstanceBoughtFilter SELECT_ALL_FILTER = newBuilder().build();

    private final List<Condition> conditions;

    // Needs to set to true, if any filter needs to get from reserved instance spec table.
    private boolean joinWithSpecTable;

    private ReservedInstanceBoughtFilter(@Nonnull final Set<Long> scopeIds,
                                         @Nonnull final int scopeEntityType,
                                         final boolean joinWithSpecTable) {
        super(scopeIds, scopeEntityType);
        if (scopeEntityType == EntityType.REGION_VALUE) {
            this.joinWithSpecTable = true;
        } else {
            this.joinWithSpecTable = joinWithSpecTable;
        }
        this.conditions = generateConditions(scopeIds, scopeEntityType);
     }

    /**
     * Get the array of {@link Condition} representing the conditions of this filter.
     *
     * @return The array of {@link Condition} representing the filter.
     */
    public Condition[] getConditions() {
        return this.conditions.toArray(new Condition[conditions.size()]);
    }

    public boolean isJoinWithSpecTable() {
        return this.joinWithSpecTable;
    }

    @Override
    protected List<Condition> generateConditions(@Nonnull final Set<Long> scopeIds,
                                                 @Nonnull final int scopeEntityType) {
        final List<Condition> conditions = new ArrayList<>();
        if (scopeIds.isEmpty()) {
            return conditions;
        }
        switch (scopeEntityType) {
            case EntityType.REGION_VALUE:
                conditions.add(Tables.RESERVED_INSTANCE_SPEC.REGION_ID.in(scopeIds));
                break;
            case EntityType.AVAILABILITY_ZONE_VALUE:
                conditions.add(Tables.RESERVED_INSTANCE_BOUGHT.AVAILABILITY_ZONE_ID.in(scopeIds));
                break;
            case EntityType.BUSINESS_ACCOUNT_VALUE:
                conditions.add(Tables.RESERVED_INSTANCE_BOUGHT.BUSINESS_ACCOUNT_ID.in(scopeIds));
                break;
            default:
                break;
        }
        return conditions;
    }

    /**
     * Create a builder used to construct a filter.
     *
     * @return The builder object.
     */
    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        // The set of scope oids.
        private Set<Long> scopeIds = new HashSet<>();
        // The scope's entity type.
        private int scopeEntityType = EntityType.UNKNOWN_VALUE;
        // Needs to set to true, if any filter needs to get from reserved instance spec table.
        private boolean joinWithSpecTable;

        private Builder() {}

        public ReservedInstanceBoughtFilter build() {
            return new ReservedInstanceBoughtFilter(scopeIds, scopeEntityType, joinWithSpecTable);
        }

        /**
         * Add all scope ids that are part of the plan sope.
         *
         * @param ids The scope oids that represent the filtering conditions.
         * @return Builder for this class.
         */
        @Nonnull
        public Builder addAllScopeId(final List<Long> ids) {
            this.scopeIds.addAll(ids);
            return this;
        }

        /**
         * Set the plan scopes' entity type.
         *
         * @param entityType  The scope's entity type as defined in
         *          @see com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType
         * @return Builder for this class.
         */
        @Nonnull
        public Builder setScopeEntityType(final int entityType) {
            this.scopeEntityType = entityType;
            return this;
        }

        @Nonnull
        public Builder setJoinWithSpecTable(final boolean isJoinWithSpecTable) {
            this.joinWithSpecTable = isJoinWithSpecTable;
            return this;
        }
    }
}
