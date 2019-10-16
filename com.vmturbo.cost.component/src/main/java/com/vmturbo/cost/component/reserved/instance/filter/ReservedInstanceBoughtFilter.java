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

    /**
     * Constructor for ReservedInstanceBoughtFilter.
     *
     * @param scopeIds The scope(s) Ids.
     * @param billingAccountIds The relevant business account OIDs, for the one or more billing families in scope.
     * @param scopeEntityType The scope(s) entity type.
     * @param joinWithSpecTable True if any filter needs to get from reserved instance spec table.
     * TODO: OM-50904 GetReservedInstanceBoughtByFilter request will take in the existing TopoologyInfo instead of
     * scope_seed_oids and scope_entity_type and call repositoryClient.getOcpScopesTuple() to figure out the
     * Regions/AZ's/BA's in any scoped or unscoped topology, which will help with mixed Group scopes, and simplify
     * filter logic further.
     */
    private ReservedInstanceBoughtFilter( final Set<Long> scopeIds,
                                         @Nonnull final Set<Long> billingAccountIds,
                                         final int scopeEntityType,
                                         final boolean joinWithSpecTable) {
        super(scopeIds, billingAccountIds, scopeEntityType);
        if (scopeEntityType == EntityType.REGION_VALUE) {
            this.joinWithSpecTable = true;
        } else {
            this.joinWithSpecTable = joinWithSpecTable;
        }
        this.conditions = generateConditions(scopeIds, billingAccountIds, scopeEntityType);
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

    /**
     * Generate a list of {@link Condition} based on different fields.
     *
     * <p>Note that the where condition is only containing one filter clause at present.
     * To have multiple filters, there would need to be AND's in the where clause.
     *
     * @param scopeIds scope ids scope OIDs to filter by.  REGION/BA/AZ or other in the fiture.
     * @param billingAccountIds The relevant business account OIDs, for the one or
     * more billing families in scope.
     * @param scopeEntityType The scope(s) entity type.
     * @return a list of {@link Condition}.
     */
    @Override
    protected List<Condition> generateConditions(@Nonnull final Set<Long> scopeIds,
                                                 @Nonnull final Set<Long> billingAccountIds,
                                                 int scopeEntityType) {
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
                conditions.add(Tables.RESERVED_INSTANCE_BOUGHT.BUSINESS_ACCOUNT_ID.in(billingAccountIds));
                break;
            // TODO:  Mixed scope of optimizable entities.
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
        // The relevant business account OIDs, for the one or more billing families in scope.
        private Set<Long> billingAccountIds = new HashSet<>();
        // Needs to set to true, if any filter needs to get from reserved instance spec table.
        private boolean joinWithSpecTable;

        private Builder() {}

        public ReservedInstanceBoughtFilter build() {
            return new ReservedInstanceBoughtFilter(scopeIds, billingAccountIds,
                                                    scopeEntityType, joinWithSpecTable);
        }

        /**
         * Add all scope ids that are part of the plan sope.
         *
         * @param ids The relevant business account Ids, in one or more billing families in scope.
         * @return Builder for this class.
         */
        @Nonnull
        public Builder addAllScopeId(@Nonnull final List<Long> ids) {
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
        public Builder setScopeEntityType(@Nonnull final int entityType) {
            this.scopeEntityType = entityType;
            return this;
        }

        /**
         * Add all billing account OIDs that are relevant.
         *
         * <p>In OCP plans, this would be all accounts/subscriptions in the billing family.
         * In real-time it could be all accounts/subscriptions in the billing family, for
         * billing family scope and a single sub-account for account scope.
         *
         * @param ids The relevant business account OIDs, for the one or more billing families in scope.
         * @return Builder for this class.
         */
        @Nonnull
        public Builder addAllBillingAccountId(@Nonnull final List<Long> ids) {
            this.billingAccountIds.addAll(ids);
            return this;
        }

        @Nonnull
        public Builder setJoinWithSpecTable(@Nonnull final boolean isJoinWithSpecTable) {
            this.joinWithSpecTable = isJoinWithSpecTable;
            return this;
        }
    }
}
