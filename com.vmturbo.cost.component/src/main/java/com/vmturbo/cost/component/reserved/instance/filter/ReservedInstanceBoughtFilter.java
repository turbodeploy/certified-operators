package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

    // The cloud scopes. Map keyed by EntityType to List of OIDs for each EntityType in scope.
    private Map<EntityType, Set<Long>> cloudScopesTuple;

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
        super(scopeIds, scopeEntityType);
        this.cloudScopesTuple = cloudScopesTuple;
        this.joinWithSpecTable = joinWithSpecTable;
        // Note that cloudScopesTuple should be set for Group scopes, and hence associated
        // Regions, AZ's and BA's should be set.
        if (!scopeIds.isEmpty()) {
            // when REGION is involved, we need to join with the reserved_instance_spec table.
            if (scopeEntityType == EntityType.REGION_VALUE) {
                this.joinWithSpecTable = true;
            }
            this.conditions = generateConditions(scopeIds, scopeEntityType);
        } else {
            // There could be a combination of REGION and BILLING ACCOUNT for e.g.
            // and we would need to join with the spec table.
            if (cloudScopesTuple.size() > 1) {
                this.joinWithSpecTable = true;
            }
            this.conditions = generateConditions(this.cloudScopesTuple);
        }
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
     * @param scopeIds scope ids scope OIDs to filter by.  REGION/BA/AZ or other in the fiture.
     * @param scopeEntityType The scope(s) entity type.
     * @return a list of {@link Condition}.
     */
    @Override
    protected List<Condition> generateConditions(@Nonnull final Set<Long> scopeIds,
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
                conditions.add(Tables.RESERVED_INSTANCE_BOUGHT.BUSINESS_ACCOUNT_ID.in(scopeIds));
                break;
            // Mixed scope of optimizable entities is handled by generateConditions(cloudScopesTuple).
            default:
                break;
        }
        return conditions;
    }

    /**
     * Generate a list of {@link Condition} based on different fields.
     *
     * @param cloudScopesTuple Cloud scopes Tuple of (Regions/AZ's/BA's/..) for the topology.
     * @return a list of {@link Condition}.
     */
    protected List<Condition> generateConditions(
                                         @Nonnull final Map<EntityType, Set<Long>> cloudScopesTuple) {
        final List<Condition> conditions = new ArrayList<>();
        if (cloudScopesTuple.isEmpty()) {
            return conditions;
        }
        // TODO: Since there is business specific logic, a similar method may be needed for Azure
        // with Azure specific entity types or this method extended.
        Set<Long> entityRegionOids =  cloudScopesTuple.get(EntityType.REGION_VALUE);
        Set<Long> entityAzOids = cloudScopesTuple.get(EntityType.AVAILABILITY_ZONE_VALUE);
        boolean regionAndAzExist = entityRegionOids != null && entityAzOids != null;
        Set<Long> entityBfOids = cloudScopesTuple.get(EntityType.BUSINESS_ACCOUNT_VALUE);
        if (regionAndAzExist) {
            Condition conditionRegion = Tables.RESERVED_INSTANCE_SPEC.REGION_ID.in(entityRegionOids);
            Condition conditionAz = Tables.RESERVED_INSTANCE_BOUGHT.AVAILABILITY_ZONE_ID.in(entityAzOids);
            conditions.add(conditionRegion.or(conditionAz));
            if (entityBfOids != null) {
                conditions.add(Tables.RESERVED_INSTANCE_BOUGHT.BUSINESS_ACCOUNT_ID.in(entityBfOids));
            }
        } else {
            for (Map.Entry<EntityType, Set<Long>> entry : cloudScopesTuple.entrySet()) {
                Set<Long> entityOids = entry.getValue();
                switch (entry.getKey().getNumber()) {
                    case EntityType.REGION_VALUE:
                        conditions.add(Tables.RESERVED_INSTANCE_SPEC.REGION_ID.in(entityOids));
                        break;
                    case EntityType.AVAILABILITY_ZONE_VALUE:
                        conditions.add(Tables.RESERVED_INSTANCE_BOUGHT.AVAILABILITY_ZONE_ID.in(entityOids));
                        break;
                    case EntityType.BUSINESS_ACCOUNT_VALUE:
                        conditions.add(Tables.RESERVED_INSTANCE_BOUGHT.BUSINESS_ACCOUNT_ID.in(entityOids));
                        break;
                    default:
                        break;
                }
            }
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
        private int scopeEntityType = EntityType.UNKNOWN_VALUE;
        // Cloud scopes Tuple of (Regions/AZ's/BA's/..)
        private Map<EntityType, Set<Long>> cloudScopesTuple = new HashMap<>();
        // Needs to set to true, if any filter needs to get from reserved instance spec table.
        private boolean joinWithSpecTable;

        private Builder() {}

        public ReservedInstanceBoughtFilter build() {
            return new ReservedInstanceBoughtFilter(scopeIds, scopeEntityType, cloudScopesTuple,
                                                    joinWithSpecTable);
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
         * Set the plan scopes tuple.
         *
         * @param cloudScopesTuple  The scopes tuple.
         * @return Builder for this class.
         */
        @Nonnull
        public Builder setCloudScopesTuple(@Nonnull final Map<EntityType, Set<Long>>
                                                                cloudScopesTuple) {
            this.cloudScopesTuple.putAll(cloudScopesTuple);
            return this;
        }

        @Nonnull
        public Builder setJoinWithSpecTable(@Nonnull final boolean isJoinWithSpecTable) {
            this.joinWithSpecTable = isJoinWithSpecTable;
            return this;
        }
    }
}
