package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.jooq.Condition;

import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * Abstract Class serving as a parent class for Filters that operate on the ReservedInstanceBought Table.
 */
public abstract class ReservedInstanceBoughtTableFilter extends ReservedInstanceFilter {

    //List of conditions.
    private final List<Condition> conditions;

    // Needs to set to true, if any filter needs to get from reserved instance spec table.
    protected boolean joinWithSpecTable;

    // The cloud scopes. Map keyed by EntityType to List of OIDs for each EntityType in scope.
    // For example REGION --> (Region1Oid, Region2Oid)
    // VIRTUAL_MACHINE --> (VM1Oid, VM2Oid, VM3Oid)
    // For real-time and global plans it stays empty as we don't need to filter by these types (
    // if we were to need this in the future,  we could fetch all entities and populate the map).
    // For a scoped plan, it will contain a mapping of entities in scope.
    private Map<CommonDTO.EntityDTO.EntityType, Set<Long>> cloudScopesTuple;

    /**
     * Constructor that takes scopeId(s) and scopeEntityType as arguments.
     *
     * @param scopeIds The scope(s) ids.  Region/BusinessAccount/AvalilabilityZone etc.
     * @param scopeEntityType  The scopes' enity type.  In general, this parameter is homegeneous and
     * the scopeEntityType determines the type of entities contained in it.  Exceptions are mixed groups.
     * @param cloudScopesTuple The Cloud Scopes Tuple for the topology scope.
     * @param joinWithSpecTable True if any filter needs to get from reserved instance spec table.
     */
    public ReservedInstanceBoughtTableFilter(@Nonnull Set<Long> scopeIds, int scopeEntityType,
                    @Nonnull final Map<CommonDTO.EntityDTO.EntityType, Set<Long>> cloudScopesTuple,
                    final boolean joinWithSpecTable) {
        super(scopeIds, scopeEntityType);
        this.cloudScopesTuple = cloudScopesTuple;
        this.joinWithSpecTable = joinWithSpecTable;
        // Note that cloudScopesTuple should be set for Group scopes, and hence associated
        // Regions, AZ's and BA's should be set.
        if (!scopeIds.isEmpty()) {
            // when REGION is involved, we need to join with the reserved_instance_spec table.
            if (scopeEntityType == CommonDTO.EntityDTO.EntityType.REGION_VALUE) {
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
                    final int scopeEntityType) {
        final List<Condition> conditions = new ArrayList<>();
        if (scopeIds.isEmpty()) {
            return conditions;
        }
        switch (scopeEntityType) {
            case CommonDTO.EntityDTO.EntityType.REGION_VALUE:
                conditions.add(Tables.RESERVED_INSTANCE_SPEC.REGION_ID.in(scopeIds));
                break;
            case CommonDTO.EntityDTO.EntityType.AVAILABILITY_ZONE_VALUE:
                conditions.add(Tables.RESERVED_INSTANCE_BOUGHT.AVAILABILITY_ZONE_ID.in(scopeIds));
                break;
            case CommonDTO.EntityDTO.EntityType.BUSINESS_ACCOUNT_VALUE:
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
                    @Nonnull final Map<CommonDTO.EntityDTO.EntityType, Set<Long>> cloudScopesTuple) {
        final List<Condition> conditions = new ArrayList<>();
        if (cloudScopesTuple.isEmpty()) {
            return conditions;
        }
        // TODO: Since there is business specific logic, a similar method may be needed for Azure
        // with Azure specific entity types or this method extended.
        Set<Long> entityRegionOids =  cloudScopesTuple.get(CommonDTO.EntityDTO.EntityType.REGION);
        Set<Long> entityAzOids = cloudScopesTuple.get(CommonDTO.EntityDTO.EntityType.AVAILABILITY_ZONE);
        boolean regionAndAzExist = entityRegionOids != null && entityAzOids != null;
        Set<Long> entityBfOids = cloudScopesTuple.get(CommonDTO.EntityDTO.EntityType.BUSINESS_ACCOUNT);
        if (regionAndAzExist) {
            Condition conditionRegion = Tables.RESERVED_INSTANCE_SPEC.REGION_ID.in(entityRegionOids);
            Condition conditionAz = Tables.RESERVED_INSTANCE_BOUGHT.AVAILABILITY_ZONE_ID.in(entityAzOids);
            conditions.add(conditionRegion.or(conditionAz));
            if (entityBfOids != null) {
                conditions.add(Tables.RESERVED_INSTANCE_BOUGHT.BUSINESS_ACCOUNT_ID.in(entityBfOids));
            }
        } else {
            for (Map.Entry<CommonDTO.EntityDTO.EntityType, Set<Long>> entry : cloudScopesTuple.entrySet()) {
                Set<Long> entityOids = entry.getValue();
                conditions.addAll(generateConditions(entityOids, entry.getKey().getNumber()));
            }
        }
        return conditions;
    }

    /**
     * Abstract Class serving as parent class for Builders to build the required Filters.
     *
     * @param <T> Child Filter Classes that extend the ReservedInstanceBoughtTableFilter class.
     * @param <U> Child Bulilder Classes that extend the AbstractBuilder class.
     */
    abstract static class AbstractBuilder<T extends ReservedInstanceBoughtTableFilter, U extends AbstractBuilder<T, U>> {
        // The set of scope oids.
        final Set<Long> scopeIds = new HashSet<>();
        int scopeEntityType = CommonDTO.EntityDTO.EntityType.UNKNOWN_VALUE;
        // Cloud scopes Tuple of (Regions/AZ's/BA's/..)
        final Map<CommonDTO.EntityDTO.EntityType, Set<Long>> cloudScopesTuple = new HashMap<>();
        // Needs to set to true, if any filter needs to get from reserved instance spec table.
        boolean joinWithSpecTable;

        AbstractBuilder() {}

        public abstract T build();

        abstract U getThis();

        /**
         * Add all scope ids that are part of the plan sope.
         *
         * @param ids The relevant business account Ids, in one or more billing families in scope.
         * @return Builder for this class.
         */
        @Nonnull
        public U addAllScopeId(@Nonnull final List<Long> ids) {
            this.scopeIds.addAll(ids);
            return getThis();
        }

        /**
         * Set the plan scopes' entity type.
         *
         * @param entityType  The scope's entity type as defined in
         *          @see com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType
         * @return Builder for this class.
         */
        @Nonnull
        public U setScopeEntityType(@Nonnull final int entityType) {
            this.scopeEntityType = entityType;
            return getThis();
        }

        /**
         * Set the plan scopes tuple.
         *
         * @param cloudScopesTuple  The scopes tuple.
         * @return Builder for this class.
         */
        @Nonnull
        public U setCloudScopesTuple(@Nonnull final Map<CommonDTO.EntityDTO.EntityType, Set<Long>>
                        cloudScopesTuple) {
            this.cloudScopesTuple.putAll(cloudScopesTuple);
            return getThis();
        }

        /**
         * Set the flag to indicate if an SQL join with the Reserved Instance Specification Table is needed.
         *
         * @param isJoinWithSpecTable boolean flag indicating if we need to perform a SQL join with
         *                            Reserved Instance Specification Table.
         * @return Builder for this class.
         */
        @Nonnull
        public U setJoinWithSpecTable(@Nonnull final boolean isJoinWithSpecTable) {
            this.joinWithSpecTable = isJoinWithSpecTable;
            return getThis();
        }
    }
}

