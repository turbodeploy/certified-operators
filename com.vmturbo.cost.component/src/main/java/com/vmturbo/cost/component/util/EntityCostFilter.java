package com.vmturbo.cost.component.util;

import static com.vmturbo.cost.component.db.Tables.ENTITY_COST;
import static com.vmturbo.cost.component.db.Tables.PLAN_ENTITY_COST;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Table;

import com.vmturbo.common.protobuf.cost.Cost.CloudCostStatsQuery.GroupBy;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostCategoryFilter;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost.ComponentCost.CostSourceLinkDTO;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.cost.component.db.Tables;

/**
 * A filter to restrict the entity cost records from the
 * {@link com.vmturbo.cost.component.entity.cost.EntityCostStore}.
 * It provider a easier way to define simple search over entity cost records
 * in the tables.
 */
public class EntityCostFilter extends CostFilter {

    private static final Logger logger = LogManager.getLogger();

    private static final String CREATED_TIME = ENTITY_COST.CREATED_TIME.getName();

    private final boolean excludeCostSources;
    private final Set<CostSource> costSources;
    private final CostCategoryFilter costCategoryFilter;
    private final Set<Long> accountIds;
    private final Set<Long> availabilityZoneIds;
    private final Set<Long> regionIds;

    private final List<Condition> conditions;

    @Nullable
    private final CostGroupBy costGroupBy;

    /**
     * Preserve the group by enums received in the RPC request.
     */
    private final List<GroupBy> requestedGroupByEnums;

    private final boolean totalValuesRequested;

    EntityCostFilter(@Nullable final Set<Long> entityFilters,
                     @Nullable final Set<Integer> entityTypeFilters,
                     @Nullable final Long startDateMillis,
                     @Nullable final Long endDateMillis,
                     @Nullable final TimeFrame timeFrame,
                     @Nonnull final Set<String> groupByFields,
                     final boolean excludeCostSources,
                     @Nullable final Set<CostSource> costSources,
                     @Nullable final CostCategoryFilter costCategoryFilter,
                     @Nullable final Set<Long> accountIds,
                     @Nullable final Set<Long> availabilityZoneIds,
                     @Nullable final Set<Long> regionIds,
                     final boolean latestTimeStampRequested,
                     @Nullable final Long topologyContextId,
                     final long realtimeTopologyContextId,
                     final List<GroupBy> requestedGroupByEnums,
                     final boolean totalValuesRequested) {
        super(entityFilters, entityTypeFilters, startDateMillis, endDateMillis, timeFrame,
            CREATED_TIME, latestTimeStampRequested, topologyContextId, realtimeTopologyContextId);
        this.excludeCostSources = excludeCostSources;
        this.costSources = costSources;
        this.costCategoryFilter = costCategoryFilter;
        this.accountIds = accountIds;
        this.availabilityZoneIds = availabilityZoneIds;
        this.regionIds = regionIds;
        this.costGroupBy = createGroupByFieldString(groupByFields);
        this.conditions = generateConditions();
        this.requestedGroupByEnums = requestedGroupByEnums;
        this.totalValuesRequested = totalValuesRequested;
    }

    /**
     * Creates a mew builder initialized with the filter fields.
     * @return a new EntityCostFilterBuilder
     */
    @Nonnull
    public  EntityCostFilterBuilder toNewBuilder() {
        EntityCostFilterBuilder builder =
                new EntityCostFilterBuilder(timeFrame, realtimeTopologyContextId);
        if (costSources != null) {
            builder.costSources(excludeCostSources, costSources);
        }
        if (costCategoryFilter != null) {
            builder.costCategoryFilter(costCategoryFilter);
        }
        if (entityTypeFilters != null) {
            builder.entityTypes(entityTypeFilters);
        }
        if (costGroupBy != null && costGroupBy.getReceivedGroupByFields() != null) {
            builder.groupByFields(costGroupBy.getReceivedGroupByFields());
        }
        if (requestedGroupByEnums != null) {
            builder.requestedGroupBy = requestedGroupByEnums;
        }
        if (regionIds != null) {
            builder.regionIds(regionIds);
        }
        if (accountIds != null) {
            builder.accountIds(accountIds);
        }
        if (availabilityZoneIds != null) {
            builder.availabilityZoneIds(availabilityZoneIds);
        }
        if (entityFilters != null) {
            builder.entityIds(entityFilters);
        }
        if (startDateMillis != null) {
            builder.startDateMillis = startDateMillis;
        }
        if (endDateMillis != null) {
            builder.endDateMillis = endDateMillis;
        }
        builder.latestTimestampRequested(latestTimeStampRequested);
        builder.totalValuesRequested(totalValuesRequested);

        return builder;
    }

    /**
     * Preserve the group by enums received in the RPC request.
     * @return the original group by enums
     */
    public List<GroupBy> getRequestedGroupBy() {
        return requestedGroupByEnums;
    }

    @Nullable
    private CostGroupBy createGroupByFieldString(@Nonnull Set<String> items ) {
        final Set<String> listOfFields = Sets.newHashSet(items);
        listOfFields.add(getTable().field(CREATED_TIME).getName());
        return items.isEmpty()
            ? null
            : new CostGroupBy(listOfFields.stream()
                                  .map(columnName -> columnName.toLowerCase(Locale.getDefault()))
                                  .collect(Collectors.toSet()), timeFrame, topologyContextId,
                    realtimeTopologyContextId);
    }

    /**
     * Generate a list of {@link Condition} based on different fields.
     *
     * @return a list of {@link Condition}.
     */
    @Override
    public List<Condition> generateConditions() {
        final List<Condition> conditions = new ArrayList<>();
        final Table<?> table = getTable();

        if (startDateMillis != null) {
            LocalDateTime localStart = LocalDateTime.ofInstant(Instant.ofEpochMilli(this.startDateMillis),
                ZoneId.from(ZoneOffset.UTC));
            LocalDateTime localEnd =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(this.endDateMillis),
                ZoneId.from(ZoneOffset.UTC));
            conditions.add(((Field<LocalDateTime>)table.field(snapshotTime))
                    .between(localStart, localEnd));
        }

        if (entityTypeFilters != null && !entityTypeFilters.isEmpty()) {
            conditions.add(table.field(ENTITY_COST.ASSOCIATED_ENTITY_TYPE.getName()).in(entityTypeFilters));
        }

        if (entityFilters != null && !entityFilters.isEmpty()) {
            conditions.add(table.field(ENTITY_COST.ASSOCIATED_ENTITY_ID.getName()).in(entityFilters));
        }

        if (costCategoryFilter != null) {

            Set<Integer> costCategoryValues = costCategoryFilter.getCostCategoryList()
                    .stream()
                    .map(CostCategory::getNumber)
                    .collect(ImmutableSet.toImmutableSet());

            if (!costCategoryValues.isEmpty()) {
                if (costCategoryFilter.getExclusionFilter()) {
                    conditions.add(table.field(ENTITY_COST.COST_TYPE.getName())
                                    .notIn(costCategoryValues));
                } else {
                    conditions.add(table.field(ENTITY_COST.COST_TYPE.getName())
                                    .in(costCategoryValues));
                }
            }
        }

        if (costSources != null && !costSources.isEmpty()) {
            final Set<Integer> costSourceValues = costSources.stream()
                    .map(CostSource::getNumber)
                    .collect(ImmutableSet.toImmutableSet());
            if (getTable() == ENTITY_COST || getTable() == PLAN_ENTITY_COST) {
                if (excludeCostSources) {
                    conditions.add(table.field(ENTITY_COST.COST_SOURCE.getName()).notIn(costSourceValues));
                } else {
                    conditions.add(table.field(ENTITY_COST.COST_SOURCE.getName()).in(costSourceValues));
                }
            } else {
                logger.warn("Cost source filter has been set on a query on a table other than the"
                        + " latest. It will be ignored.");
            }
        }

        if (accountIds != null) {
            conditions.add(table.field(ENTITY_COST.ACCOUNT_ID.getName()).in(accountIds));
        }

        if (availabilityZoneIds != null) {
            conditions.add(table.field(ENTITY_COST.AVAILABILITY_ZONE_ID.getName()).in(availabilityZoneIds));
        }

        if (regionIds != null) {
            conditions.add(table.field(ENTITY_COST.REGION_ID.getName()).in(regionIds));
        }

        if (hasPlanTopologyContextId()) {
            conditions.add(PLAN_ENTITY_COST.PLAN_ID.eq(topologyContextId));
        }

        return conditions;
    }

    @Override
    public Condition[] getConditions() {
        return this.conditions.toArray(new Condition[conditions.size()]);
    }

    @Override
    public Table<?> getTable() {
        if (this.hasPlanTopologyContextId()) {
            return Tables.PLAN_ENTITY_COST;
        } else if (this.timeFrame == null || this.timeFrame.equals(TimeFrame.LATEST)) {
            return Tables.ENTITY_COST;
        } else if (this.timeFrame.equals(TimeFrame.HOUR)) {
            return Tables.ENTITY_COST_BY_HOUR;
        } else if (this.timeFrame.equals(TimeFrame.DAY)) {
            return Tables.ENTITY_COST_BY_DAY;
        } else {
            return Tables.ENTITY_COST_BY_MONTH;
        }
    }

    /**
     * Should exclude the cost sources specified in this filter.
     * @return true if the cost sources will be excluded and false if they are included.
     */
    public boolean isCostSourcesToBeExcluded() {
        return excludeCostSources;
    }

    public Optional<Set<CostSource>> getCostSources() {
        return Optional.ofNullable(costSources);
    }

    public Optional<CostCategoryFilter> getCostCategoryFilter() {
        return Optional.ofNullable(costCategoryFilter);
    }

    /**
     * Determines whether a {@link EntityCost.ComponentCost} is within scope of this filter.
     *
     * @param componentCost The {@link EntityCost.ComponentCost} to test
     * @return True, if this filter includes {@code componentCost}, false otherwise
     */
    public boolean filterComponentCost(EntityCost.ComponentCost componentCost) {
        return filterComponentCostByCostSource(componentCost)
                && filterComponentCostByCostCategory(componentCost);
    }

    public Optional<Set<Long>> getAccountIds() {
        return Optional.ofNullable(accountIds);
    }

    public Optional<Set<Long>> getAvailabilityZoneIds() {
        return Optional.ofNullable(availabilityZoneIds);
    }

    public Optional<Set<Long>> getRegionIds() {
        return Optional.ofNullable(regionIds);
    }

    /**
     * Returns true if none of the scopes fields are set.
     *
     * @return true if none of the scopes fields are set.
     */
    public boolean isGlobalScope() {
        return !getEntityTypeFilters().isPresent()
                && !getEntityFilters().isPresent()
                && !getRegionIds().isPresent()
                && !getAccountIds().isPresent()
                && !getAvailabilityZoneIds().isPresent()
                && !getCostCategoryFilter().isPresent()
                && !getCostSources().isPresent();
    }

    /**
     * Gets the flag indicating to return values and their units depending on the requested time
     * interval.
     *
     * @return the flag indicating to return values and their units depending on the requested time
     *         interval.
     */
    public boolean isTotalValuesRequested() {
        return totalValuesRequested;
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            final EntityCostFilter other = (EntityCostFilter)obj;
            return Objects.equals(costSources, other.costSources)
                && Objects.equals(costCategoryFilter, other.costCategoryFilter)
                && excludeCostSources == other.excludeCostSources
                && Objects.equals(accountIds, other.accountIds)
                && Objects.equals(availabilityZoneIds, other.availabilityZoneIds)
                && Objects.equals(regionIds, other.regionIds)
                && Objects.equals(topologyContextId, other.topologyContextId)
                && totalValuesRequested == other.totalValuesRequested;
        }
        return false;
    }

    @Override
    public int hashCode() {
        Function<Set<?>, Integer> setHashCode = (set) -> (set == null) ? 0 : set.stream()
            .map(Object::hashCode).collect(Collectors.summingInt(Integer::intValue));
        return Objects.hash(setHashCode.apply(costSources),
            costCategoryFilter,
            excludeCostSources, setHashCode.apply(accountIds), setHashCode.apply(availabilityZoneIds),
            setHashCode.apply(regionIds), super.hashCode(), totalValuesRequested);
    }

    @Override
    @Nonnull
    public String toString() {
        StringBuilder builder = new StringBuilder(super.toString());
        builder.append("\n exclude cost sources: ");
        builder.append(excludeCostSources);
        builder.append("\n cost sources: ");
        builder.append((costSources == null) ? "NOT SET"
                : costSources.stream().map(String::valueOf).collect(Collectors.joining(",")));
        builder.append("\n cost category filter: ");
        builder.append((costCategoryFilter == null) ? "NOT SET" : costCategoryFilter);
        builder.append("\n account ids: ");
        builder.append((accountIds == null) ? "NOT SET"
                : accountIds.stream().map(String::valueOf).collect(Collectors.joining(",")));
        builder.append("\n availability zone ids: ");
        builder.append((availabilityZoneIds == null) ? "NOT SET"
                : availabilityZoneIds.stream().map(String::valueOf).collect(Collectors.joining(",")));
        builder.append("\n region ids: ");
        builder.append((regionIds == null) ? "NOT SET"
                : regionIds.stream().map(String::valueOf).collect(Collectors.joining(",")));
        if (conditions != null) {
            builder.append("\n conditions: ");
            builder.append(
                    conditions.stream().map(Condition::toString).collect(Collectors.joining(" AND ")));
        }
        builder.append("\n total values requested: ");
        builder.append(totalValuesRequested);
        return builder.toString();
    }

    private Set<CostSource> resolveCostSources(@Nonnull CostSourceLinkDTO costSourceLink) {

        final ImmutableSet.Builder<CostSource> costSources = ImmutableSet.<CostSource>builder()
                .add(costSourceLink.getCostSource());

        if (costSourceLink.hasDiscountCostSourceLink()) {
            costSources.addAll(resolveCostSources(costSourceLink.getDiscountCostSourceLink()));
        }

        return costSources.build();
    }

    private boolean filterComponentCostByCostSource(@Nonnull EntityCost.ComponentCost componentCost) {

        final Set<CostSource> componentCostSources = componentCost.hasCostSourceLink()
                ? resolveCostSources(componentCost.getCostSourceLink())
                : Collections.EMPTY_SET;

        return getCostSources().map(costSources -> {

            // If this is exclusion filter, it covers those entries that don't have cost source and
            // those that don't have any of the filtered costs sources in the cost source chain.
            if (excludeCostSources) {
                return Sets.intersection(costSources, componentCostSources).isEmpty();
            } else {
                // if this is inclusion filter only return true if the cost component has a cost source
                // chain and all cost sources in the chain are included in the filter
                final Set<CostSource> costSourceIntersection = Sets.intersection(costSources, componentCostSources);
                return !componentCostSources.isEmpty()
                        && costSourceIntersection.size() == componentCostSources.size();
            }
        }).orElse(true);
    }

    private boolean filterComponentCostByCostCategory(@Nonnull EntityCost.ComponentCost componentCost) {
        return getCostCategoryFilter().map(filter -> {
            if (filter.getExclusionFilter()) {
                return !filter.getCostCategoryList().contains(componentCost.getCategory());
            } else {
                return filter.getCostCategoryList().contains(componentCost.getCategory());
            }
        }).orElse(true);
    }

    /**
     * The builder class for {@link EntityCostFilter}.
     */
    public static class EntityCostFilterBuilder extends CostFilterBuilder<EntityCostFilterBuilder,
            EntityCostFilter> {
        private boolean excludeCostSources = false;
        private Set<CostSource> costSources = null;
        private CostCategoryFilter costCategoryFilter = null;
        private Set<Long> accountIds = null;
        private Set<Long> availabilityZoneIds = null;
        private Set<Long> regionIds = null;
        private Long topologyContextId = null;
        private List<GroupBy> requestedGroupBy = null;
        private boolean totalValuesRequested = false;

        private EntityCostFilterBuilder(@Nonnull TimeFrame timeFrame,
                long realtimeTopologyContextId) {
            super(realtimeTopologyContextId);
            this.timeFrame = timeFrame;
        }

        /**
         * Factory method.
         * @param timeFrame  the time frame that we are making this query for.
         * @param realtimeTopologyContextId RT topology context ID
         * @return a new instance of builder class.
         */
        @Nonnull
        public static EntityCostFilterBuilder newBuilder(@Nonnull TimeFrame timeFrame,
                long realtimeTopologyContextId) {
            return new EntityCostFilterBuilder(timeFrame, realtimeTopologyContextId);
        }

        @Override
        @Nonnull
        public EntityCostFilter build() {
            return new EntityCostFilter(entityIds, entityTypeFilters, startDateMillis,
                    endDateMillis, timeFrame, groupByFields, excludeCostSources, costSources,
                    costCategoryFilter, accountIds, availabilityZoneIds, regionIds,
                    latestTimeStampRequested, topologyContextId, realtimeTopologyContextId,
                    requestedGroupBy, totalValuesRequested);
        }

        /**
         * Sets cost sources to filter.
         * @param excludeCostSources the list of cost sources.
         * @param costSources if true the cost sources provided will be excluded and other cost
         *                    sources will be shown. Otherwise, only the specified cost sources
         *                    will be included.
         * @return the builder.
         */
        @Nonnull
        public EntityCostFilterBuilder costSources(boolean excludeCostSources,
                @Nonnull Collection<CostSource> costSources) {
            this.excludeCostSources = excludeCostSources;
            this.costSources = ImmutableSet.copyOf(costSources);
            return this;
        }

        /**
         * Sets cost category filter.
         * @param costCategoryFilter The filter for cost categories
         * @return the builder.
         */
        @Nonnull
        public EntityCostFilterBuilder costCategoryFilter(@Nonnull CostCategoryFilter costCategoryFilter) {
            this.costCategoryFilter = costCategoryFilter;
            return this;
        }

        /**
         * Sets account ids to filter.
         * @param accountIds the account ids for entities to include.
         * @return the builder.
         */
        @Nonnull
        public EntityCostFilterBuilder accountIds(
            @Nonnull Collection<Long> accountIds) {
            this.accountIds = new HashSet<>(accountIds);
            return this;
        }

        /**
         * Return the original group by enums from the request.
         * @param requestedGroupBy requested group by enum.
         * @return the builder.
         */
        @Nonnull
        public EntityCostFilterBuilder requestedGroupByEnums(@Nonnull List<GroupBy> requestedGroupBy) {
            this.requestedGroupBy = new ArrayList<>(requestedGroupBy);
            return this;
        }

        /**
         * Sets availability zone ids to filter.
         * @param availabilityZoneIds the availability zone for entities to include.
         * @return the builder.
         */
        @Nonnull
        public EntityCostFilterBuilder availabilityZoneIds(
            @Nonnull Collection<Long> availabilityZoneIds) {
            this.availabilityZoneIds = new HashSet<>(availabilityZoneIds);
            return this;
        }

        /**
         * Sets region ids to filter.
         * @param regionIds the region ids for entities to include.
         * @return the builder.
         */
        @Nonnull
        public EntityCostFilterBuilder regionIds(
            @Nonnull Collection<Long> regionIds) {
            this.regionIds = new HashSet<>(regionIds);
            return this;
        }

        /**
         * Set the associated topology context ID.
         * @param topologyContextId the topology context ID
         * @return this builder
         */
        @Nonnull
        public EntityCostFilterBuilder topologyContextId(long topologyContextId) {
            this.topologyContextId = topologyContextId;
            return this;
        }

        /**
         * Set the flag indicating to return values and their units depending on the requested time
         * interval.
         *
         * @param totalValuesRequested the flag indicating to return values and their units
         *         depending on the requested time interval.
         * @return this builder
         */
        @Nonnull
        public EntityCostFilterBuilder totalValuesRequested(final boolean totalValuesRequested) {
            this.totalValuesRequested = totalValuesRequested;
            return this;
        }
    }

    /**
     * GroupBy for {@link com.vmturbo.common.protobuf.cost.Cost.CloudCostStatRecord}.
     * @return null if there was no groupBy property in request.
     */
    @Override
    @Nullable
    public CostGroupBy getCostGroupBy() {
        return costGroupBy;
    }
}
