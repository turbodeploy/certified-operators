package com.vmturbo.cost.component.util;

import static com.vmturbo.cost.component.db.Tables.ENTITY_COST;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Table;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostCategoryFilter;
import com.vmturbo.common.protobuf.cost.Cost.EntityCost;
import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;
import com.vmturbo.cost.component.db.Tables;

/**
 * A filter to restrict the entity cost records from the
 * {@link com.vmturbo.cost.component.entity.cost.EntityCostStore}.
 * It provider a easier way to define simple search over entity cost records
 * in the tables.
 */
public class EntityCostFilter extends CostFilter {

    private static final Logger logger = LogManager.getLogger();

    private static final String CREATED_TIME = "created_time";

    private final boolean excludeCostSources;
    private final Set<Integer> costSources;
    private final CostCategoryFilter costCategoryFilter;
    private final Set<Long> accountIds;
    private final Set<Long> availabilityZoneIds;
    private final Set<Long> regionIds;

    private final List<Condition> conditions;

    EntityCostFilter(@Nullable final Set<Long> entityFilters,
                     @Nullable final Set<Integer> entityTypeFilters,
                     @Nullable final Long startDateMillis,
                     @Nullable final Long endDateMillis,
                     @Nullable final TimeFrame timeFrame,
                     final boolean excludeCostSources,
                     @Nullable final Set<Integer> costSources,
                     @Nullable final CostCategoryFilter costCategoryFilter,
                     @Nullable final Set<Long> accountIds,
                     @Nullable final Set<Long> availabilityZoneIds,
                     @Nullable final Set<Long> regionIds,
                     final boolean latestTimeStampRequested) {
        super(entityFilters, entityTypeFilters, startDateMillis, endDateMillis, timeFrame,
            CREATED_TIME, latestTimeStampRequested);
        this.excludeCostSources = excludeCostSources;
        this.costSources = costSources;
        this.costCategoryFilter = costCategoryFilter;
        this.accountIds = accountIds;
        this.availabilityZoneIds = availabilityZoneIds;
        this.regionIds = regionIds;

        this.conditions = generateConditions();
    }

    /**
     * Generate a list of {@link Condition} based on different fields.
     *
     * @return a list of {@link Condition}.
     */
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

        if (entityTypeFilters != null) {
            conditions.add(table.field(ENTITY_COST.ASSOCIATED_ENTITY_TYPE.getName()).in(entityTypeFilters));
        }

        if (entityFilters != null) {
            conditions.add(table.field(ENTITY_COST.ASSOCIATED_ENTITY_ID.getName()).in(entityFilters));
        }

        if (costCategoryFilter != null) {

            Set<Integer> costCategoryValues = costCategoryFilter.getCostCategoryList()
                    .stream()
                    .map(CostCategory::getNumber)
                    .collect(ImmutableSet.toImmutableSet());

            if (costCategoryFilter.getExclusionFilter()) {
                conditions.add(table.field(ENTITY_COST.COST_TYPE.getName()).notIn(costCategoryValues));
            } else {
                conditions.add(table.field(ENTITY_COST.COST_TYPE.getName()).in(costCategoryValues));
            }
        }

        if (costSources != null) {
            if (getTable() == ENTITY_COST) {
                if (excludeCostSources) {
                    conditions.add(table.field(ENTITY_COST.COST_SOURCE.getName()).notIn(costSources));
                } else {
                    conditions.add(table.field(ENTITY_COST.COST_SOURCE.getName()).in(costSources));
                }
            } else {
                logger.warn("Cost source filter has been set on a query on a table other than the" +
                    " latest. It will be ignored.");
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

        return conditions;
    }

    @Override
    public Condition[] getConditions() {
        return this.conditions.toArray(new Condition[conditions.size()]);
    }

    @Override
    public Table<?> getTable() {
        if (this.timeFrame == null || this.timeFrame.equals(TimeFrame.LATEST)) {
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

    public Optional<Set<Integer>> getCostSources() {
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
        return filterComponentCostByCostSource(componentCost) &&
                filterComponentCostByCostCategory(componentCost);
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

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            final EntityCostFilter other = (EntityCostFilter)obj;
            return Objects.equals(costSources, other.costSources)
                && Objects.equals(costCategoryFilter, other.costCategoryFilter)
                && excludeCostSources == other.excludeCostSources
                && Objects.equals(accountIds, other.accountIds)
                && Objects.equals(availabilityZoneIds, other.availabilityZoneIds)
                && Objects.equals(regionIds, other.regionIds);
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
            setHashCode.apply(regionIds), super.hashCode());
    }

    @Override
    @Nonnull
    public String toString() {
        StringBuilder builder = new StringBuilder(super.toString());
        builder.append("\n exclude cost sources: ");
        builder.append(excludeCostSources);
        builder.append("\n cost sources: ");
        builder.append((costSources == null) ? "NOT SET" :
            costSources.stream().map(String::valueOf).collect(Collectors.joining(",")));
        builder.append("\n cost category filter: ");
        builder.append((costCategoryFilter == null) ? "NOT SET" : costCategoryFilter);
        builder.append("\n account ids: ");
        builder.append((accountIds == null) ? "NOT SET" :
            accountIds.stream().map(String::valueOf).collect(Collectors.joining(",")));
        builder.append("\n availability zone ids: ");
        builder.append((availabilityZoneIds == null) ? "NOT SET" :
            availabilityZoneIds.stream().map(String::valueOf).collect(Collectors.joining(",")));
        builder.append("\n region ids: ");
        builder.append((regionIds == null) ? "NOT SET" :
            regionIds.stream().map(String::valueOf).collect(Collectors.joining(",")));
        builder.append("\n conditions: ");
        builder.append(
            conditions.stream().map(Condition::toString).collect(Collectors.joining(" AND ")));

        return builder.toString();
    }

    private boolean filterComponentCostByCostSource(@Nonnull EntityCost.ComponentCost componentCost) {

        return getCostSources().map(costSources -> {

            // If this is exclusion filter, it covers those entries that don't have cost source and
            // those that have cost source and is in the filter
            if (excludeCostSources) {
                return !componentCost.hasCostSource()
                        || !costSources.contains(componentCost.getCostSource().getNumber());
            } else {
                // if this is inclusion filter only return true if the cost component has a cost source
                // and its value is in input filter
                return componentCost.hasCostSource()
                        && costSources.contains(componentCost.getCostSource().getNumber());
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
        private Set<Integer> costSources = null;
        private CostCategoryFilter costCategoryFilter = null;
        private Set<Long> accountIds = null;
        private Set<Long> availabilityZoneIds = null;
        private Set<Long> regionIds = null;

        private EntityCostFilterBuilder(@Nonnull TimeFrame timeFrame) {
            this.timeFrame = timeFrame;
        }

        /**
         * Factory method.
         * @param timeFrame  the time frame that we are making this query for.
         * @return a new instance of builder class.
         */
        @Nonnull
        public static EntityCostFilterBuilder newBuilder(@Nonnull TimeFrame timeFrame) {
            return new EntityCostFilterBuilder(timeFrame);
        }

        @Override
        @Nonnull
        public EntityCostFilter build() {
            return new EntityCostFilter(entityIds, entityTypeFilters, startDateMillis,
                endDateMillis, timeFrame, excludeCostSources, costSources, costCategoryFilter,
                accountIds, availabilityZoneIds, regionIds, latestTimeStampRequested);
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
                                                   @Nonnull Set<Integer> costSources) {
            this.excludeCostSources = excludeCostSources;
            this.costSources = costSources;
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
    }
}
