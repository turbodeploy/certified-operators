package com.vmturbo.cost.component.util;

import static com.vmturbo.cost.component.db.Tables.ENTITY_COST;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
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
    private final Set<Integer> costCategories;

    private final List<Condition> conditions;

    EntityCostFilter(@Nullable final Set<Long> entityFilters,
                     @Nullable final Set<Integer> entityTypeFilters,
                     @Nullable final Long startDateMillis,
                     @Nullable final Long endDateMillis,
                     @Nullable final TimeFrame timeFrame,
                     final boolean excludeCostSources,
                     @Nullable final Set<Integer> costSources,
                     @Nullable final Set<Integer> costCategories,
                     final boolean latestTimeStampRequested) {
        super(entityFilters, entityTypeFilters, startDateMillis, endDateMillis, timeFrame,
            CREATED_TIME, latestTimeStampRequested);
        this.excludeCostSources = excludeCostSources;
        this.costSources = costSources;
        this.costCategories = costCategories;
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

        if (costCategories != null) {
            conditions.add(table.field(ENTITY_COST.COST_TYPE.getName()).in(costCategories));
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

    public Optional<Set<Integer>> getCostCategories() {
        return Optional.ofNullable(costCategories);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            final EntityCostFilter other = (EntityCostFilter)obj;
            return Objects.equals(costSources, other.costSources)
                && Objects.equals(costCategories, other.costCategories)
                && excludeCostSources == other.excludeCostSources;
        }
        return false;
    }

    @Override
    public int hashCode() {
        Function<Set<?>, Integer> setHashCode = (set) -> (set == null) ? 0 : set.stream()
            .map(Object::hashCode).collect(Collectors.summingInt(Integer::intValue));
        return Objects.hash(setHashCode.apply(costSources),
            setHashCode.apply(costCategories),
            excludeCostSources, super.hashCode());
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
        builder.append((costCategories == null) ? "NOT SET" :
            costCategories.stream().map(String::valueOf).collect(Collectors.joining(",")));
        builder.append("\n conditions: ");
        builder.append(
            conditions.stream().map(Condition::toString).collect(Collectors.joining(" AND ")));

        return builder.toString();
    }

    /**
     * The builder class for {@link EntityCostFilter}.
     */
    public static class EntityCostFilterBuilder extends CostFilterBuilder<EntityCostFilterBuilder,
            EntityCostFilter> {
        private boolean excludeCostSources = false;
        private Set<Integer> costSources = null;
        private Set<Integer> costCategories = null;

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
                endDateMillis, timeFrame, excludeCostSources, costSources, costCategories,
                latestTimeStampRequested);
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
         * Sets cost categories to filter.
         * @param costCategories the cost categories to include.
         * @return the builder.
         */
        @Nonnull
        public EntityCostFilterBuilder costCategories(
                                                   @Nonnull Set<Integer> costCategories) {
            this.costCategories = costCategories;
            return this;
        }
    }
}
