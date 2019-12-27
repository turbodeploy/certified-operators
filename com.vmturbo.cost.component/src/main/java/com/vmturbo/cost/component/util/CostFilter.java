package com.vmturbo.cost.component.util;

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

import org.jooq.Condition;
import org.jooq.Table;

import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;


/**
 * A abstract class represents the filter object.
 * Current it's only used with entity and expense cost tables.
 * TODO there is discussion to remove this abstract class.
 * See https://rbcommons.com/s/VMTurbo/r/26851/.
 */
public abstract class CostFilter {

    protected final Long startDateMillis;
    protected final Long endDateMillis;
    protected final Set<Integer> entityTypeFilters;
    protected final Set<Long> entityFilters;
    protected final TimeFrame timeFrame;
    protected final String snapshotTime;
    protected final boolean latestTimeStampRequested;


    CostFilter(@Nullable final Set<Long> entityFilters,
               @Nullable final Set<Integer> entityTypeFilters,
               @Nullable final Long startDateMillis,
               @Nullable final Long endDateMillis,
               @Nonnull final TimeFrame timeFrame,
               @Nonnull final String snapshotTime,
               final boolean latestTimeStampRequested) {
        this.startDateMillis = startDateMillis;
        this.endDateMillis = endDateMillis;
        this.timeFrame = timeFrame;
        this.snapshotTime = snapshotTime;
        this.entityFilters = entityFilters;
        this.entityTypeFilters = entityTypeFilters;
        this.latestTimeStampRequested = latestTimeStampRequested;
    }

    /**
     * Generate a list of {@link Condition} based on different fields.
     *
     * @return a list of {@link Condition}.
     */
    abstract public List<Condition> generateConditions();

    abstract public Condition[] getConditions();

    public abstract Table<?> getTable();

    @Nonnull
    public Optional<Long> getStartDateMillis() {
        return Optional.ofNullable(startDateMillis);
    }

    @Nonnull
    public Optional<Long> getEndDateMillis() {
        return Optional.ofNullable(endDateMillis);
    }

    @Nonnull
    public Optional<Set<Long>> getEntityFilters() {
        return Optional.ofNullable(entityFilters);
    }

    @Nonnull
    public Optional<Set<Integer>> getEntityTypeFilters() {
        return Optional.ofNullable(entityTypeFilters);
    }

    @Nonnull
    public TimeFrame getTimeFrame() {
        return timeFrame;
    }

    /**
     * if we should return latest timestamp rather than all the timestamps in the query.
     * @return if true latest timestamp is only returned.
     */
    public boolean isLatestTimeStampRequested() {
        return latestTimeStampRequested;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        final CostFilter other = (CostFilter)obj;
        return Objects.equals(startDateMillis, other.startDateMillis)
            && Objects.equals(endDateMillis, other.endDateMillis)
            && Objects.equals(entityTypeFilters, other.entityTypeFilters)
            && Objects.equals(entityFilters, other.entityFilters)
            && Objects.equals(timeFrame, other.timeFrame)
            && Objects.equals(snapshotTime, other.snapshotTime)
            && (latestTimeStampRequested == other.latestTimeStampRequested);
    }

    @Override
    public int hashCode() {
        Function<Set<?>, Integer> setHashCode = (set) -> (set == null) ? 0 : set.stream()
            .map(Object::hashCode).collect(Collectors.summingInt(Integer::intValue));
        return Objects.hash(startDateMillis, endDateMillis,
            setHashCode.apply(entityTypeFilters),
            setHashCode.apply(entityFilters),
            timeFrame, snapshotTime, latestTimeStampRequested);
    }

    @Override
    @Nonnull
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("start time (ms): ");
        builder.append(startDateMillis);
        builder.append("\n end time (ms): ");
        builder.append(endDateMillis);
        builder.append("\n entity type filters: ");
        builder.append((entityTypeFilters == null) ? "NOT SET" :
            entityTypeFilters.stream().map(String::valueOf).collect(Collectors.joining(",")));
        builder.append("\n entity id filters: ");
        builder.append((entityFilters == null) ? "NOT SET" :
            entityFilters.stream().map(String::valueOf).collect(Collectors.joining(",")));
        builder.append("\n time frame: ");
        builder.append(timeFrame);
        builder.append("\n is latest timestamp requested: ");
        builder.append(isLatestTimeStampRequested());
        builder.append("\n snapshotTime: ");
        builder.append(snapshotTime);

        return builder.toString();
    }

    /**
     * Represents the builder for {@link CostFilter}.
     * @param <B> the type of the builder that implements this.
     * @param <F> the type of class that this builder make instances of.
     */
    public abstract static class CostFilterBuilder<B extends CostFilterBuilder,
            F extends CostFilter> {
        protected Long startDateMillis = null;
        protected Long endDateMillis = null;
        protected Set<Integer> entityTypeFilters = null;
        protected Set<Long> entityIds = null;
        protected TimeFrame timeFrame = null;
        protected boolean latestTimeStampRequested = false;

        /**
         * Returns the new instance of built filter.
         *
         * @return the new instance of built filter.
         */
        @Nonnull
        public abstract F build();

        /**
         * Sets the start and end of duration we want to filter the costs for.
         *
         * @param startDateMillis the start of duration in linux timestamp (ms).
         * @param endDateMillis the end of durantion in linux timestamp (ms).
         * @return the builder.
         */
        @Nonnull
        public B duration(@Nonnull Long startDateMillis,
                          @Nonnull Long  endDateMillis) {
            this.startDateMillis = startDateMillis;
            this.endDateMillis = endDateMillis;
            return (B)this;
        }

        /**
         * Removes the duration that has been set on the filter if it has been set.
         * @return the builder.
         */
        public B removeDuration() {
            this.startDateMillis = null;
            this.endDateMillis = null;
            return (B)this;
        }

        /**
         * Sets the entity type to include in the cost.
         *
         * @param entityTypeFilters the entity types represented by the type number.
         * @return the builder.
         */
        @Nonnull
        public B entityTypes(@Nonnull Collection<Integer> entityTypeFilters) {
            this.entityTypeFilters = new HashSet<>(entityTypeFilters);
            return (B)this;
        }

        /**
         * Sets the entity ids to include the cost for.
         *
         * @param entityIds the oid of entities to include the cost for.
         * @return the builder.
         */
        @Nonnull
        public B entityIds(@Nonnull Collection<Long> entityIds) {
            this.entityIds = new HashSet<>(entityIds);
            return (B)this;
        }

        /**
         * Sets the time frame to include the cost for.
         *
         * @param timeFrame the time frame to include the cost for.
         * @return the builder.
         */
        @Nonnull
        public B timeFrame(@Nonnull TimeFrame timeFrame) {
            this.timeFrame = timeFrame;
            return (B)this;
        }

        /**
         * Sets if we should return latest timestamp rather than all the timestamps in the query.
         *
         * @param latestTimeStampRequested if true only latest timestamp will be returned.
         * @return the builder.
         */
        @Nonnull
        public B latestTimestampRequested(boolean latestTimeStampRequested) {
            this.latestTimeStampRequested = latestTimeStampRequested;
            return (B)this;
        }
    }
}
