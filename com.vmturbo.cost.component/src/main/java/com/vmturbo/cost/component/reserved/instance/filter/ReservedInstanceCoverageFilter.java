package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.Table;

import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;
import com.vmturbo.cost.component.db.Tables;

/**
 * A filter to restrict the reserved instance coverage records from the
 * {@link com.vmturbo.cost.component.reserved.instance.ReservedInstanceCoverageStore}.
 * It provider a easier way to define simple search over reserved instances coverage records
 * in the tables.
 */
public class ReservedInstanceCoverageFilter extends ReservedInstanceStatsFilter {

    /**
     * Constructor for ReservedInstanceCoverageFilter.
     *
     * @param scopeIds The scope(s) Ids.
     * @param scopeEntityType The scope(s) entity type.
     * @param startDateMillis Start time in ms.
     * @param endDateMillis End time in ms.
     * @param timeFrame The timeframe for which to obtain stats.
     */
    private ReservedInstanceCoverageFilter(@Nonnull final Set<Long> scopeIds,
                                           final Optional<Integer> scopeEntityType,
                                           final long startDateMillis,
                                           final long endDateMillis,
                                           @Nullable final TimeFrame timeFrame) {
        super(scopeIds, scopeEntityType, startDateMillis, endDateMillis,
              timeFrame);
    }

    @Override
    public Table<?> getTableName() {
        if (this.timeFrame == null || this.timeFrame.equals(TimeFrame.LATEST)) {
            return Tables.RESERVED_INSTANCE_COVERAGE_LATEST;
        } else if (this.timeFrame.equals(TimeFrame.HOUR)) {
            return Tables.RESERVED_INSTANCE_COVERAGE_BY_HOUR;
        } else if (this.timeFrame.equals(TimeFrame.DAY)) {
            return Tables.RESERVED_INSTANCE_COVERAGE_BY_DAY;
        } else {
            return Tables.RESERVED_INSTANCE_COVERAGE_BY_MONTH;
        }
    }

    /**
     * Create a builder used to construct a filter.
     *
     * @return The builder object.
     */
    public static ReservedInstanceCoverageFilter.Builder newBuilder() {
        return new ReservedInstanceCoverageFilter.Builder();
    }

    public static class Builder {
        // The set of scope oids.
        private Set<Long> scopeIds = new HashSet<>();
        // The scope's entity type.
        private Integer scopeEntityType;
        private long startDateMillis = 0;
        private long endDateMillis = 0;
        private TimeFrame timeFrame = null;

        private Builder() {}

        public ReservedInstanceCoverageFilter build() {
            return new ReservedInstanceCoverageFilter(scopeIds, getScopeEntityType(),
                                              startDateMillis, endDateMillis, timeFrame);
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
        public Builder setScopeEntityType(@Nullable final Integer entityType) {
            this.scopeEntityType = entityType;
            return this;
        }

        @Nonnull
        public ReservedInstanceCoverageFilter.Builder setStartDateMillis(final long millis) {
            this.startDateMillis = millis;
            return this;
        }

        @Nonnull
        public ReservedInstanceCoverageFilter.Builder setEndDateMillis(final long millis) {
            this.endDateMillis = millis;
            return this;
        }

        @Nonnull
        public ReservedInstanceCoverageFilter.Builder setTimeFrame(final TimeFrame timeFrame) {
            this.timeFrame = timeFrame;
            return this;
        }

        @Nonnull
        public Optional<Integer> getScopeEntityType() {
            return Optional.ofNullable(scopeEntityType);
        }
    }
}
