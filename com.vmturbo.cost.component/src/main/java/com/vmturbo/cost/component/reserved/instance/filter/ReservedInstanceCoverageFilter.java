package com.vmturbo.cost.component.reserved.instance.filter;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.Table;

import com.vmturbo.components.api.TimeFrameCalculator.TimeFrame;
import com.vmturbo.cost.component.db.Tables;

/**
 * A filter to restrict the reserved instance coverage records from the
 * {@link com.vmturbo.cost.component.reserved.instance.ReservedInstanceCoverageStore}.
 * It provider a easier way to define simple search over reserved instances coverage records
 * in the tables.
 */
public class ReservedInstanceCoverageFilter extends ReservedInstanceStatsFilter {

    private ReservedInstanceCoverageFilter(@Nonnull final Set<Long> regionIds,
                                           @Nonnull final Set<Long> availabilityZoneIds,
                                           @Nonnull final Set<Long> businessAccountIds,
                                           final long startDateMillis,
                                           final long endDateMillis,
                                           @Nullable final TimeFrame timeFrame) {
        super(regionIds, availabilityZoneIds, businessAccountIds, startDateMillis, endDateMillis, timeFrame);
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
        private Set<Long> regionIds = new HashSet<>();
        private Set<Long> availabilityZoneIds = new HashSet<>();
        private Set<Long> businessAccountIds = new HashSet<>();
        private long startDateMillis = 0;
        private long endDateMillis = 0;
        private TimeFrame timeFrame = null;

        private Builder() {}

        public ReservedInstanceCoverageFilter build() {
            return new ReservedInstanceCoverageFilter(regionIds, availabilityZoneIds, businessAccountIds,
                    startDateMillis, endDateMillis, timeFrame);
        }

        @Nonnull
        public ReservedInstanceCoverageFilter.Builder addRegionId(final long id) {
            this.regionIds.add(id);
            return this;
        }

        @Nonnull
        public ReservedInstanceCoverageFilter.Builder addAvailabilityZoneId(final long id) {
            this.availabilityZoneIds.add(id);
            return this;
        }

        @Nonnull
        public ReservedInstanceCoverageFilter.Builder addBusinessAccountId(final long id) {
            this.businessAccountIds.add(id);
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
    }
}
