package com.vmturbo.cost.component.cleanup;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.jooq.DSLContext;

import com.vmturbo.cost.component.cleanup.RetentionDurationFetcher.BoundedDuration;

/**
 * A cleanup task with a custom retention period defined through {@link RetentionDurationFetcher}.
 */
public class CustomRetentionCleanup implements CostTableCleanup {

    private static final Map<TemporalUnit, UnaryOperator<LocalDateTime>> TRUNCATION_BY_UNIT =
            ImmutableMap.<TemporalUnit, UnaryOperator<LocalDateTime>>builder()
                    .put(ChronoUnit.MONTHS, CustomRetentionCleanup::truncateMonths)
                    .put(ChronoUnit.DAYS, CustomRetentionCleanup::truncateDays)
                    .put(ChronoUnit.HOURS, CustomRetentionCleanup::truncateHours)
                    .put(ChronoUnit.MINUTES, CustomRetentionCleanup::truncateMinutes)
                    .build();


    private final DSLContext dslContext;

    private final Clock clock;

    private final TableInfo tableInfo;

    private final RetentionDurationFetcher retentionDurationFetcher;

    /**
     * Constructs a new cleanup task.
     * @param dslContext The {@link DSLContext}.
     * @param clock The {@link Clock} to use in determining the trim time.
     * @param tableInfo The target table info to trim.
     * @param retentionDurationFetcher The provider of the retention period.
     */
    public CustomRetentionCleanup(@Nonnull DSLContext dslContext,
                                  @Nonnull Clock clock,
                                  @Nonnull TableInfo tableInfo,
                                  @Nonnull RetentionDurationFetcher retentionDurationFetcher) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.clock = Objects.requireNonNull(clock);
        this.tableInfo = Objects.requireNonNull(tableInfo);
        this.retentionDurationFetcher = Objects.requireNonNull(retentionDurationFetcher);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public LocalDateTime getTrimTime() {

        final BoundedDuration retentionDuration = retentionDurationFetcher.getRetentionDuration();
        final UnaryOperator<LocalDateTime> truncateFn = TRUNCATION_BY_UNIT.get(retentionDuration.unit());

        if (truncateFn == null) {
            throw new UnsupportedOperationException(
                    String.format("%s is not a supported retention unit", retentionDuration.unit()));
        } else {
            final LocalDateTime truncatedTime = truncateFn.apply(LocalDateTime.now(clock));
            return truncatedTime.minus(retentionDuration.amount(), retentionDuration.unit());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Trimmer writer() {
        return new CostTableTrimmer(dslContext, tableInfo);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public TableInfo tableInfo() {
        return tableInfo;
    }

    @Nonnull
    private static LocalDateTime truncateMonths(@Nonnull LocalDateTime sourceTime) {
        return LocalDateTime.of(sourceTime.getYear(), sourceTime.getMonth(), 1, 0, 0);
    }

    @Nonnull
    private static LocalDateTime truncateDays(@Nonnull LocalDateTime sourceTime) {
        return sourceTime.truncatedTo(ChronoUnit.DAYS);
    }

    @Nonnull
    private static LocalDateTime truncateHours(@Nonnull LocalDateTime sourceTime) {
        return sourceTime.truncatedTo(ChronoUnit.HOURS);
    }

    @Nonnull
    private static LocalDateTime truncateMinutes(@Nonnull LocalDateTime sourceTime) {
        return sourceTime.truncatedTo(ChronoUnit.MINUTES);
    }

}
