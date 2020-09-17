package com.vmturbo.cost.component.cleanup;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;

/**
 * Table info describing the daily cost stat tables.
 */
public class CostStatDayTable extends AbstractStatTableCleanup {

    /**
     * Constructor for the cost day Stat table.
     *
     * @param dslContext the dsl context.
     * @param clock the clock
     * @param tableInfo Information describing the table.
     * @param retentionPeriodFetcher A fetcher to resolve the stats retention periods.
     */
    public CostStatDayTable(@Nonnull final DSLContext dslContext,
                            @Nonnull final Clock clock,
                            @Nonnull final TableInfo tableInfo,
                            @Nonnull RetentionPeriodFetcher retentionPeriodFetcher) {
        super(dslContext, clock, tableInfo, retentionPeriodFetcher);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public LocalDateTime getTrimTime() {
        final RetentionPeriods retentionPeriods = retentionPeriodFetcher.getRetentionPeriods();

        final LocalDateTime truncatedTime = LocalDateTime.now(clock).truncatedTo(ChronoUnit.DAYS);
        return truncatedTime.minusDays(retentionPeriods.dailyRetentionDays());
    }
}
