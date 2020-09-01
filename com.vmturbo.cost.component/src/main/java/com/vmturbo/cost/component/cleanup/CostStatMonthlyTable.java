package com.vmturbo.cost.component.cleanup;

import java.time.Clock;
import java.time.LocalDateTime;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;

/**
 * Class representing the monthly cost stat table.
 */
public class CostStatMonthlyTable extends AbstractStatTableCleanup {

    /**
     * Constructor for the Cost Monthly Stat table.
     *
     * @param context the dsl context.
     * @param clock the clock
     * @param tableInfo Information describing the table.
     * @param retentionPeriodFetcher A fetcher to resolve the stats retention periods.
     */
    public CostStatMonthlyTable(@Nonnull DSLContext context,
                                @Nonnull Clock clock,
                                @Nonnull TableInfo tableInfo,
                                @Nonnull RetentionPeriodFetcher retentionPeriodFetcher) {
        super(context, clock, tableInfo, retentionPeriodFetcher);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public LocalDateTime getTrimTime() {
        final RetentionPeriods retentionPeriods = retentionPeriodFetcher.getRetentionPeriods();

        final LocalDateTime now = LocalDateTime.now(clock);
        final LocalDateTime truncatedTime = LocalDateTime.of(now.getYear(), now.getMonth(), 1, 0, 0);
        return truncatedTime.minusMonths(retentionPeriods.monthlyRetentionMonths());
    }
}
