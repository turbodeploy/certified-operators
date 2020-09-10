package com.vmturbo.cost.component.cleanup;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;

/**
 * Class representing the latest cost stat table.
 */
public class CostStatLatestTable extends AbstractStatTableCleanup {

    /**
     * Constructor for the Cost Latest Stat table.
     *
     * @param context the dsl context.
     * @param clock the clock
     * @param tableInfo Information describing the table.
     * @param retentionPeriodFetcher A fetcher to resolve the stats retention periods.
     */
    public CostStatLatestTable(@Nonnull DSLContext context,
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

        final LocalDateTime truncatedTime = LocalDateTime.now(clock).truncatedTo(ChronoUnit.MINUTES);
        return truncatedTime.minusMinutes(retentionPeriods.latestRetentionMinutes());
    }
}
