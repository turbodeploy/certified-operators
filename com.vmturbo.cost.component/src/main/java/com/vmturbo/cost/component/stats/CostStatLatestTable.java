package com.vmturbo.cost.component.stats;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import java.time.Clock;
import java.time.LocalDateTime;

import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;

/**
 * Class representing the latest cost stat table.
 */
public class CostStatLatestTable implements CostStatTable {

    private final DSLContext dslContext;
    private final Clock clock;
    private final TableInfo tableInfo;

    /**
     * Constructor for the Cost Latest Stat table.
     *
     * @param context the dsl context.
     * @param clock the clock
     * @param tableInfo Information describing the table.
     */
    public CostStatLatestTable(@Nonnull DSLContext context, @Nonnull Clock clock,
                               @Nonnull TableInfo tableInfo) {
        this.dslContext = context;
        this.clock = clock;
        this.tableInfo = tableInfo;
    }

    @Nonnull
    @Override
    public LocalDateTime getTrimTime(@Nonnull final RetentionPeriods retentionPeriods) {
        return tableInfo.timeTruncateFn().apply(
                LocalDateTime.now(clock).minusMinutes(retentionPeriods.latestRetentionMinutes()));
    }

    @Override
    public Trimmer writer() {
        return new CostStatsTableTrimmer(dslContext, clock, tableInfo);
    }
}
