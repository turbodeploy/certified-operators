package com.vmturbo.cost.component.stats;

import java.time.Clock;
import java.time.LocalDateTime;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;

/**
 * Class representing the hourly cost stat tables.
 */
public class CostStatHourTable implements CostStatTable {

    private final DSLContext dslContext;
    private final Clock clock;
    private final TableInfo tableInfo;

    /**
     * Constructor for the Cost Hour Stat table.
     *
     * @param context the dsl context.
     * @param clock the clock
     * @param tableInfo Information describing the table.
     */
    public CostStatHourTable(@Nonnull DSLContext context,
                             @Nonnull Clock clock,
                             @Nonnull TableInfo tableInfo) {
        this.dslContext = context;
        this.clock = clock;
        this.tableInfo = tableInfo;
    }

    @Nonnull
    @Override
    public LocalDateTime getTrimTime(@Nonnull final RetentionPeriods retentionPeriods) {
        return tableInfo.timeTruncateFn().apply(
                LocalDateTime.now(clock).minusHours(retentionPeriods.hourlyRetentionHours()));
    }

    @Override
    public Trimmer writer() {
        return new CostStatsTableTrimmer(dslContext, clock, tableInfo);
    }
}
