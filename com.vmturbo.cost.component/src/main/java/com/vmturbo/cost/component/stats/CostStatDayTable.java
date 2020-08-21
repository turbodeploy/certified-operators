package com.vmturbo.cost.component.stats;

import java.time.Clock;
import java.time.LocalDateTime;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;

/**
 * Table info describing the daily cost stat tables.
 */
public class CostStatDayTable implements CostStatTable {

    private final DSLContext dslContext;
    private final Clock clock;
    private final TableInfo tableInfo;

    /**
     * Constructor for the cost day Stat table.
     *
     * @param dslContext the dsl context.
     * @param clock the clock
     * @param tableInfo Information describing the table.
     */
    public CostStatDayTable(final DSLContext dslContext,
                            final Clock clock, final TableInfo tableInfo) {
        this.dslContext = dslContext;
        this.clock = clock;
        this.tableInfo = tableInfo;
    }

    @Nonnull
    @Override
    public LocalDateTime getTrimTime(@Nonnull final RetentionPeriods retentionPeriods) {
        return tableInfo.timeTruncateFn().apply(
                LocalDateTime.now(clock).minusDays(retentionPeriods.dailyRetentionDays()));
    }

    @Override
    public Trimmer writer() {
        return new CostStatsTableTrimmer(dslContext, clock, tableInfo);
    }
}
