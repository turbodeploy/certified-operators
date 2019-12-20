package com.vmturbo.cost.component.stats;

import java.time.Clock;
import java.time.LocalDateTime;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;
import com.vmturbo.cost.component.db.Tables;

/**
 * Class representing the ReservedInstanceCoverageMonthly table.
 */
public class ReservedInstanceCoverageMonthlyStatTable implements ReservedInstanceStatTable {
    /**
     * Table info about the ReservedInstanceCoverageMonthlyTable.
     */
    public static final TableInfo COVERAGE_MONTHLY_TABLE_INFO =
            ImmutableTableInfo.builder().statTableSnapshotTime(Tables.RESERVED_INSTANCE_COVERAGE_BY_MONTH.SNAPSHOT_TIME)
                    .statTable(Tables.RESERVED_INSTANCE_COVERAGE_BY_MONTH).shortTableName("Coverage_monthly")
                    .timeTruncateFn(time -> LocalDateTime.of(time.getYear(), time.getMonth(), 1, 0, 0)).build();
    private final DSLContext dslContext;
    private final Clock clock;
    private final TableInfo tableInfo;

    /**
     * Constructor for the Reserved Instance Coverage Monthly Stat table.
     *
     * @param context the dsl context.
     * @param clock the clock
     * @param tableInfo Information describing the table.
     */
    public ReservedInstanceCoverageMonthlyStatTable(@Nonnull DSLContext context, @Nonnull Clock clock, @Nonnull TableInfo tableInfo) {
        this.clock = clock;
        this.dslContext = context;
        this.tableInfo = tableInfo;
    }

    @Nonnull
    @Override
    public LocalDateTime getTrimTime(@Nonnull final RetentionPeriods retentionPeriods) {
        return COVERAGE_MONTHLY_TABLE_INFO.timeTruncateFn().apply(
                LocalDateTime.now(clock).minusMonths(retentionPeriods.monthlyRetentionMonths()));
    }

    @Override
    public Trimmer writer() {
        return new ReservedInstanceStatsTableTrimmer(dslContext, clock, COVERAGE_MONTHLY_TABLE_INFO);
    }
}
