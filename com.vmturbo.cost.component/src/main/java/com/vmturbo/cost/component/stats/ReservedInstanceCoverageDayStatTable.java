package com.vmturbo.cost.component.stats;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;
import com.vmturbo.cost.component.db.Tables;

/**
 * Table info describing the ReservedInstanceCoverageDayTable.
 */
public class ReservedInstanceCoverageDayStatTable implements ReservedInstanceStatTable {
    /**
     * Table info describing the ReservedInstanceCoverageHourTable.
     */
    public static final TableInfo COVERAGE_DAY_TABLE_INFO =
            ImmutableTableInfo.builder().statTableSnapshotTime(Tables.RESERVED_INSTANCE_COVERAGE_BY_DAY.SNAPSHOT_TIME)
                    .statTable(Tables.RESERVED_INSTANCE_COVERAGE_BY_DAY).shortTableName("Coverage_daily")
                    .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.DAYS)).build();
    private final DSLContext dslContext;
    private final Clock clock;
    private TableInfo tableInfo;

    /**
     * Constructor for the Reserved Instance Coverage day Stat table.
     *
     * @param context the dsl context.
     * @param clock the clock
     * @param tableInfo Information describing the table.
     */
    public ReservedInstanceCoverageDayStatTable(@Nonnull DSLContext context, @Nonnull Clock clock, @Nonnull TableInfo tableInfo) {
        this.clock = clock;
        this.dslContext = context;
        this.tableInfo = tableInfo;
    }

    @Nonnull
    @Override
    public LocalDateTime getTrimTime(@Nonnull final RetentionPeriods retentionPeriods) {
        return COVERAGE_DAY_TABLE_INFO.timeTruncateFn().apply(
                LocalDateTime.now(clock).minusDays(retentionPeriods.dailyRetentionDays()));
    }

    @Override
    public Trimmer writer() {
        return new ReservedInstanceStatsTableTrimmer(dslContext, clock, COVERAGE_DAY_TABLE_INFO);
    }
}
