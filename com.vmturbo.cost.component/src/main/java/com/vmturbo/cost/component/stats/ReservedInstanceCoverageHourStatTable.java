package com.vmturbo.cost.component.stats;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;
import com.vmturbo.cost.component.db.Tables;

/**
 * Class representing the ReservedInstanceCoverageHourTable.
 */
public class ReservedInstanceCoverageHourStatTable implements ReservedInstanceStatTable {

    /**
     * Table info describing the ReservedInstanceCoverageHourTable.
     */
    public static final TableInfo COVERAGE_HOUR_TABLE_INFO =
            ImmutableTableInfo.builder().statTableSnapshotTime(Tables.RESERVED_INSTANCE_COVERAGE_BY_HOUR.SNAPSHOT_TIME)
                    .statTable(Tables.RESERVED_INSTANCE_COVERAGE_BY_HOUR).shortTableName("Coverage_hourly")
                    .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.HOURS)).build();
    private final DSLContext dslContext;
    private final Clock clock;
    private final TableInfo tableInfo;

    /**
     * Constructor for the Reserved Instance Coverage Hour Stat table.
     *
     * @param context the dsl context.
     * @param clock the clock
     * @param tableInfo Information describing the table.
     */
    public ReservedInstanceCoverageHourStatTable(@Nonnull DSLContext context, @Nonnull Clock clock, @Nonnull TableInfo tableInfo) {
        this.dslContext = context;
        this.clock = clock;
        this.tableInfo = tableInfo;
    }

    @Nonnull
    @Override
    public LocalDateTime getTrimTime(@Nonnull final RetentionPeriods retentionPeriods) {
        return COVERAGE_HOUR_TABLE_INFO.timeTruncateFn().apply(
                LocalDateTime.now(clock).minusHours(retentionPeriods.hourlyRetentionHours()));
    }

    @Override
    public Trimmer writer() {
        return new ReservedInstanceStatsTableTrimmer(dslContext, clock, COVERAGE_HOUR_TABLE_INFO);
    }
}
