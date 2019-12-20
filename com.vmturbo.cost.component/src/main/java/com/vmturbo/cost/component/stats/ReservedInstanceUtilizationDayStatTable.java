package com.vmturbo.cost.component.stats;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import javax.annotation.Nonnull;

import org.jooq.DSLContext;

import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;
import com.vmturbo.cost.component.db.Tables;

/**
 * Table info describing the ReservedInstanceUtilizationDayTable.
 */
public class ReservedInstanceUtilizationDayStatTable implements ReservedInstanceStatTable {
    /**
     * Table info describing the ReservedInstanceUtilizationHourTable.
     */
    public static final TableInfo UTILIZATION_DAY_TABLE_INFO =
            ImmutableTableInfo.builder().statTableSnapshotTime(Tables.RESERVED_INSTANCE_UTILIZATION_BY_DAY.SNAPSHOT_TIME)
                    .statTable(Tables.RESERVED_INSTANCE_UTILIZATION_BY_DAY).shortTableName("Utilization_daily")
                    .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.DAYS)).build();
    private final DSLContext dslContext;
    private final Clock clock;
    private final TableInfo tableInfo;

    /**
     * Constructor for the Reserved Instance Utilization day Stat table.
     *
     * @param dslContext the dsl context.
     * @param clock the clock
     * @param tableInfo Information describing the table.
     */
    public ReservedInstanceUtilizationDayStatTable(final DSLContext dslContext,
                                                   final Clock clock, final TableInfo tableInfo) {
        this.dslContext = dslContext;
        this.clock = clock;
        this.tableInfo = tableInfo;
    }

    @Nonnull
    @Override
    public LocalDateTime getTrimTime(@Nonnull final RetentionPeriods retentionPeriods) {
        return UTILIZATION_DAY_TABLE_INFO.timeTruncateFn().apply(
                LocalDateTime.now(clock).minusDays(retentionPeriods.dailyRetentionDays()));
    }

    @Override
    public Trimmer writer() {
        return new ReservedInstanceStatsTableTrimmer(dslContext, clock, UTILIZATION_DAY_TABLE_INFO);
    }
}
