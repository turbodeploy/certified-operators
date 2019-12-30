package com.vmturbo.cost.component.stats;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import org.jooq.DSLContext;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.stats.CostStatTable.TableInfo;
import com.vmturbo.cost.component.stats.CostStatTable.Trimmer;

/**
 * Testing all the stat tables for Reserved Instances.
 */
public class CostStatTablesTest {
    private DSLContext dslContext = mock(DSLContext.class);

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private static final TableInfo COVERAGE_DAY_TABLE_INFO = ImmutableTableInfo.builder().statTableSnapshotTime(Tables.RESERVED_INSTANCE_COVERAGE_BY_DAY.SNAPSHOT_TIME)
            .statTable(Tables.RESERVED_INSTANCE_COVERAGE_BY_DAY).shortTableName("Coverage_daily")
            .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.DAYS)).build();

    private static final TableInfo COVERAGE_HOUR_TABLE_INFO =
            ImmutableTableInfo.builder().statTableSnapshotTime(Tables.RESERVED_INSTANCE_COVERAGE_BY_HOUR.SNAPSHOT_TIME)
                    .statTable(Tables.RESERVED_INSTANCE_COVERAGE_BY_HOUR).shortTableName("Coverage_hourly")
                    .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.HOURS)).build();

    public static final TableInfo COVERAGE_LATEST_TABLE_INFO =
            ImmutableTableInfo.builder().statTableSnapshotTime(Tables.RESERVED_INSTANCE_COVERAGE_LATEST.SNAPSHOT_TIME)
                    .statTable(Tables.RESERVED_INSTANCE_COVERAGE_LATEST).shortTableName("Coverage_latest")
                    .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.MINUTES)).build();

    private static final TableInfo COVERAGE_MONTHLY_TABLE_INFO =
            ImmutableTableInfo.builder().statTableSnapshotTime(Tables.RESERVED_INSTANCE_COVERAGE_BY_MONTH.SNAPSHOT_TIME)
                    .statTable(Tables.RESERVED_INSTANCE_COVERAGE_BY_MONTH).shortTableName("Coverage_monthly")
                    .timeTruncateFn(time -> LocalDateTime.of(time.getYear(), time.getMonth(), 1, 0, 0)).build();

    private static final TableInfo UTILIZATION_HOUR_TABLE_INFO =
            ImmutableTableInfo.builder().statTableSnapshotTime(Tables.RESERVED_INSTANCE_UTILIZATION_BY_HOUR.SNAPSHOT_TIME)
                    .statTable(Tables.RESERVED_INSTANCE_UTILIZATION_BY_HOUR).shortTableName("Utilization_hourly")
                    .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.HOURS)).build();

    private static final TableInfo UTILIZATION_DAY_TABLE_INFO =
            ImmutableTableInfo.builder().statTableSnapshotTime(Tables.RESERVED_INSTANCE_UTILIZATION_BY_DAY.SNAPSHOT_TIME)
                    .statTable(Tables.RESERVED_INSTANCE_UTILIZATION_BY_DAY).shortTableName("Utilization_daily")
                    .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.DAYS)).build();

    private static final TableInfo UTILIZATION_LATEST_TABLE_INFO =
            ImmutableTableInfo.builder().statTableSnapshotTime(Tables.RESERVED_INSTANCE_UTILIZATION_LATEST.SNAPSHOT_TIME)
                    .statTable(Tables.RESERVED_INSTANCE_UTILIZATION_LATEST).shortTableName("Utilization_Latest")
                    .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.MINUTES)).build();

    private static final TableInfo UTILIZATION_MONTHLY_TABLE_INFO =
            ImmutableTableInfo.builder().statTableSnapshotTime(Tables.RESERVED_INSTANCE_UTILIZATION_BY_MONTH.SNAPSHOT_TIME)
                    .statTable(Tables.RESERVED_INSTANCE_UTILIZATION_BY_MONTH).shortTableName("Utilization_Monthly")
                    .timeTruncateFn(time -> LocalDateTime.of(time.getYear(), time.getMonth(), 1, 0, 0)).build();

    private static final TableInfo ENTITY_COST =  ImmutableTableInfo.builder().statTableSnapshotTime(Tables.ENTITY_COST.CREATED_TIME)
            .statTable(Tables.ENTITY_COST).shortTableName("entity_cost")
            .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.MINUTES)).build();

    private static final TableInfo ENTITY_COST_BY_HOUR = ImmutableTableInfo.builder().statTableSnapshotTime(Tables.ENTITY_COST_BY_HOUR.CREATED_TIME)
            .statTable(Tables.ENTITY_COST_BY_HOUR).shortTableName("entity_cost_by_hour")
            .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.HOURS)).build();

    private static final TableInfo ENTITY_COST_BY_DAY = ImmutableTableInfo.builder().statTableSnapshotTime(Tables.ENTITY_COST_BY_DAY.CREATED_TIME)
            .statTable(Tables.ENTITY_COST_BY_DAY).shortTableName("entity_cost_by_day")
            .timeTruncateFn(time -> time.truncatedTo(ChronoUnit.DAYS)).build();

    private static final TableInfo ENTITY_COST_BY_MONTH = ImmutableTableInfo.builder().statTableSnapshotTime(Tables.ENTITY_COST_BY_MONTH.CREATED_TIME)
            .statTable(Tables.ENTITY_COST_BY_MONTH).shortTableName("entity_cost_by_month")
            .timeTruncateFn(time -> LocalDateTime.of(time.getYear(), time.getMonth(), 1, 0, 0)).build();



    @Test
    public void testReservedInstanceCoverageDayStatTable() {
        CostStatDayTable reservedInstanceCoverageDayStatTable =
                new CostStatDayTable(dslContext, clock, COVERAGE_DAY_TABLE_INFO);
        assertTrue(reservedInstanceCoverageDayStatTable.writer() instanceof Trimmer);
        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriods.dailyRetentionDays()).thenReturn(1);
        final LocalDateTime trimmedTime = reservedInstanceCoverageDayStatTable.getTrimTime(retentionPeriods);
        assertThat(trimmedTime, is(LocalDateTime.now(clock).minusDays(1).truncatedTo(ChronoUnit.DAYS)));
    }

    @Test
    public void testReservedInstanceCoverageHourStatTable() {
        CostStatHourTable reservedInstanceCoverageHourStatTable =
                new CostStatHourTable(dslContext, clock, COVERAGE_HOUR_TABLE_INFO);
        assertTrue(reservedInstanceCoverageHourStatTable.writer() instanceof Trimmer);
        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriods.hourlyRetentionHours()).thenReturn(1);
        final LocalDateTime trimmedTime = reservedInstanceCoverageHourStatTable.getTrimTime(retentionPeriods);
        assertThat(trimmedTime, is(LocalDateTime.now(clock).minusHours(1).truncatedTo(ChronoUnit.HOURS)));
    }

    @Test
    public void testReservedInstanceCoverageLatestStatTable() {
        CostStatLatestTable reservedInstanceCoverageLatestStatTable =
                new CostStatLatestTable(dslContext, clock, COVERAGE_LATEST_TABLE_INFO);
        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriods.latestRetentionMinutes()).thenReturn(5);
        final LocalDateTime trimmedTime = reservedInstanceCoverageLatestStatTable.getTrimTime(retentionPeriods);
        assertThat(trimmedTime, is(LocalDateTime.now(clock).minusMinutes(5).truncatedTo(ChronoUnit.MINUTES)));
    }

    @Test
    public void testReservedInstanceCoverageMonthlyStatTable() {
        CostStatMonthlyTable reservedInstanceCoverageMonthlyStatTable =
                new CostStatMonthlyTable(dslContext, clock, COVERAGE_MONTHLY_TABLE_INFO);
        assertTrue(reservedInstanceCoverageMonthlyStatTable.writer() instanceof Trimmer);
        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriods.monthlyRetentionMonths()).thenReturn(1);

        final LocalDateTime time = LocalDateTime.of(2018, Month.SEPTEMBER, 1, 1, 1);
        clock.changeInstant(time.toInstant(ZoneOffset.UTC));
        final LocalDateTime trimmedTime = reservedInstanceCoverageMonthlyStatTable.getTrimTime(retentionPeriods);
        assertThat(trimmedTime, is(LocalDateTime.of(2018, Month.AUGUST, 1, 0, 0)));
    }

    @Test
    public void testReservedInstanceUtilizationDayStatTable() {
        CostStatDayTable costStatDayTable =
                new CostStatDayTable(dslContext, clock, UTILIZATION_DAY_TABLE_INFO);
        assertTrue(costStatDayTable.writer() instanceof Trimmer);
        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriods.dailyRetentionDays()).thenReturn(1);
        final LocalDateTime trimmedTime = costStatDayTable.getTrimTime(retentionPeriods);
        assertThat(trimmedTime, is(LocalDateTime.now(clock).minusDays(1).truncatedTo(ChronoUnit.DAYS)));
    }

    @Test
    public void testReservedInstanceUtilizationHourStatTable() {
        CostStatHourTable costStatHourTable =
                new CostStatHourTable(dslContext, clock, UTILIZATION_HOUR_TABLE_INFO);
        assertTrue(costStatHourTable.writer() instanceof Trimmer);
        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriods.hourlyRetentionHours()).thenReturn(1);
        final LocalDateTime trimmedTime = costStatHourTable.getTrimTime(retentionPeriods);
        assertThat(trimmedTime, is(LocalDateTime.now(clock).minusHours(1).truncatedTo(ChronoUnit.HOURS)));
    }

    @Test
    public void testReservedInstanceUtilizationLatestStatTable() {
        CostStatLatestTable costStatLatestTable =
                new CostStatLatestTable(dslContext, clock, UTILIZATION_LATEST_TABLE_INFO);
        assertTrue(costStatLatestTable.writer() instanceof Trimmer);
        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriods.latestRetentionMinutes()).thenReturn(5);
        final LocalDateTime trimmedTime = costStatLatestTable.getTrimTime(retentionPeriods);
        assertThat(trimmedTime, is(LocalDateTime.now(clock).minusMinutes(5).truncatedTo(ChronoUnit.MINUTES)));
    }

    @Test
    public void testReservedInstanceUtilizationMonthlyStatTable() {
        CostStatMonthlyTable reservedInstanceCoverageMonthlyStatTable =
                new CostStatMonthlyTable(dslContext, clock, UTILIZATION_MONTHLY_TABLE_INFO);
        assertTrue(reservedInstanceCoverageMonthlyStatTable.writer() instanceof Trimmer);
        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriods.monthlyRetentionMonths()).thenReturn(1);

        final LocalDateTime time = LocalDateTime.of(2018, Month.SEPTEMBER, 1, 1, 1);
        clock.changeInstant(time.toInstant(ZoneOffset.UTC));
        final LocalDateTime trimmedTime = reservedInstanceCoverageMonthlyStatTable.getTrimTime(retentionPeriods);
        assertThat(trimmedTime, is(LocalDateTime.of(2018, Month.AUGUST, 1, 0, 0)));
    }

    @Test
    public void testEntityCostTable() {
        CostStatLatestTable entityCostStatLatestTable =
                new CostStatLatestTable(dslContext, clock, ENTITY_COST);
        assertTrue(entityCostStatLatestTable.writer() instanceof Trimmer);
        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriods.latestRetentionMinutes()).thenReturn(5);
        final LocalDateTime trimmedTime = entityCostStatLatestTable.getTrimTime(retentionPeriods);
        assertThat(trimmedTime, is(LocalDateTime.now(clock).minusMinutes(5).truncatedTo(ChronoUnit.MINUTES)));
    }

    @Test
    public void testEntityCostHourlyTable() {
        CostStatHourTable entityCostStatHourTable =
                new CostStatHourTable(dslContext, clock, ENTITY_COST_BY_HOUR);
        assertTrue(entityCostStatHourTable.writer() instanceof Trimmer);
        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriods.hourlyRetentionHours()).thenReturn(1);
        final LocalDateTime trimmedTime = entityCostStatHourTable.getTrimTime(retentionPeriods);
        assertThat(trimmedTime, is(LocalDateTime.now(clock).minusHours(1).truncatedTo(ChronoUnit.HOURS)));

    }

    @Test
    public void testEntityCostDailyTable() {
        CostStatDayTable entityCostStatDayTable =
                new CostStatDayTable(dslContext, clock, ENTITY_COST_BY_DAY);
        assertTrue(entityCostStatDayTable.writer() instanceof Trimmer);
        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriods.dailyRetentionDays()).thenReturn(1);
        final LocalDateTime trimmedTime = entityCostStatDayTable.getTrimTime(retentionPeriods);
        assertThat(trimmedTime, is(LocalDateTime.now(clock).minusDays(1).truncatedTo(ChronoUnit.DAYS)));
    }

    @Test
    public void testEntityCostMonthlyTable() {
        CostStatMonthlyTable entityCostStatMonthlyStatTable =
                new CostStatMonthlyTable(dslContext, clock, ENTITY_COST_BY_MONTH);
        assertTrue(entityCostStatMonthlyStatTable.writer() instanceof Trimmer);
        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriods.monthlyRetentionMonths()).thenReturn(1);

        final LocalDateTime time = LocalDateTime.of(2018, Month.SEPTEMBER, 1, 1, 1);
        clock.changeInstant(time.toInstant(ZoneOffset.UTC));
        final LocalDateTime trimmedTime = entityCostStatMonthlyStatTable.getTrimTime(retentionPeriods);
        assertThat(trimmedTime, is(LocalDateTime.of(2018, Month.AUGUST, 1, 0, 0)));
    }
}
