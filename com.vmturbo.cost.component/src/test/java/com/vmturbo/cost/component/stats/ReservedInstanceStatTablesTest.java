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
import com.vmturbo.cost.component.stats.ReservedInstanceStatTable.Trimmer;

/**
 * Testing all the stat tables for Reserved Instances.
 */
public class ReservedInstanceStatTablesTest {
    private DSLContext dslContext = mock(DSLContext.class);

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    @Test
    public void testReservedInstanceCoverageDayStatTable() {
        ReservedInstanceCoverageDayStatTable reservedInstanceCoverageDayStatTable =
                new ReservedInstanceCoverageDayStatTable(dslContext, clock, ReservedInstanceCoverageDayStatTable.COVERAGE_DAY_TABLE_INFO);
        assertTrue(reservedInstanceCoverageDayStatTable.writer() instanceof Trimmer);
        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriods.dailyRetentionDays()).thenReturn(1);
        final LocalDateTime trimmedTime = reservedInstanceCoverageDayStatTable.getTrimTime(retentionPeriods);
        assertThat(trimmedTime, is(LocalDateTime.now(clock).minusDays(1).truncatedTo(ChronoUnit.DAYS)));
    }

    @Test
    public void testReservedInstanceCoverageHourStatTable() {
        ReservedInstanceCoverageHourStatTable reservedInstanceCoverageHourStatTable =
                new ReservedInstanceCoverageHourStatTable(dslContext, clock, ReservedInstanceCoverageHourStatTable.COVERAGE_HOUR_TABLE_INFO);
        assertTrue(reservedInstanceCoverageHourStatTable.writer() instanceof Trimmer);
        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriods.hourlyRetentionHours()).thenReturn(1);
        final LocalDateTime trimmedTime = reservedInstanceCoverageHourStatTable.getTrimTime(retentionPeriods);
        assertThat(trimmedTime, is(LocalDateTime.now(clock).minusHours(1).truncatedTo(ChronoUnit.HOURS)));
    }

    @Test
    public void testReservedInstanceCoverageLatestStatTable() {
        ReservedInstanceCoverageLatestStatTable reservedInstanceCoverageLatestStatTable =
                new ReservedInstanceCoverageLatestStatTable(dslContext, clock, ReservedInstanceCoverageLatestStatTable.COVERAGE_LATEST_TABLE_INFO);
        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriods.latestRetentionMinutes()).thenReturn(5);
        final LocalDateTime trimmedTime = reservedInstanceCoverageLatestStatTable.getTrimTime(retentionPeriods);
        assertThat(trimmedTime, is(LocalDateTime.now(clock).minusMinutes(5).truncatedTo(ChronoUnit.MINUTES)));
    }

    @Test
    public void testReservedInstanceCoverageMonthlyStatTable() {
        ReservedInstanceCoverageMonthlyStatTable reservedInstanceCoverageMonthlyStatTable =
                new ReservedInstanceCoverageMonthlyStatTable(dslContext, clock, ReservedInstanceCoverageMonthlyStatTable.COVERAGE_MONTHLY_TABLE_INFO);
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
        ReservedInstanceUtilizationDayStatTable reservedInstanceUtilizationDayStatTable =
                new ReservedInstanceUtilizationDayStatTable(dslContext, clock, ReservedInstanceUtilizationDayStatTable.UTILIZATION_DAY_TABLE_INFO);
        assertTrue(reservedInstanceUtilizationDayStatTable.writer() instanceof Trimmer);
        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriods.dailyRetentionDays()).thenReturn(1);
        final LocalDateTime trimmedTime = reservedInstanceUtilizationDayStatTable.getTrimTime(retentionPeriods);
        assertThat(trimmedTime, is(LocalDateTime.now(clock).minusDays(1).truncatedTo(ChronoUnit.DAYS)));
    }

    @Test
    public void testReservedInstanceUtilizationHourStatTable() {
        ReservedInstanceUtilizationHourStatTable reservedInstanceUtilizationHourStatTable =
                new ReservedInstanceUtilizationHourStatTable(dslContext, clock, ReservedInstanceUtilizationHourStatTable.UTILIZATION_HOUR_TABLE_INFO);
        assertTrue(reservedInstanceUtilizationHourStatTable.writer() instanceof Trimmer);
        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriods.hourlyRetentionHours()).thenReturn(1);
        final LocalDateTime trimmedTime = reservedInstanceUtilizationHourStatTable.getTrimTime(retentionPeriods);
        assertThat(trimmedTime, is(LocalDateTime.now(clock).minusHours(1).truncatedTo(ChronoUnit.HOURS)));
    }

    @Test
    public void testReservedInstanceUtilizationLatestStatTable() {
        ReservedInstanceUtilizationLatestTable reservedInstanceUtilizationLatestTable =
                new ReservedInstanceUtilizationLatestTable(dslContext, clock, ReservedInstanceUtilizationLatestTable.UTILIZATION_LATEST_TABLE_INFO);
        assertTrue(reservedInstanceUtilizationLatestTable.writer() instanceof Trimmer);
        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriods.latestRetentionMinutes()).thenReturn(5);
        final LocalDateTime trimmedTime = reservedInstanceUtilizationLatestTable  .getTrimTime(retentionPeriods);
        assertThat(trimmedTime, is(LocalDateTime.now(clock).minusMinutes(5).truncatedTo(ChronoUnit.MINUTES)));
    }

    @Test
    public void testReservedInstanceUtilizationMonthlyStatTable() {
        ReservedInstanceCoverageMonthlyStatTable reservedInstanceCoverageMonthlyStatTable =
                new ReservedInstanceCoverageMonthlyStatTable(dslContext, clock, ReservedInstanceCoverageMonthlyStatTable.COVERAGE_MONTHLY_TABLE_INFO);
        assertTrue(reservedInstanceCoverageMonthlyStatTable.writer() instanceof Trimmer);
        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriods.monthlyRetentionMonths()).thenReturn(1);

        final LocalDateTime time = LocalDateTime.of(2018, Month.SEPTEMBER, 1, 1, 1);
        clock.changeInstant(time.toInstant(ZoneOffset.UTC));
        final LocalDateTime trimmedTime = reservedInstanceCoverageMonthlyStatTable.getTrimTime(retentionPeriods);
        assertThat(trimmedTime, is(LocalDateTime.of(2018, Month.AUGUST, 1, 0, 0)));
    }
}
