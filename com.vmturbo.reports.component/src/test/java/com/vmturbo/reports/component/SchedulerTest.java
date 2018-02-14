package com.vmturbo.reports.component;

import java.time.Clock;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.mail.EmailException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.reporting.api.protobuf.Reporting;
import com.vmturbo.reports.component.communication.ReportingServiceRpc;
import com.vmturbo.reports.component.instances.ReportsGenerator;
import com.vmturbo.reports.component.schedules.ScheduleDAO;
import com.vmturbo.reports.component.schedules.Scheduler;
import com.vmturbo.sql.utils.DbException;

/**
 * Tests for scheduling of reports. ReportsGenerator is mocked so it doesn't generate reports really
 * but it calculates when report should be generated next time and tests it.
 */
public class SchedulerTest {

    public static final String DAILY = "Daily";
    public static final String WEEKLY = "Weekly";
    public static final String MONTHLY = "Monthly";

    private final Clock clock = Mockito.mock(Clock.class);

    private ScheduleDAO scheduleDAO;

    private Scheduler scheduler;

    private ReportsGenerator reportsGenerator;

    private final int generationTime = 1;

    private final MockTimer mockTimer = new MockTimer(clock);

    private final Reporting.ScheduleDTO dailyScheduleDto =
                    buildScheduleDto(19, "Daily", "PDF", "", 0);

    private final Reporting.ScheduleDTO weeklyScheduleDto =
                    buildScheduleDto(19, "Weekly", "PDF", "Wed", 0);

    private final Reporting.ScheduleDTO monthlyScheduleDto =
                    buildScheduleDto(19, "Monthly", "PDF", "", 7);

    private final Reporting.ScheduleDTO illegalPeriodDto = buildScheduleDto(19, "Illegal", "PDF",
                    "Mon", 4);

    @Before
    public void init() throws DbException {
        scheduleDAO = Mockito.mock(ScheduleDAO.class);
        Mockito.when(scheduleDAO.addSchedule(dailyScheduleDto.getScheduleInfo())).thenReturn(dailyScheduleDto);
        Mockito.when(scheduleDAO.addSchedule(weeklyScheduleDto.getScheduleInfo())).thenReturn(weeklyScheduleDto);
        Mockito.when(scheduleDAO.addSchedule(monthlyScheduleDto.getScheduleInfo())).thenReturn(monthlyScheduleDto);
        Mockito.when(scheduleDAO.addSchedule(illegalPeriodDto.getScheduleInfo())).thenReturn(illegalPeriodDto);
        reportsGenerator = Mockito.mock(ReportsGenerator.class);
        Mockito.when(clock.getZone()).thenReturn(ZoneId.systemDefault());
    }

    @Test
    public void testScheduleReportGenerationDaily() throws DbException, EmailException {
        final long timeInMillis = new GregorianCalendar(2018, Calendar.FEBRUARY, 5).getTimeInMillis();
        Mockito.when(clock.millis()).thenReturn(timeInMillis);
        Mockito.when(clock.instant()).thenReturn(Instant.ofEpochMilli(timeInMillis));
        scheduler = new Scheduler(reportsGenerator, scheduleDAO, generationTime, clock, mockTimer);
        scheduler.addSchedule(dailyScheduleDto.getScheduleInfo());
        mockTimer.assertFirstGenerationTime(LocalDateTime.of(2018, Month.FEBRUARY, 6, generationTime, 0), 0);
    }

    @Test
    public void testScheduleReportGenerationWeeklyGenerateThisWeek()
                    throws DbException, EmailException {
        final long timeInMillis = new GregorianCalendar(2018, Calendar.FEBRUARY, 5).getTimeInMillis();
        Mockito.when(clock.millis()).thenReturn(timeInMillis);
        Mockito.when(clock.instant()).thenReturn(Instant.ofEpochMilli(timeInMillis));
        scheduler = new Scheduler(reportsGenerator, scheduleDAO, generationTime, clock, mockTimer);
        scheduler.addSchedule(weeklyScheduleDto.getScheduleInfo());
        mockTimer.assertFirstGenerationTime(LocalDateTime.of(2018, Month.FEBRUARY, 7, generationTime, 0), 0);
    }

    @Test
    public void testScheduleReportGenerationWeeklyGenerateNextWeek()
                    throws DbException, EmailException {
        final long timeInMillis = new GregorianCalendar(2018, Calendar.FEBRUARY, 9).getTimeInMillis();
        Mockito.when(clock.millis()).thenReturn(timeInMillis);
        Mockito.when(clock.instant()).thenReturn(Instant.ofEpochMilli(timeInMillis));
        scheduler = new Scheduler(reportsGenerator, scheduleDAO, generationTime, clock, mockTimer);
        scheduler.addSchedule(weeklyScheduleDto.getScheduleInfo());
        mockTimer.assertFirstGenerationTime(LocalDateTime.of(2018, Month.FEBRUARY, 14, generationTime, 0), 0);
    }

    @Test
    public void testScheduleReportGenerationMonthlyGenerateThisMonth()
                    throws DbException, EmailException {
        final long timeInMillis = new GregorianCalendar(2018, Calendar.FEBRUARY, 5).getTimeInMillis();
        Mockito.when(clock.millis()).thenReturn(timeInMillis);
        Mockito.when(clock.instant()).thenReturn(Instant.ofEpochMilli(timeInMillis));
        scheduler = new Scheduler(reportsGenerator, scheduleDAO, generationTime, clock, mockTimer);
        scheduler.addSchedule(monthlyScheduleDto.getScheduleInfo());
        final LocalDateTime nextGeneratingTime = LocalDateTime.of(2018, Month.FEBRUARY, 7, generationTime, 0);
        mockTimer.assertFirstGenerationTime(nextGeneratingTime, 0);
        mockTimer.assertFirstGenerationTime(nextGeneratingTime.plusMonths(1), 1);
    }

    @Test
    public void testScheduleReportGenerationMonthlyGenerateNextMonth()
                    throws DbException, EmailException {
        final long timeInMillis = new GregorianCalendar(2018, Calendar.FEBRUARY, 9).getTimeInMillis();
        Mockito.when(clock.millis()).thenReturn(timeInMillis);
        Mockito.when(clock.instant()).thenReturn(Instant.ofEpochMilli(timeInMillis));
        scheduler = new Scheduler(reportsGenerator, scheduleDAO, generationTime, clock, mockTimer);
        scheduler.addSchedule(monthlyScheduleDto.getScheduleInfo());
        final LocalDateTime nextGeneratingTime = LocalDateTime.of(2018, Month.MARCH, 7, generationTime, 0);
        mockTimer.assertFirstGenerationTime(nextGeneratingTime, 0);
        mockTimer.assertFirstGenerationTime(nextGeneratingTime.plusMonths(1), 1);
    }

    @Test
    public void testScheduleReportGenerationMonthlyGenerateNextYear()
                    throws DbException, EmailException {
        long timeInMillis = new GregorianCalendar(2018, Calendar.DECEMBER, 9).getTimeInMillis();
        Mockito.when(clock.millis()).thenReturn(timeInMillis);
        Mockito.when(clock.instant()).thenReturn(Instant.ofEpochMilli(timeInMillis));
        scheduler = new Scheduler(reportsGenerator, scheduleDAO, generationTime, clock, mockTimer);
        scheduler.addSchedule(monthlyScheduleDto.getScheduleInfo());
        final LocalDateTime nextGeneratingTime = LocalDateTime.of(2019, Month.JANUARY, 7, generationTime, 0);
        mockTimer.assertFirstGenerationTime(nextGeneratingTime, 0);
        mockTimer.assertFirstGenerationTime(nextGeneratingTime.plusMonths(1), 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testScheduleReportGenerationIllegalPeriod() throws DbException, EmailException {
        long timeInMillis = new GregorianCalendar(2018, Calendar.DECEMBER, 9).getTimeInMillis();
        Mockito.when(clock.millis()).thenReturn(timeInMillis);
        Mockito.when(clock.instant()).thenReturn(Instant.ofEpochMilli(timeInMillis));
        scheduler = new Scheduler(reportsGenerator, scheduleDAO, generationTime, clock, mockTimer);
        scheduler.addSchedule(illegalPeriodDto.getScheduleInfo());
    }

    @Nonnull
    private Reporting.ScheduleDTO buildScheduleDto(int templateId, @Nonnull String period, @Nonnull String format,
                    @Nonnull String dayOfWeek, int dayOfMonth) {
        return Reporting.ScheduleDTO.newBuilder().setId(1l).setScheduleInfo(
                        Reporting.ScheduleInfo.newBuilder()
                                        .setTemplateId(templateId)
                                        .setPeriod(period)
                                        .setFormat(format)
                                        .setDayOfWeek(dayOfWeek)
                                        .setDayOfMonth(dayOfMonth)
                                        .setReportType(1)
                                        .setShowCharts(true)
                                        .build())
                        .build();
    }

    private static class MockTimer extends Timer {

        private List<Long> scheduledTimes = new ArrayList<>();

        private final Clock clock;

        public MockTimer(@Nonnull Clock clock) {
            this.clock = clock;
        }

        @Override
        public int purge() {
            return 0;
        }

        @Override
        public void cancel() {

        }

        @Override
        public void schedule(@Nonnull TimerTask task, @Nonnull long delay) {
            scheduledTimes.add(delay);
        }

        @Override
        public void schedule(@Nonnull TimerTask task, long delay, long period) {
            scheduledTimes.add(delay);
        }

        public void assertFirstGenerationTime(@Nonnull LocalDateTime expected, int generationNumber) {
            final long nextTime = scheduledTimes.get(generationNumber);
            Assert.assertEquals(expected.atZone(clock.getZone()).toInstant().toEpochMilli(), nextTime);
        }
    }
}