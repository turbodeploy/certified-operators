package com.vmturbo.reports.component.schedules;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import javax.annotation.Nonnull;

import org.apache.commons.mail.EmailException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.reporting.api.protobuf.Reporting;
import com.vmturbo.reporting.api.protobuf.Reporting.GenerateReportRequest;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplateId;
import com.vmturbo.reports.component.ReportingException;
import com.vmturbo.reports.component.instances.ReportsGenerator;
import com.vmturbo.reports.component.schedules.Scheduler.ScheduleTask;
import com.vmturbo.sql.utils.DbException;

/**
 * Tests for scheduling of reports. ReportsGenerator is mocked so it doesn't generate reports really
 * but it calculates when report should be generated next time and tests it.
 */
public class SchedulerTest {

    public static final String DAILY = "Daily";
    public static final String WEEKLY = "Weekly";
    public static final String MONTHLY = "Monthly";

    private final Clock clock = mock(Clock.class);

    private ScheduleDAO scheduleDAO;

    private Scheduler scheduler;

    private ReportsGenerator reportsGenerator;

    private final int generationTime = 1;

    private final long generationTimeInMillis = generationTime * 60 * 60 * 1000;

    private final MockTimer mockTimer = new MockTimer(clock);

    private final Reporting.ScheduleDTO dailyScheduleDto =
                    buildScheduleDto(19, "Daily", "PDF", "", 0);

    private final Reporting.ScheduleDTO weeklyScheduleDto =
                    buildScheduleDto(19, "Weekly", "PDF", "Wed", 0);

    private final Reporting.ScheduleDTO monthlyScheduleDto =
                    buildScheduleDto(19, "Monthly", "PDF", "", 7);

    private final Reporting.ScheduleDTO endOfTheMonthMonthlyScheduleDto =
        buildScheduleDto(19, "Monthly", "PDF", "", 31);

    private final Reporting.ScheduleDTO illegalPeriodDto = buildScheduleDto(19, "Illegal", "PDF",
                    "Mon", 4);

    @Before
    public void init() throws DbException {
        scheduleDAO = mock(ScheduleDAO.class);
        Mockito.when(scheduleDAO.addSchedule(dailyScheduleDto.getScheduleInfo())).thenReturn(dailyScheduleDto);
        Mockito.when(scheduleDAO.addSchedule(weeklyScheduleDto.getScheduleInfo())).thenReturn(weeklyScheduleDto);
        Mockito.when(scheduleDAO.addSchedule(monthlyScheduleDto.getScheduleInfo())).thenReturn(monthlyScheduleDto);
        Mockito.when(scheduleDAO.addSchedule(endOfTheMonthMonthlyScheduleDto.getScheduleInfo())).thenReturn(endOfTheMonthMonthlyScheduleDto);
        Mockito.when(scheduleDAO.addSchedule(illegalPeriodDto.getScheduleInfo())).thenReturn(illegalPeriodDto);
        reportsGenerator = mock(ReportsGenerator.class);
        Mockito.when(clock.getZone()).thenReturn(ZoneId.systemDefault());
    }

    @Test
    public void testScheduleReportGenerationDaily() throws DbException, EmailException {
        final long timeInMillis = new GregorianCalendar(2018, Calendar.FEBRUARY, 5).getTimeInMillis();
        Mockito.when(clock.millis()).thenReturn(timeInMillis);
        Mockito.when(clock.instant()).thenReturn(Instant.ofEpochMilli(timeInMillis));
        scheduler = new Scheduler(reportsGenerator, scheduleDAO, generationTime, clock, mockTimer);
        scheduler.addSchedule(dailyScheduleDto.getScheduleInfo());
        final long delay = LocalDateTime.of(2018, Month.FEBRUARY, 6, generationTime, 0)
            .atZone(clock.getZone()).toInstant().toEpochMilli() - timeInMillis;
        mockTimer.assertDelayTime(delay, 0);
    }

    @Test
    public void testScheduleReportGenerationWeeklyGenerateThisWeek()
                    throws DbException, EmailException {
        final long timeInMillis = new GregorianCalendar(2018, Calendar.FEBRUARY, 5).getTimeInMillis();
        Mockito.when(clock.millis()).thenReturn(timeInMillis);
        Mockito.when(clock.instant()).thenReturn(Instant.ofEpochMilli(timeInMillis));
        scheduler = new Scheduler(reportsGenerator, scheduleDAO, generationTime, clock, mockTimer);
        scheduler.addSchedule(weeklyScheduleDto.getScheduleInfo());
        final long delay = LocalDateTime.of(2018, Month.FEBRUARY, 7, generationTime, 0)
            .atZone(clock.getZone()).toInstant().toEpochMilli() - timeInMillis;
        mockTimer.assertDelayTime(delay, 0);
    }

    @Test
    public void testScheduleReportGenerationWeeklyGenerateNextWeek()
                    throws DbException, EmailException {
        final long timeInMillis = new GregorianCalendar(2018, Calendar.FEBRUARY, 9).getTimeInMillis();
        Mockito.when(clock.millis()).thenReturn(timeInMillis);
        Mockito.when(clock.instant()).thenReturn(Instant.ofEpochMilli(timeInMillis));
        scheduler = new Scheduler(reportsGenerator, scheduleDAO, generationTime, clock, mockTimer);
        scheduler.addSchedule(weeklyScheduleDto.getScheduleInfo());
        final long delay = LocalDateTime.of(2018, Month.FEBRUARY, 14, generationTime, 0)
            .atZone(clock.getZone()).toInstant().toEpochMilli() - timeInMillis;
        mockTimer.assertDelayTime(delay, 0);
    }

    @Test
    public void testScheduleReportGenerationMonthlyGenerateThisMonth()
                    throws DbException, EmailException {
        final long timeInMillis = new GregorianCalendar(2018, Calendar.FEBRUARY, 5).getTimeInMillis();
        Mockito.when(clock.millis()).thenReturn(timeInMillis);
        Mockito.when(clock.instant()).thenReturn(Instant.ofEpochMilli(timeInMillis));
        scheduler = new Scheduler(reportsGenerator, scheduleDAO, generationTime, clock, mockTimer);
        // current date is 5th with time, scheduled to 7th with same time
        scheduler.addSchedule(monthlyScheduleDto.getScheduleInfo());
        // get the date of 7th with same time
        final LocalDateTime firstGeneratingTime = LocalDateTime.of(2018, Month.FEBRUARY, 7, generationTime, 0);
        // first delay is 7th with same time - current time - generation time
        final long firstDelay = firstGeneratingTime.atZone(clock.getZone()).toInstant()
            .toEpochMilli() - timeInMillis;
        // second delay is 7th with same time on next month (March) -  7th with same time on this month (February)
        final long secondDelay = firstGeneratingTime.plusMonths(1).atZone(clock.getZone()).toInstant()
            .toEpochMilli() - firstGeneratingTime.atZone(clock.getZone()).toInstant()
            .toEpochMilli();
        mockTimer.assertDelayTime(firstDelay, 0);
        mockTimer.assertDelayTime(secondDelay, 1);
    }

    @Test
    public void testScheduleReportGenerationMonthlyGenerateThisMonthWithDateSetTo31()
        throws DbException, EmailException {
        final long timeInMillis = new GregorianCalendar(2018, Calendar.FEBRUARY, 5).getTimeInMillis();
        Mockito.when(clock.millis()).thenReturn(timeInMillis);
        Mockito.when(clock.instant()).thenReturn(Instant.ofEpochMilli(timeInMillis));
        scheduler = new Scheduler(reportsGenerator, scheduleDAO, generationTime, clock, mockTimer);
        // current date is 5th with time, scheduled to 7th with same time
        scheduler.addSchedule(endOfTheMonthMonthlyScheduleDto.getScheduleInfo());
        // get the date of 7th with same time
        final LocalDateTime firstGeneratingTime = LocalDateTime.of(2018, Month.FEBRUARY, 28, generationTime, 0);
        // first delay is 7th with same time - current time - generation time
        final long firstDelay = firstGeneratingTime.atZone(clock.getZone()).toInstant()
            .toEpochMilli() - timeInMillis;
        // second delay is 7th with same time on next month (March) -  7th with same time on this month (February)
        final long secondDelay = firstGeneratingTime.plusDays(31).atZone(clock.getZone()).toInstant()
            .toEpochMilli() - firstGeneratingTime.atZone(clock.getZone()).toInstant().toEpochMilli();
        mockTimer.assertDelayTime(firstDelay, 0);
        mockTimer.assertDelayTime(secondDelay, 1);
    }

    @Test
    public void testScheduleReportGenerationMonthlyGenerateNextMonth()
        throws DbException, EmailException {
        final long timeInMillis = new GregorianCalendar(2018, Calendar.FEBRUARY, 9).getTimeInMillis();
        Mockito.when(clock.millis()).thenReturn(timeInMillis);
        Mockito.when(clock.instant()).thenReturn(Instant.ofEpochMilli(timeInMillis));
        scheduler = new Scheduler(reportsGenerator, scheduleDAO, generationTime, clock, mockTimer);
        // current date is 9th with time, scheduled to 7th with same time
        scheduler.addSchedule(monthlyScheduleDto.getScheduleInfo());
        // get the date of 7th with same time on next month
        final LocalDateTime nextGeneratingTime = LocalDateTime.of(2018, Month.MARCH, 7, generationTime, 0);
        // first delay is 7th with same time on next month - current time - generation time
        final long firstDelay = nextGeneratingTime.atZone(clock.getZone()).toInstant()
            .toEpochMilli() - timeInMillis;
        // second delay is 7th with same time on next next month (April) -  7th with same time on next month (March)
        final long secondDelay = nextGeneratingTime.plusMonths(1).atZone(clock.getZone()).toInstant()
            .toEpochMilli() - nextGeneratingTime.atZone(clock.getZone()).toInstant().toEpochMilli();

        mockTimer.assertDelayTime(firstDelay, 0);
        mockTimer.assertDelayTime(secondDelay, 1);
    }

    @Test
    public void testScheduleReportGenerationMonthlyGenerateNextMonthWithDateSetTo31()
        throws DbException, EmailException {
        final long timeInMillis = new GregorianCalendar(2018, Calendar.MARCH, 31).getTimeInMillis();
        Mockito.when(clock.millis()).thenReturn(timeInMillis);
        Mockito.when(clock.instant()).thenReturn(Instant.ofEpochMilli(timeInMillis));
        scheduler = new Scheduler(reportsGenerator, scheduleDAO, generationTime, clock, mockTimer);
        // current date is 9th with time, scheduled to 7th with same time
        scheduler.addSchedule(endOfTheMonthMonthlyScheduleDto.getScheduleInfo());
        // get the date of 7th with same time on next month
        final LocalDateTime nextGeneratingTime = LocalDateTime.of(2018, Month.APRIL, 30, generationTime, 0);
        // first delay is 7th with same time on next month - current time - generation time
        final long firstDelay = nextGeneratingTime.atZone(clock.getZone()).toInstant()
            .toEpochMilli() - timeInMillis;
        // second delay is 7th with same time on next next month (April) -  7th with same time on next month (March)
        final long secondDelay = nextGeneratingTime.plusDays(31).atZone(clock.getZone()).toInstant()
            .toEpochMilli() - nextGeneratingTime.atZone(clock.getZone()).toInstant().toEpochMilli();

        mockTimer.assertDelayTime(firstDelay, 0);
        mockTimer.assertDelayTime(secondDelay, 1);
    }

    @Test
    public void testScheduleReportGenerationMonthlyGenerateNextYear()
                    throws DbException, EmailException {
        long timeInMillis = new GregorianCalendar(2018, Calendar.DECEMBER, 9).getTimeInMillis();
        Mockito.when(clock.millis()).thenReturn(timeInMillis);
        Mockito.when(clock.instant()).thenReturn(Instant.ofEpochMilli(timeInMillis));
        scheduler = new Scheduler(reportsGenerator, scheduleDAO, generationTime, clock, mockTimer);
        // current date is 9th with time on December, scheduled to 7th with same time
        scheduler.addSchedule(monthlyScheduleDto.getScheduleInfo());
        // get the date of 7th with same time on next month (which is in next year)
        final LocalDateTime nextGeneratingTime = LocalDateTime.of(2019, Month.JANUARY, 7, generationTime, 0);
        // first delay is 7th with same time on next month - current time - generation time
        final long firstDelay = nextGeneratingTime.atZone(clock.getZone()).toInstant()
            .toEpochMilli() - timeInMillis;
        // second delay is 7th with same time on next next month (February) -  7th with same time on next month (January)
        final long secondDelay = nextGeneratingTime.plusMonths(1).atZone(clock.getZone()).toInstant()
            .toEpochMilli() - nextGeneratingTime.atZone(clock.getZone()).toInstant().toEpochMilli();
        mockTimer.assertDelayTime(firstDelay, 0);
        mockTimer.assertDelayTime(secondDelay, 1);
    }

    @Test
    public void testScheduleReportGenerationMonthlyGenerateNextYearWithDateSetTo31()
        throws DbException, EmailException {
        long timeInMillis = new GregorianCalendar(2018, Calendar.DECEMBER, 31).getTimeInMillis();
        Mockito.when(clock.millis()).thenReturn(timeInMillis);
        Mockito.when(clock.instant()).thenReturn(Instant.ofEpochMilli(timeInMillis));
        scheduler = new Scheduler(reportsGenerator, scheduleDAO, generationTime, clock, mockTimer);
        // current date is 9th with time on December, scheduled to 7th with same time
        scheduler.addSchedule(endOfTheMonthMonthlyScheduleDto.getScheduleInfo());
        // get the date of 7th with same time on next month (which is in next year)
        final LocalDateTime nextGeneratingTime = LocalDateTime.of(2019, Month.JANUARY, 31, generationTime, 0);
        // first delay is 7th with same time on next month - current time - generation time
        final long firstDelay = nextGeneratingTime.atZone(clock.getZone()).toInstant()
            .toEpochMilli() - timeInMillis;
        // second delay is 7th with same time on next next month (February) -  7th with same time on next month (January)
        final long secondDelay = nextGeneratingTime.plusDays(28).atZone(clock.getZone()).toInstant()
            .toEpochMilli() - nextGeneratingTime.atZone(clock.getZone()).toInstant().toEpochMilli();
        mockTimer.assertDelayTime(firstDelay, 0);
        mockTimer.assertDelayTime(secondDelay, 1);
    }

    /**
     * Verified the report generation will actually get called with real scheduler and timer.
     */
    @Ignore("Sometimes fails in CI build due to unstable environment")
    @Test
    public void testRealScheduleWillTriggerReportGeneration() throws ReportingException, InterruptedException, DbException, EmailException {
        final Scheduler realScheduler = new Scheduler(reportsGenerator, scheduleDAO, 0, Clock.systemDefaultZone(), new Timer());
        final ScheduleTask scheduleTask = new ScheduleTask(reportsGenerator, GenerateReportRequest.getDefaultInstance());
        realScheduler.schedule(scheduleTask, 0L);
        // TODO try avoiding Thread.sleep()
        Thread.sleep(2000L);
        verify(reportsGenerator).generateReport(any());
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
        final GenerateReportRequest request = GenerateReportRequest.newBuilder()
                .setFormat(format)
                .setTemplate(ReportTemplateId.newBuilder()
                        .setId(templateId)
                        .setReportType(1)
                        .build())
                .putParameters("show_charts", Boolean.toString(true))
                .build();
        return Reporting.ScheduleDTO.newBuilder().setId(1l).setScheduleInfo(
                        Reporting.ScheduleInfo.newBuilder()
                                        .setPeriod(period)
                                        .setDayOfWeek(dayOfWeek)
                                        .setDayOfMonth(dayOfMonth)
                                        .setReportRequest(request)
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

        public void assertDelayTime(final long delay, int generationNumber) {
            final long nextTime = scheduledTimes.get(generationNumber);
            Assert.assertEquals(delay, nextTime);
        }
    }
}