package com.vmturbo.reports.component.schedules;

import java.time.Clock;
import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.TextStyle;
import java.time.temporal.TemporalAdjusters;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.mail.EmailException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.enums.Period;
import com.vmturbo.reporting.api.protobuf.Reporting;
import com.vmturbo.reports.component.EmailAddressValidator;
import com.vmturbo.reports.component.ReportingException;
import com.vmturbo.reports.component.instances.ReportsGenerator;
import com.vmturbo.sql.utils.DbException;

/**
 * Class for scheduling of reports generating.
 */
@ThreadSafe
public class Scheduler implements AutoCloseable {

    private final Object lock = new Object();

    private final Logger logger = LogManager.getLogger();

    private final ReportsGenerator reportsGenerator;

    private final ScheduleDAO scheduleDAO;

    private final int scheduledReportsGenerationTime;

    private final Clock clock;

    private static final Map<String, DayOfWeek> DAYS_OF_WEEK;

    private static final Function<LocalDateTime, Integer> lastDateOfTheMonthFun =
        (LocalDateTime dateTime) -> dateTime.toLocalDate().with(TemporalAdjusters.lastDayOfMonth()).getDayOfMonth();

    static {
        final ImmutableMap.Builder<String, DayOfWeek> mapBuilder = ImmutableMap.builder();
        for (DayOfWeek day : DayOfWeek.values()) {
            mapBuilder.put(day.getDisplayName(TextStyle.SHORT, Locale.getDefault()), day);
        }
        DAYS_OF_WEEK = mapBuilder.build();
    }

    private final Timer timer;

    /**
     * General consturctor of scheduler.
     *
     * @param reportsGenerator to generate reports
     * @param scheduleDAO to get current schedules
     * @param timer should be used only by scheduler as it may purged from time to time
     */
    public Scheduler(@Nonnull ReportsGenerator reportsGenerator, @Nonnull ScheduleDAO scheduleDAO,
                    int scheduledReportsGenerationTime, @Nonnull Clock clock, @Nonnull Timer timer) {
        if (scheduledReportsGenerationTime > 24 || scheduledReportsGenerationTime < 0) {
            throw new IllegalArgumentException("Illegal reports generation time: " + scheduledReportsGenerationTime);
        }
        this.scheduledReportsGenerationTime = scheduledReportsGenerationTime;
        this.reportsGenerator = Objects.requireNonNull(reportsGenerator);
        this.scheduleDAO = Objects.requireNonNull(scheduleDAO);
        this.clock = Objects.requireNonNull(clock);
        this.timer = Objects.requireNonNull(timer);
    }

    /**
     * Schedules generation of all scheduled reports stored in db.
     */
    public void scheduleAllReportsGeneration() {
        synchronized (lock) {
            timer.purge();
            try {
                final Set<Reporting.ScheduleDTO> allSchedules = scheduleDAO.getAllSchedules();
                allSchedules.forEach(this::scheduleReportGeneration);
            } catch (DbException e) {
                logger.error("Failed to get reporting schedules from db", e);
            }
        }
    }


    @Override
    public void close() {
        timer.cancel();
    }

    @Nonnull
    public Reporting.ScheduleDTO addSchedule(@Nonnull Reporting.ScheduleInfo scheduleInfo)
                    throws DbException, EmailException {
        EmailAddressValidator.validateAddresses(
                scheduleInfo.getReportRequest().getSubscribersEmailsList());
        synchronized (lock) {
            final Reporting.ScheduleDTO scheduleDTO = scheduleDAO.addSchedule(scheduleInfo);
            scheduleReportGeneration(scheduleDTO);
            return scheduleDTO;
        }
    }

    @Nonnull
    public Set<Reporting.ScheduleDTO> getAllSchedules() throws DbException {
        return scheduleDAO.getAllSchedules();
    }

    public void deleteSchedule(long id) throws DbException {
        synchronized (lock) {
            scheduleDAO.deleteSchedule(id);
            scheduleAllReportsGeneration();
        }
    }

    @Nonnull
    public void editSchedule(@Nonnull Reporting.ScheduleDTO scheduleDTO)
                    throws DbException, EmailException {
        EmailAddressValidator.validateAddresses(
                scheduleDTO.getScheduleInfo().getReportRequest().getSubscribersEmailsList());
        synchronized (lock) {
            scheduleDAO.editSchedule(scheduleDTO);
            scheduleAllReportsGeneration();
        }
    }

    @Nonnull
    public Reporting.ScheduleDTO getSchedule(long id) throws DbException {
        return scheduleDAO.getSchedule(id);
    }

    @Nonnull
    public Set<Reporting.ScheduleDTO> getSchedulesBy(int reportType, int templateId)
                    throws DbException {
        return scheduleDAO.getSchedulesBy(reportType, templateId);
    }

    /**
     * Schedules generation of report.
     *
     * @param scheduleDTO scheduled report to be generated
     * @return time when report will be generated first time
     */
    private LocalDateTime scheduleReportGeneration(@Nonnull Reporting.ScheduleDTO scheduleDTO) {
        final Reporting.ScheduleInfo info = scheduleDTO.getScheduleInfo();
        final Reporting.GenerateReportRequest request = info.getReportRequest();
        final ScheduleTask scheduleTask = new ScheduleTask(reportsGenerator, request);
        // by default scheduledReportsGenerationTime is 1 (1 AM of the day)
        final LocalDateTime dateTime = LocalDateTime.now(clock)
                        .withHour(scheduledReportsGenerationTime)
                        .withMinute(0)
                        .withSecond(0);
        final String period = info.getPeriod();
        final LocalDateTime firstGeneratingTime;
        if (period.equals(Period.Daily.getName())) {
            firstGeneratingTime = scheduleDaily(scheduleTask, dateTime);
        } else if (period.equals(Period.Weekly.getName())) {
            firstGeneratingTime = scheduleWeekly(info, scheduleTask, dateTime);
        } else if (period.equals(Period.Monthly.getName())) {
            firstGeneratingTime = scheduleMonthly(info, scheduleTask, dateTime);
        } else {
            throw new IllegalArgumentException(info.getPeriod() + " period is not supported by scheduling");
        }
        return firstGeneratingTime;
    }

    @Nonnull
    private LocalDateTime scheduleMonthly(@Nonnull Reporting.ScheduleInfo info, @Nonnull ScheduleTask scheduleTask,
                                          @Nonnull LocalDateTime currentDate) {
        final LocalDateTime firstGeneratingTime;
        // the time user set to generate report on every month. It will always be current time for current UI,
        // since selecting time is not enabled in UI.
        final int reportDayOfMonth = info.getDayOfMonth();
        // if the user chosen time is in past, schedule to next month; otherwise this month
        if (currentDate.getDayOfMonth() >= reportDayOfMonth) {
            final LocalDateTime currentDateInNextMonth = currentDate.plusMonths(1);
            final int lastDayInNextMonth = lastDateOfTheMonthFun.apply(currentDateInNextMonth);
            firstGeneratingTime = (reportDayOfMonth <= lastDayInNextMonth) ?
                currentDateInNextMonth.withDayOfMonth(reportDayOfMonth) :
                currentDateInNextMonth.withDayOfMonth(lastDayInNextMonth);
        } else {
            final int lastDayInCurrentMonth = lastDateOfTheMonthFun.apply(currentDate);
            firstGeneratingTime = (reportDayOfMonth <= lastDayInCurrentMonth) ?
                currentDate.withDayOfMonth(reportDayOfMonth) : currentDate.withDayOfMonth(lastDayInCurrentMonth);
        }
        // Delay is in milliseconds before task is to be executed: delay = (firstGeneratingTime in epoch milli)
        // - (currentDate in epoch milli)
        final long delay = firstGeneratingTime.atZone(clock.getZone()).toInstant().toEpochMilli()
            - LocalDateTime.now(clock).atZone(clock.getZone()).toInstant().toEpochMilli();
        scheduleMonthlyTask(scheduleTask, firstGeneratingTime, delay, reportDayOfMonth);
        return firstGeneratingTime;
    }

    /**
     * Recursive function to:
     * 1. execute the task. For the initial delay, it will be (time to execute - current time); for
     * subsequent delay, it will be 0. Since it's normalized to time in month already.
     * Note: for currently UI, the initial dealy will always be 0 too. Because UI doesn't allow choose
     * a date in a month, so the default is current time.
     * 2. calculate the date in next month, and schedule same task to be executed on that date
     * @param task           task to execute report generation
     * @param timeToExecute  on which date (YYYY-MM-DD-SS) to execute the task
     * @param delay          delay in Milli to execute the task
     * @param reportDayOfMonth the date report will be generated on the month
     */
    private void scheduleMonthlyTask(@Nonnull ScheduleTask task, @Nonnull LocalDateTime timeToExecute,
                                     final long delay, final int reportDayOfMonth) {
        // 1. execute the task
        timer.schedule(task, delay);

        // 2. calculate the day in next month
        final LocalDateTime timeInNextMonth = timeToExecute.plusMonths(1);
        final int lastDayInNextMonth = lastDateOfTheMonthFun.apply(timeInNextMonth);
        // if next monday doesn't have the date, use the last date of next month.
        // e.g. if user chooses 31th, and next month is April, set it to April 30th.
        final LocalDateTime sameTimeInNextMonth = (reportDayOfMonth <= lastDayInNextMonth) ?
            timeInNextMonth.withDayOfMonth(reportDayOfMonth) : timeInNextMonth.withDayOfMonth(lastDayInNextMonth);

        final long sameTimeInNextMonthEpochMilli = sameTimeInNextMonth.atZone(clock.getZone()).toInstant().toEpochMilli();
        final long timeToExecuteEpochMilli = timeToExecute.atZone(clock.getZone()).toInstant().toEpochMilli();
        final long delayToSameDateOfNextMonth = sameTimeInNextMonthEpochMilli - timeToExecuteEpochMilli;
        final TimerTask nextTask = new TimerTask() {
            @Override
            public void run() {
                // subsequent task will always have zero delay, since we already know which time to execute on a month
                scheduleMonthlyTask(task, sameTimeInNextMonth, 0L, reportDayOfMonth);
            }
        };
        // schedule task to the same date of next month
        timer.schedule(nextTask, delayToSameDateOfNextMonth);
    }

    @Nonnull
    private LocalDateTime scheduleWeekly(@Nonnull Reporting.ScheduleInfo info, @Nonnull ScheduleTask scheduleTask,
                    @Nonnull LocalDateTime currentDate) {
        final LocalDateTime firstGeneratingTime;
        final DayOfWeek dayOfWeek = DAYS_OF_WEEK.get(info.getDayOfWeek());
        if (dayOfWeek == null) {
            logger.warn("Cannot resolve day of week {} for scheduling", info.getDayOfWeek());
            return LocalDateTime.MIN;
        }
        firstGeneratingTime = currentDate.with(TemporalAdjusters.next(dayOfWeek));
        final long delay = firstGeneratingTime.atZone(clock.getZone()).toInstant().toEpochMilli()
            - LocalDateTime.now(clock).atZone(clock.getZone()).toInstant().toEpochMilli();
        timer.schedule(scheduleTask, delay, TimeUnit.DAYS.toMillis(7));
        return firstGeneratingTime;
    }

    @Nonnull
    private LocalDateTime scheduleDaily(@Nonnull ScheduleTask scheduleTask, @Nonnull LocalDateTime currentDate) {
        final LocalDateTime firstGeneratingTime = currentDate.plusDays(1);
        final long delay = firstGeneratingTime.atZone(clock.getZone()).toInstant().toEpochMilli()
            - LocalDateTime.now(clock).atZone(clock.getZone()).toInstant().toEpochMilli();
        timer.schedule(scheduleTask, delay, TimeUnit.DAYS.toMillis(1));
        return firstGeneratingTime;
    }

    /**
     * For test only: verifying the report generation will actually get called with real scheduler and timer.
     */
    @VisibleForTesting
    @Nonnull
    void schedule(@Nonnull ScheduleTask scheduleTask, final long delay) {
        timer.schedule(scheduleTask, delay, TimeUnit.DAYS.toMillis(1));
    }

    /**
     * Task to generate one certain scheduled report.
     */
    @VisibleForTesting
    static class ScheduleTask extends TimerTask {

        private final Logger logger = LogManager.getLogger();

        private final ReportsGenerator reportsGenerator;

        private final Reporting.GenerateReportRequest request;

        @VisibleForTesting
        ScheduleTask(@Nonnull ReportsGenerator reportsGenerator,
                        @Nonnull Reporting.GenerateReportRequest request) {
            this.reportsGenerator = reportsGenerator;
            this.request = request;
        }

        /**
         * Run generating report.
         */
        @Override
        public void run() {
            try {
                reportsGenerator.generateReport(request);
            } catch (DbException | ReportingException e) {
                logger.error("Failed to generate scheduled report " + request, e);
            } catch (EmailException ex) {
                logger.error("Invalid address provided for scheduled report generation " +
                                request.getSubscribersEmailsList(), ex);
            }
        }
    }
}