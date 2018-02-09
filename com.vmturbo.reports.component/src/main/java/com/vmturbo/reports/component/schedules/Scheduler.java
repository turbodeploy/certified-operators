package com.vmturbo.reports.component.schedules;

import java.time.Clock;
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

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.enums.Period;
import com.vmturbo.reporting.api.protobuf.Reporting;
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
                    throws DbException {
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
    public void editSchedule(@Nonnull Reporting.ScheduleDTO scheduleDTO) throws DbException {
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
        final Reporting.GenerateReportRequest request = Reporting.GenerateReportRequest.newBuilder()
                        .setFormat(info.getFormat())
                        .setTemplate(Reporting.ReportTemplateId.newBuilder()
                                        .setId(info.getTemplateId())
                                        .setReportType(info.getReportType()))
                        .build();
        final ScheduleTask scheduleTask = new ScheduleTask(reportsGenerator, request);
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
        final int reportDayOfMonth = info.getDayOfMonth();
        if (currentDate.getDayOfMonth() > reportDayOfMonth) {
            firstGeneratingTime = currentDate.plusMonths(1).withDayOfMonth(reportDayOfMonth);

        } else {
            firstGeneratingTime = currentDate.withDayOfMonth(reportDayOfMonth);
        }
        scheduleMonthlyTask(scheduleTask, firstGeneratingTime);
        return firstGeneratingTime;
    }

    private void scheduleMonthlyTask(@Nonnull ScheduleTask task, @Nonnull LocalDateTime timeToExecute) {
        timer.schedule(task, timeToExecute.atZone(clock.getZone()).toInstant().toEpochMilli());
        final TimerTask nextTask = new TimerTask() {
            @Override
            public void run() {
                scheduleMonthlyTask(task, timeToExecute);
            }
        };
        final int daysInMonth = timeToExecute.with(TemporalAdjusters.lastDayOfMonth()).getDayOfMonth();
        timer.schedule(nextTask, timeToExecute.plusDays(daysInMonth)
                        .atZone(clock.getZone()).toInstant().toEpochMilli());
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
        timer.schedule(scheduleTask, firstGeneratingTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli(),
                        TimeUnit.DAYS.toMillis(7));
        return firstGeneratingTime;
    }

    @Nonnull
    private LocalDateTime scheduleDaily(@Nonnull ScheduleTask scheduleTask, @Nonnull LocalDateTime currentDate) {
        final LocalDateTime firstGeneratingTime;
        firstGeneratingTime = currentDate.plusDays(1);
        timer.schedule(scheduleTask, firstGeneratingTime.atZone(clock.getZone()).toInstant().toEpochMilli(),
                        TimeUnit.DAYS.toMillis(1));
        return firstGeneratingTime;
    }

    /**
     * Task to generate one certain scheduled report.
     */
    private static class ScheduleTask extends TimerTask {

        private final Logger logger = LogManager.getLogger();

        private final ReportsGenerator reportsGenerator;

        private final Reporting.GenerateReportRequest request;

        private ScheduleTask(@Nonnull ReportsGenerator reportsGenerator,
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
            }
        }
    }
}