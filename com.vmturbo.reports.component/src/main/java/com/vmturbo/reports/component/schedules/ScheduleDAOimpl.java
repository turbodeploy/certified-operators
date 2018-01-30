package com.vmturbo.reports.component.schedules;

import static com.vmturbo.reports.component.db.tables.Schedule.SCHEDULE;
import static com.vmturbo.reports.component.db.tables.ScheduleSubscribers.SCHEDULE_SUBSCRIBERS;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.reporting.api.protobuf.Reporting;
import com.vmturbo.reports.component.db.tables.pojos.Schedule;
import com.vmturbo.reports.component.db.tables.records.ScheduleRecord;
import com.vmturbo.reports.component.db.tables.records.ScheduleSubscribersRecord;
import com.vmturbo.sql.utils.DbException;

/**
 * Implementation of data access object for schedules.
 */
@NotThreadSafe
public class ScheduleDAOimpl implements ScheduleDAO {

    private final Logger logger = LogManager.getLogger();

    private final DSLContext context;

    public ScheduleDAOimpl(@Nonnull DSLContext context) {
        this.context = Objects.requireNonNull(context);
    }

    /**
     * Store new schedule to db.
     *
     * @param scheduleInfo params of schedule
     * @return stored schedule object
     */
    @Nonnull
    @Override
    public Reporting.ScheduleDTO addSchedule(@Nonnull Reporting.ScheduleInfo scheduleInfo)
                    throws DbException {
        final long scheduleId = IdentityGenerator.next();
        final Schedule schedule = new Schedule(
                scheduleId, scheduleInfo.getDayOfWeek(),
                scheduleInfo.getFormat(),
                scheduleInfo.getPeriod(),
                (short) scheduleInfo.getReportType(),
                scheduleInfo.getScopeOid(),
                (byte) (scheduleInfo.getShowCharts() ? 1 : 0),
                scheduleInfo.getTemplateId());
        try {
            storeSchedule(scheduleInfo, schedule);
        } catch (DataAccessException e) {
            final String message = String.format("Failed to store reporting schedule %s", schedule);
            throw new DbException(message, e);
        }
        return Reporting.ScheduleDTO.newBuilder().setId(scheduleId).setScheduleInfo(scheduleInfo).build();
    }

    private void storeSchedule(@Nonnull Reporting.ScheduleInfo scheduleInfo, Schedule schedule) {
        logger.debug("Storing schedule {}...", schedule);
        context.newRecord(SCHEDULE, schedule).store();
        final long scheduleId = schedule.getId();
        for (String email : scheduleInfo.getSubscribersEmailsList()) {
            context.insertInto(SCHEDULE_SUBSCRIBERS, SCHEDULE_SUBSCRIBERS.SCHEDULE_ID,
                    SCHEDULE_SUBSCRIBERS.EMAIL_ADDRESS)
                    .values(scheduleId, email).execute();
        }
        logger.debug("Schedule {} stored to db", schedule);
    }

    /**
     * Lists all stored schedules.
     *
     * @return all schedules stored in db.
     */
    @Nonnull
    @Override
    public Set<Reporting.ScheduleDTO> getAllSchedules() throws DbException {
        logger.debug("Reading all schedules from db...");
        try {
            return context.selectFrom(SCHEDULE).fetch().stream()
                            .map(this::toScheduleDTO)
                            .collect(Collectors.toSet());
        } catch (DataAccessException e) {
            final String message = "Failed to get all reporting schedules.";
            throw new DbException(message, e);
        }
    }

    /**
     * Delete schedule from db by id.
     *
     * @param id of schedule to delete
     */
    @Override
    public void deleteSchedule(long id) throws DbException {
        logger.debug("Removing of schedule with id {} from db", id);
        /* schedule_subcribers table has ON DELETE CASCADE property for schedule_id foreign key,
        *  so, we need only delete from 'schedule' table. */
        try {
            context.deleteFrom(SCHEDULE).where(SCHEDULE.ID.eq(id)).execute();
        } catch (DataAccessException e) {
            final String message = String.format("Failed to delete reporting schedule with id = %d", id);
            throw new DbException(message, e);
        }
    }

    /**
     * Edit stored schedule.
     *
     * @param scheduleDTO with schedule id to edit and new params.
     */
    @Override
    public void editSchedule(@Nonnull Reporting.ScheduleDTO scheduleDTO) throws DbException {
        logger.debug("Editing of schedule {}", scheduleDTO);
        final Reporting.ScheduleInfo scheduleInfo = scheduleDTO.getScheduleInfo();
        try {
            context.update(SCHEDULE)
                            .set(SCHEDULE.REPORT_TYPE, (short)scheduleInfo.getReportType())
                            .set(SCHEDULE.TEMPLATE_ID, scheduleInfo.getTemplateId())
                            .set(SCHEDULE.PERIOD, scheduleInfo.getPeriod())
                            .set(SCHEDULE.DAY_OF_WEEK, scheduleInfo.getDayOfWeek())
                            .set(SCHEDULE.FORMAT, scheduleInfo.getFormat())
                            .set(SCHEDULE.SHOW_CHARTS, (byte)(scheduleInfo.getShowCharts() ? 1 : 0))
                            .set(SCHEDULE.SCOPE_OID, scheduleInfo.getScopeOid())
                        .where(SCHEDULE.ID.eq(scheduleDTO.getId()))
                        .execute();
            /* As subscribers may be changed different ways we need to rewrite
             subcribers for edited schedule. */
            context.deleteFrom(SCHEDULE_SUBSCRIBERS).where(SCHEDULE_SUBSCRIBERS.SCHEDULE_ID
                            .eq(scheduleDTO.getId())).execute();
            final List<String> emails = scheduleInfo.getSubscribersEmailsList();
            emails.forEach(email -> context.insertInto(SCHEDULE_SUBSCRIBERS,
                            SCHEDULE_SUBSCRIBERS.SCHEDULE_ID, SCHEDULE_SUBSCRIBERS.EMAIL_ADDRESS)
                            .values(scheduleDTO.getId(), email).execute());
        } catch (DataAccessException e) {
            final String message = String.format("Failed to edit reporting schedule with id: %d",
                            scheduleDTO.getId());
            throw new DbException(message, e);
        }
    }

    /**
     * Returns stored schedule by id from db.
     *
     * @param id of schedule to return
     * @return schedule with specified id
     */
    @Nonnull
    @Override
    public Reporting.ScheduleDTO getSchedule(long id) throws DbException {
        logger.debug("Reading of schedule with id {} from db...", id);
        try {
            final ScheduleRecord record = context.selectFrom(SCHEDULE)
                            .where(SCHEDULE.ID.eq(id)).iterator().next();
            return record == null
                            ? Reporting.ScheduleDTO.newBuilder().build()
                            : toScheduleDTO(record);
        } catch (DataAccessException e) {
            final String message = String.format("Failed to get schedule with id %d", id);
            throw new DbException(message);
        }
    }

    /**
     * Returns schedules with specified report type and template id.
     *
     * @param reportType specified
     * @param templateId specified
     * @return schedule with specified reportType and templateId
     */
    @Nonnull
    @Override
    public Set<Reporting.ScheduleDTO> getSchedulesBy(int reportType, int templateId)
                    throws DbException {
        logger.debug("Reading of schedules with reportType {} and templateId {}",
                        reportType, templateId);
        try {
            return context.selectFrom(SCHEDULE).where(SCHEDULE.REPORT_TYPE.eq((short)reportType)
                            .and(SCHEDULE.TEMPLATE_ID.eq(templateId))).fetch()
                            .stream()
                            .map(this::toScheduleDTO)
                            .collect(Collectors.toSet());
        } catch (DataAccessException e) {
            final String message = String.format("Failed to get schedules with reportType %d and templateId %d",
                            reportType, templateId);
            throw new DbException(message, e);
        }
    }

    @Nonnull
    private Reporting.ScheduleDTO toScheduleDTO(@Nonnull ScheduleRecord scheduleRecord) {
        final List<String> subscribers = context.selectFrom(SCHEDULE_SUBSCRIBERS)
                .where(SCHEDULE_SUBSCRIBERS.SCHEDULE_ID.eq(scheduleRecord.getId()))
                .fetch().stream().map(ScheduleSubscribersRecord::getEmailAddress)
                .collect(Collectors.toList());
        final Reporting.ScheduleInfo info = toScheduleInfo(scheduleRecord, subscribers);
        return Reporting.ScheduleDTO.newBuilder()
                .setId(scheduleRecord.getId())
                .setScheduleInfo(info)
                .build();
    }

    private Reporting.ScheduleInfo toScheduleInfo(@Nonnull ScheduleRecord scheduleRecord,
                                                  @Nonnull List<String> subscribers) {
        return Reporting.ScheduleInfo.newBuilder()
                .addAllSubscribersEmails(subscribers)
                .setReportType(scheduleRecord.getReportType())
                .setTemplateId(scheduleRecord.getTemplateId())
                .setDayOfWeek(scheduleRecord.getDayOfWeek())
                .setPeriod(scheduleRecord.getPeriod())
                .setFormat(scheduleRecord.getFormat())
                .setScopeOid(scheduleRecord.getScopeOid())
                .setShowCharts(scheduleRecord.getShowCharts() > 0).build();
    }
}
