package com.vmturbo.reports.component.schedules;

import static com.vmturbo.reports.component.db.tables.Schedule.SCHEDULE;
import static com.vmturbo.reports.component.db.tables.ScheduleSubscribers.SCHEDULE_SUBSCRIBERS;
import static com.vmturbo.reports.component.db.tables.ScheduleParameters.SCHEDULE_PARAMETERS;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.api.enums.ReportType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.reporting.api.protobuf.Reporting;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplateId;
import com.vmturbo.reports.component.db.tables.pojos.Schedule;
import com.vmturbo.reports.component.db.tables.pojos.ScheduleParameters;
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
        final Schedule schedule = toPojoSchedule(scheduleInfo, scheduleId);
        try {
            storeSchedule(scheduleInfo, schedule);
        } catch (DataAccessException e) {
            final String message = String.format("Failed to store reporting schedule %s", schedule);
            throw new DbException(message, e);
        }
        return Reporting.ScheduleDTO.newBuilder().setId(scheduleId).setScheduleInfo(scheduleInfo).build();
    }

    @Nonnull
    private Schedule toPojoSchedule(@Nonnull Reporting.ScheduleInfo scheduleInfo, long scheduleId) {
        return new Schedule(
                        scheduleId, scheduleInfo.getDayOfWeek(),
                        scheduleInfo.getReportRequest().getFormat(),
                        scheduleInfo.getPeriod(),
                        ReportType.get(scheduleInfo.getReportRequest().getTemplate().getReportType()),
                        scheduleInfo.getReportRequest().getTemplate().getId(),
                        scheduleInfo.hasDayOfMonth()
                                        ? scheduleInfo.getDayOfMonth()
                                        : LocalDateTime.now().getDayOfMonth());
    }

    @Nonnull
    private Collection<ScheduleParameters> toPojoParameters(@Nonnull Map<String, String> parameters,
            long scheduleId) {
        final Collection<ScheduleParameters> result = new ArrayList<>(parameters.size());
        for (Entry<String, String> parameter : parameters.entrySet()) {
            final ScheduleParameters pojo =
                    new ScheduleParameters(scheduleId, parameter.getKey(), parameter.getValue());
            result.add(pojo);
        }
        return result;
    }

    private void storeSchedule(@Nonnull Reporting.ScheduleInfo scheduleInfo, @Nonnull Schedule schedule) {
        logger.debug("Storing schedule {}...", schedule);
        context.transaction(configuration -> {
                    final DSLContext transaction = DSL.using(configuration);
                    transaction.newRecord(SCHEDULE, schedule).store();
                    final long scheduleId = schedule.getId();
                    for (String email : scheduleInfo.getReportRequest().getSubscribersEmailsList()) {
                        transaction.insertInto(SCHEDULE_SUBSCRIBERS, SCHEDULE_SUBSCRIBERS.SCHEDULE_ID,
                                SCHEDULE_SUBSCRIBERS.EMAIL_ADDRESS).values(scheduleId, email).execute();
                    }
                    for (ScheduleParameters param : toPojoParameters(
                            scheduleInfo.getReportRequest().getParametersMap(), schedule.getId())) {
                        transaction.newRecord(SCHEDULE_PARAMETERS, param).store();
                    }
                });
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
            return context.transactionResult(configuration -> {
                final DSLContext transaction = DSL.using(configuration);
                return transaction.selectFrom(SCHEDULE)
                        .fetch()
                        .stream()
                        .map(info -> toScheduleDTO(transaction, info))
                        .collect(Collectors.toSet());
            });
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
        final Reporting.GenerateReportRequest request = scheduleInfo.getReportRequest();
        try {
            context.transaction(configuration -> {
                final DSLContext transaction = DSL.using(configuration);
                transaction.update(SCHEDULE)
                        .set(SCHEDULE.REPORT_TYPE,
                                ReportType.get(request.getTemplate().getReportType()))
                        .set(SCHEDULE.TEMPLATE_ID, request.getTemplate().getId())
                        .set(SCHEDULE.PERIOD, scheduleInfo.getPeriod())
                        .set(SCHEDULE.DAY_OF_WEEK, scheduleInfo.getDayOfWeek())
                        .set(SCHEDULE.FORMAT, request.getFormat())
                        .where(SCHEDULE.ID.eq(scheduleDTO.getId()))
                        .execute();
                /* As subscribers may be changed different ways we need to rewrite
                 subcribers for edited schedule. */
                transaction.deleteFrom(SCHEDULE_SUBSCRIBERS)
                        .where(SCHEDULE_SUBSCRIBERS.SCHEDULE_ID.eq(scheduleDTO.getId()))
                        .execute();
                final List<String> emails = request.getSubscribersEmailsList();
                emails.forEach(email -> transaction.insertInto(SCHEDULE_SUBSCRIBERS,
                        SCHEDULE_SUBSCRIBERS.SCHEDULE_ID, SCHEDULE_SUBSCRIBERS.EMAIL_ADDRESS)
                        .values(scheduleDTO.getId(), email)
                        .execute());
                transaction.deleteFrom(SCHEDULE_PARAMETERS)
                        .where(SCHEDULE_PARAMETERS.SCHEDULE_ID.eq(scheduleDTO.getId()))
                        .execute();
                for (ScheduleParameters parameter : toPojoParameters(request.getParametersMap(),
                        scheduleDTO.getId())) {
                    transaction.newRecord(SCHEDULE_PARAMETERS, parameter).store();
                }
            });
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
            return context.transactionResult(configuration -> {
                final DSLContext transaction = DSL.using(configuration);
                final ScheduleRecord record = transaction.selectFrom(SCHEDULE)
                        .where(SCHEDULE.ID.eq(id))
                        .iterator()
                        .next();
                return record == null ? Reporting.ScheduleDTO.newBuilder().build() :
                        toScheduleDTO(transaction, record);
            });
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
            return context.transactionResult(configuration -> {
                final DSLContext transaction = DSL.using(configuration);
                return transaction.selectFrom(SCHEDULE)
                        .where(SCHEDULE.REPORT_TYPE.eq(ReportType.get(reportType))
                                .and(SCHEDULE.TEMPLATE_ID.eq(templateId)))
                        .fetch()
                        .stream()
                        .map(info -> toScheduleDTO(transaction, info))
                        .collect(Collectors.toSet());
            });
        } catch (DataAccessException e) {
            final String message = String.format("Failed to get schedules with reportType %d and templateId %d",
                            reportType, templateId);
            throw new DbException(message, e);
        }
    }

    @Nonnull
    private Reporting.ScheduleDTO toScheduleDTO(@Nonnull DSLContext transaction,
            @Nonnull ScheduleRecord scheduleRecord) {
        final List<String> subscribers = transaction.selectFrom(SCHEDULE_SUBSCRIBERS)
                .where(SCHEDULE_SUBSCRIBERS.SCHEDULE_ID.eq(scheduleRecord.getId()))
                .fetch()
                .stream()
                .map(ScheduleSubscribersRecord::getEmailAddress)
                .collect(Collectors.toList());
        final List<ScheduleParameters> parameters = transaction.selectFrom(SCHEDULE_PARAMETERS)
                .where(SCHEDULE_PARAMETERS.SCHEDULE_ID.eq(scheduleRecord.getId()))
                .fetchInto(ScheduleParameters.class);
        final Reporting.ScheduleInfo info = toScheduleInfo(scheduleRecord, subscribers, parameters);
        return Reporting.ScheduleDTO.newBuilder()
                .setId(scheduleRecord.getId())
                .setScheduleInfo(info)
                .build();
    }

    @Nonnull
    private Map<String, String> fromPojoParameters(
            @Nonnull Collection<ScheduleParameters> parameters) {
        final Map<String, String> result = new HashMap<>();
        for (ScheduleParameters parameter : parameters) {
            result.put(parameter.getParamName(), parameter.getParamValue());
        }
        return result;
    }

    @Nonnull
    private Reporting.ScheduleInfo toScheduleInfo(@Nonnull ScheduleRecord scheduleRecord,
            @Nonnull List<String> subscribers, @Nonnull Collection<ScheduleParameters> parameters) {
        final Reporting.GenerateReportRequest request = Reporting.GenerateReportRequest.newBuilder()
                .setFormat(scheduleRecord.getFormat())
                .addAllSubscribersEmails(subscribers)
                .setTemplate(ReportTemplateId.newBuilder()
                        .setReportType(scheduleRecord.getReportType().getValue())
                        .setId(scheduleRecord.getTemplateId())
                        .build())
                .putAllParameters(fromPojoParameters(parameters))
                .build();
        return Reporting.ScheduleInfo.newBuilder()
                .setDayOfWeek(scheduleRecord.getDayOfWeek())
                .setPeriod(scheduleRecord.getPeriod())
                .setDayOfMonth(scheduleRecord.getDayOfMonth())
                .setReportRequest(request)
                .build();
    }
}
