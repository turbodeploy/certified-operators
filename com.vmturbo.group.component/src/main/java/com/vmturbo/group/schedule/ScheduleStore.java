package com.vmturbo.group.schedule;

import static com.vmturbo.group.db.Tables.SCHEDULE;
import static com.vmturbo.group.db.Tables.SETTING_POLICY_SCHEDULE;
import static org.jooq.impl.DSL.deleteFrom;
import static org.jooq.impl.DSL.trueCondition;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.schedule.ScheduleProto;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.OneTime;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.Perpetual;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.RecurrenceStart;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.components.common.diagnostics.DiagsZipReader;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.InvalidItemException;
import com.vmturbo.group.common.ItemActionException.InvalidScheduleAssignmentException;
import com.vmturbo.group.common.ItemDeleteException.ScheduleInUseDeleteException;
import com.vmturbo.group.common.ItemNotFoundException.ScheduleNotFoundException;
import com.vmturbo.group.common.ItemNotFoundException.SettingPolicyNotFoundException;
import com.vmturbo.group.db.tables.pojos.Schedule;
import com.vmturbo.group.db.tables.records.ScheduleRecord;
import com.vmturbo.group.db.tables.records.SettingPolicyScheduleRecord;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.setting.SettingStore;

/**
 * This class implements persistence layer for schedules.
 */
public class ScheduleStore implements DiagsRestorable {

    /**
     * The file name for the schedules dump collected from the {@link ScheduleStore}.
     * It's a string file, so the "diags" extension is required for compatibility
     * with {@link DiagsZipReader}.
     */
    private static final String SCHEDULES_DUMP_FILE = "schedules_dump";
    private static final int MAX_BATCH_SIZE = 50;

    private final Logger logger = LogManager.getLogger();

    private final DSLContext dslContext;
    private final ScheduleValidator scheduleValidator;
    private final IdentityProvider identityProvider;
    private final SettingStore settingStore;

    /**
     * Create new ScheduleStore.
     *
     * @param dslContext A context with which to interact with the underlying datastore
     * @param scheduleValidator Schedule validator
     * @param identityProvider The identity provider used to assign OIDs
     * @param settingStore Settings store
     */
    public ScheduleStore(@Nonnull final DSLContext dslContext,
                         @Nonnull final ScheduleValidator scheduleValidator,
                         @Nonnull final IdentityProvider identityProvider,
                         @Nonnull final SettingStore settingStore) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.scheduleValidator = Objects.requireNonNull(scheduleValidator);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.settingStore = Objects.requireNonNull(settingStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender appender) throws DiagnosticsException {
        try {
            dslContext.transaction(configuration -> {
                final DSLContext context = DSL.using(configuration);
                logger.info("Exporting schedules");
                appender.appendString(exportSchedulesAsJson(context));
                appender.appendString(exportSettingPolicySchedulesAsJson(context));
            });
        } catch (DataAccessException e) {
            logger.error("Exception collecting schedule diags", e);
            throw new DiagnosticsException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags) throws DiagnosticsException {
        final List<String> errors = new ArrayList<>();
        if (collectedDiags.size() != 2) {
            throw new DiagnosticsException("Wrong number of diagnostics lines: "
                + collectedDiags.size() + ". Expected: 2.");
        }
        try {
            dslContext.transaction(configuration -> {
                final DSLContext context = DSL.using(configuration);
                logger.info("Restoring schedules");
                deleteAllSchedules(context);
                final int schedSize = importSchedulesFromJson(context, collectedDiags.get(0));
                logger.info("Imported {} schedules", () -> schedSize);

                logger.info("Restoring setting policy schedule mappings");
                deleteAllSettingPolicyMappings(context);
                final int schedMappingSize = importSettingPolicySchedulesFromJson(context,
                    collectedDiags.get(1));
                logger.info("Imported {} setting policy schedule mappings", () -> schedMappingSize);
            });
        } catch (DataAccessException e) {
            logger.error("Exception restoring schedule diags", e);
            errors.add("Exception restoring schedule diags: " + e.getMessage() + ": " +
                ExceptionUtils.getStackTrace(e));
        }
        if (!errors.isEmpty()) {
            throw new DiagnosticsException(errors);
        }
    }

    @Nonnull
    @Override
    public String getFileName() {
        return SCHEDULES_DUMP_FILE;
    }

    /**
     * Get all schedules.
     *
     * @return All schedules stored in the database
     */
    @Nonnull
    public Stream<ScheduleProto.Schedule> getSchedules() {
        return getSchedules(Sets.newHashSet());
    }

    /**
     * Get schedules from the database.
     *
     * @param ids of schedules to fetch.
     *
     * @return Schedules for specified ids or all schedules stored in the database if
     * no ids have been provided
     */
    @Nonnull
    public Stream<ScheduleProto.Schedule> getSchedules(@Nonnull final Set<Long> ids) {
        return dslContext.transactionResult(configuration -> {
            final DSLContext context = DSL.using(configuration);
            Condition whereCondition = trueCondition();
            if (!ids.isEmpty()) {
                whereCondition = SCHEDULE.ID.in(ids);
            }
            return context.selectFrom(SCHEDULE)
                .where(whereCondition)
                .fetch()
                .into(Schedule.class)
                .stream()
                .map(this::toScheduleMessage);
        });
    }

    /**
     * Get a schedule by its unique OID.
     *
     * @param oid The OID (object id) of the setting policy to retrieve.
     * @return The {@link ScheduleProto.Schedule} associated with the OID, or an empty schedule.
     */
    @Nonnull
    public Optional<ScheduleProto.Schedule> getSchedule(final long oid) {
        return dslContext.transactionResult(configuration -> {
            final DSLContext context = DSL.using(configuration);
            final ScheduleRecord record = context.fetchOne(SCHEDULE, SCHEDULE.ID.eq(oid));

            ScheduleProto.Schedule schedule = record == null ? null
                : toScheduleMessage(record.into(Schedule.class));
            return Optional.ofNullable(schedule);
        });
    }

    /**
     * Persists a new {@link Schedule} in {@link ScheduleStore} based on the specified
     * {@link ScheduleProto.Schedule}.
     *
     * @param schedule {@link ScheduleProto.Schedule} message to persist.
     * @return Generated {@link ScheduleProto.Schedule} message. It will have ID assigned.
     * @throws InvalidItemException If the specified schedule message is invalid.
     * @throws DuplicateNameException If there is already a schedule with the same display name as
     * the input schedule.
     */
    @Nonnull
    public ScheduleProto.Schedule createSchedule(@Nonnull final ScheduleProto.Schedule schedule)
        throws InvalidItemException, DuplicateNameException {
        try {
            return dslContext.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                // Validate before saving
                scheduleValidator.validateSchedule(schedule);
                // Explicitly search for a schedule with the same name, so that we
                // know when to throw a DuplicateNameException as opposed to a generic
                // DataIntegrityException.
                final Record1<Long> existingId = context.select(SCHEDULE.ID)
                    .from(SCHEDULE)
                    .where(SCHEDULE.DISPLAY_NAME.eq(schedule.getDisplayName()))
                    .fetchOne();
                if (existingId != null) {
                    throw new DuplicateNameException(existingId.value1(),
                        schedule.getDisplayName());
                }
                Schedule scheduleRecord = generateScheduleRecord(schedule);
                context.newRecord(SCHEDULE, scheduleRecord).store();
                return toScheduleMessage(scheduleRecord);
            });
        } catch (DataAccessException e) {
            if (e.getCause() instanceof InvalidItemException) {
                throw (InvalidItemException)e.getCause();
            } else if (e.getCause() instanceof DuplicateNameException) {
                throw (DuplicateNameException)e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Updates an existing schedule in {@link ScheduleStore}.
     *
     * @param id The ID of the schedule to update.
     * @param schedule The {@link ScheduleProto.Schedule} to update from.
     * @return The updated {@link ScheduleProto.Schedule}
     * @throws InvalidItemException If the specified schedule fails validation.
     * @throws ScheduleNotFoundException If no schedule with the specified ID exists in
     * {@link ScheduleStore}
     * @throws DuplicateNameException If another with the same display name already exists in
     * {@link ScheduleStore}
     */
    @Nonnull
    public ScheduleProto.Schedule updateSchedule(final long id,
         @Nonnull final ScheduleProto.Schedule schedule)
        throws InvalidItemException, ScheduleNotFoundException, DuplicateNameException {
        try {
            return dslContext.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                final ScheduleRecord existingRecord = context.fetchOne(SCHEDULE, SCHEDULE.ID.eq(id));
                if (existingRecord == null) {
                    throw new ScheduleNotFoundException(id);
                }

                // Explicitly search for an existing schedule with the same name that's NOT
                // the schedule being edited. We do this because we want to know
                // know when to throw a DuplicateNameException as opposed to a generic
                // DataIntegrityException.
                final Record1<Long> existingId = context.select(SCHEDULE.ID)
                    .from(SCHEDULE)
                    .where(SCHEDULE.DISPLAY_NAME.eq(schedule.getDisplayName()))
                    .and(SCHEDULE.ID.ne(id))
                    .fetchOne();
                if (existingId != null) {
                    throw new DuplicateNameException(existingId.value1(), schedule.getDisplayName());
                }

                // Validate before saving
                scheduleValidator.validateSchedule(schedule);
                updateScheduleRecord(existingRecord, schedule);
                final int modifiedRecords = existingRecord.update();
                if (modifiedRecords == 0) {
                    // This should not really happen
                    logger.error("Failed to update schedule record id {}", () -> id);
                }
                return toScheduleMessage(existingRecord.into(Schedule.class));
            });
        } catch (DataAccessException e) {
            if (e.getCause() instanceof ScheduleNotFoundException) {
                throw (ScheduleNotFoundException)e.getCause();
            } else if (e.getCause() instanceof DuplicateNameException) {
                throw (DuplicateNameException)e.getCause();
            } else if (e.getCause() instanceof InvalidItemException) {
                throw (InvalidItemException)e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Delete a schedule for {@link ScheduleStore}.
     *
     * @param id The ID of the schedule to delete
     * @return Deleted {@link ScheduleProto.Schedule}
     * @throws ScheduleNotFoundException If schedule to delete does not exist in {@link ScheduleStore}
     * @throws ScheduleInUseDeleteException If schedule to delete is currently in use
     */
    public ScheduleProto.Schedule deleteSchedule(final long id)
        throws ScheduleNotFoundException, ScheduleInUseDeleteException {
        try {
            return dslContext.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                final ScheduleRecord existingRecord = context.fetchOne(SCHEDULE, SCHEDULE.ID.eq(id));
                if (existingRecord == null) {
                    throw new ScheduleNotFoundException(id);
                }
                // Verify no setting policies are assigned to this schedule
                if (getAssignedSettingPolicyCount(context, id) > 0) {
                    throw new ScheduleInUseDeleteException("Cannot delete schedule record id: " + id +
                        " because it is used by setting policies");
                }

                final int modifiedRecords = existingRecord.delete();
                if (modifiedRecords == 0) {
                    // This should not really happen
                    logger.error("Failed to delete schedule record id {}", () -> id);
                }
                return toScheduleMessage(existingRecord.into(Schedule.class));
            });
        } catch (DataAccessException e) {
            if (e.getCause() instanceof ScheduleNotFoundException) {
                throw (ScheduleNotFoundException)e.getCause();
            } else if (e.getCause() instanceof ScheduleInUseDeleteException) {
                throw (ScheduleInUseDeleteException)e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Bulk delete schedules.
     * Currently they are being deleted in transactional fashion, that is either all of them ot none
     * of them get deleted.
     *
     * @param ids Schedules to delete
     * @return Number of deleted schedules
     * @throws ScheduleNotFoundException If schedule to delete does not exist in {@link ScheduleStore}
     * @throws ScheduleInUseDeleteException If schedule to delete is currently in use
     */
    public int deleteSchedules(@Nonnull final Set<Long> ids) throws ScheduleNotFoundException,
        ScheduleInUseDeleteException {
        try {
            return dslContext.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                List<Long> existingIds = context
                    .select(SCHEDULE.ID)
                    .from(SCHEDULE)
                    .where(SCHEDULE.ID.in(ids))
                    .fetch(SCHEDULE.ID);
                if (existingIds.size() < ids.size()) {
                    // we are asked to delete some non-existing schedules
                    final Set<String> missingIds = Sets.difference(ids, Sets.newHashSet(existingIds))
                        .stream()
                        .map(String::valueOf)
                        .collect(Collectors.toSet());
                    throw new ScheduleNotFoundException("Schedules " + String.join(",", missingIds) +
                        " not found");
                }

                if (getAssignedSettingPolicyMultipleScheduleCount(context, ids) > 0) {
                    final Set<String> usedIds = ids.stream()
                        .map(String::valueOf)
                        .collect(Collectors.toSet());
                    throw new ScheduleInUseDeleteException("Cannot delete schedule records with ids: "
                        + String.join(",", usedIds) + " because they are used by setting policies");
                }

                Lists.partition(existingIds, MAX_BATCH_SIZE).stream()
                    .forEach(batchToDelete -> context.batch(
                        deleteFrom(SCHEDULE)
                        .where(SCHEDULE.ID.in(batchToDelete)))
                        .execute()
                    );
                return existingIds.size();
            });
        } catch (DataAccessException e) {
            if (e.getCause() instanceof ScheduleInUseDeleteException) {
                throw (ScheduleInUseDeleteException)e.getCause();
            } else if (e.getCause() instanceof ScheduleNotFoundException) {
                throw (ScheduleNotFoundException)e.getCause();
            } else {
                throw e;
            }
        }

    }

    /**
     * Assign schedule to setting policy.
     * Note that is currently used mostly for testing, the eventual workflow will include schedule
     * being assigned to setting policy through an API call.
     *
     * @param settingPolicyId {@link com.vmturbo.group.db.tables.pojos.SettingPolicy} id to which
     *          the schedule is being assigned
     * @param scheduleId {@link Schedule} id which is assigned to to the SettingPolicy
     * @throws SettingPolicyNotFoundException If setting policy does not exist in {@link SettingStore}
     * @throws ScheduleNotFoundException If schedule does not exist in {@link ScheduleStore}
     * @throws InvalidScheduleAssignmentException If scheduled cannot be assigned to the specified
     *          {@link SettingPolicy}
     */
     public void assignScheduleToSettingPolicy(final long settingPolicyId, final long scheduleId)
        throws SettingPolicyNotFoundException, ScheduleNotFoundException, InvalidScheduleAssignmentException {
        try {
            dslContext.transactionResult(configuration -> {
                final DSLContext context = DSL.using(configuration);
                // Verify both objects exists
                Optional<SettingPolicy> settingPolicy = settingStore.getSettingPolicy(settingPolicyId);
                if (!settingPolicy.isPresent()) {
                    throw new SettingPolicyNotFoundException(settingPolicyId);
                } else {
                    if (Type.DISCOVERED == settingPolicy.get().getSettingPolicyType()) {
                        throw new InvalidScheduleAssignmentException("Schedule cannot be assigned " +
                            "to discovered setting policy");
                    }
                }
                if (!getSchedule(scheduleId).isPresent()) {
                    throw new ScheduleNotFoundException(scheduleId);
                }
                SettingPolicyScheduleRecord record =
                    new SettingPolicyScheduleRecord(settingPolicyId, scheduleId);
                return context.newRecord(SETTING_POLICY_SCHEDULE, record).store();
            });
        } catch (DataAccessException e) {
            if (e.getCause() instanceof SettingPolicyNotFoundException) {
                throw (SettingPolicyNotFoundException)e.getCause();
            } else if (e.getCause() instanceof ScheduleNotFoundException) {
                throw (ScheduleNotFoundException)e.getCause();
            } else if (e.getCause() instanceof InvalidScheduleAssignmentException) {
                throw (InvalidScheduleAssignmentException)e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * Get count of setting policies assigned to the specified schedule.
     *
     * @param context jooq DSL context
     * @param scheduleId Schedule ID {@link Schedule} id which is assigned to to the SettingPolicy
     * @return Number of assigned {@link com.vmturbo.group.db.tables.records.SettingPolicyRecord}
     */
    private int getAssignedSettingPolicyCount(
        @Nonnull final DSLContext context, @Nonnull final long scheduleId) {
        return context
            .selectCount()
            .from(SETTING_POLICY_SCHEDULE
            .where(SETTING_POLICY_SCHEDULE.SCHEDULE_ID.eq(scheduleId)))
            .fetchOne(0, Integer.class);
    }

    /**
     * Get count of all setting policies assigned to the specified collection of schedules, if any.
     *
     * @param context jooq DSL context
     * @param scheduleIds Collection of schedule IDs
     * @return Number of assigned {@link com.vmturbo.group.db.tables.records.SettingPolicyRecord}
     * */
    private int getAssignedSettingPolicyMultipleScheduleCount(
        @Nonnull final DSLContext context, final Collection<Long> scheduleIds) {
        return context
            .selectCount()
            .from(SETTING_POLICY_SCHEDULE
            .where(SETTING_POLICY_SCHEDULE.SCHEDULE_ID.in(scheduleIds)))
            .fetchOne(0, Integer.class);
    }

    /**
     * Truncate schedules, for internal use when restoring diags.
     *
     * @param context jooq DSL context
     * @return truncate execution result
     */
    private int deleteAllSchedules(@Nonnull final DSLContext context) {
        return context.deleteFrom(SCHEDULE).execute();
    }

    /**
     * Truncate setting policy schedule mappings, for internal use when restoring diags.
     *
     * @param context jooq DSL context
     * @return truncate execution result
     */
    private int deleteAllSettingPolicyMappings(@Nonnull final DSLContext context) {
        return context.truncate(SETTING_POLICY_SCHEDULE).execute();
    }

    /**
     * Export schedules as JSON, for internal use when collecting diags.
     *
     * @param context jooq DSL context
     * @return Exported schedules as jooq JSON string
     */
    @Nonnull
    private String exportSchedulesAsJson(@Nonnull final DSLContext context) {
        return context.fetch(SCHEDULE).formatJSON();
    }

    /**
     * Export setting policy schedule mappings as JSON, for internal use when collecting diags.
     *
     * @param context jooq DSL context
     * @return Exported setting policy schedule mappings as jooq JSON string
     */
    @Nonnull
    private String exportSettingPolicySchedulesAsJson(@Nonnull final DSLContext context) {
        return context.fetch(SETTING_POLICY_SCHEDULE).formatJSON();
    }

    /**
     * Import schedules from JSON, for internal use when restoring diags.
     *
     * @param context jooq DSL context
     * @param schedulesJson Schedules as jooq Json string
     * @return Number of imported records.
     * @throws IOException If any IO exception
     */
    private int importSchedulesFromJson(@Nonnull final DSLContext context,
                                        @Nonnull final String schedulesJson) throws IOException {
        return context.loadInto(SCHEDULE)
            .loadJSON(schedulesJson)
            .fields(SCHEDULE.ID, SCHEDULE.DISPLAY_NAME, SCHEDULE.START_TIME, SCHEDULE.END_TIME,
                SCHEDULE.LAST_DATE, SCHEDULE.RECUR_RULE, SCHEDULE.TIME_ZONE_ID)
            .execute()
            .executed();
    }

    /**
     * Import setting policy schedule mappings from JSON, for internal use when restoring diags.
     *
     * @param context jooq DSL context
     * @param settingPolicyJson Setting policy schedule mappings as jooq Json string
     * @return Number of imported records.
     * @throws IOException If any IO exception
     */
    private int importSettingPolicySchedulesFromJson(@Nonnull final DSLContext context,
                                                     @Nonnull final String settingPolicyJson)
        throws IOException {
        return context.loadInto(SETTING_POLICY_SCHEDULE)
                .loadJSON(settingPolicyJson)
            .fields(SETTING_POLICY_SCHEDULE.SETTING_POLICY_ID, SETTING_POLICY_SCHEDULE.SCHEDULE_ID)
            .execute()
            .executed();
    }

    /**
     * Convert {@link Schedule} record to {@link ScheduleProto.Schedule} message.
     *
     * @param jooqSchedule {@link Schedule} record to convert
     * @return {@link ScheduleProto.Schedule} message
     */
    @Nonnull
    private ScheduleProto.Schedule toScheduleMessage(@Nonnull final Schedule jooqSchedule) {
        final ScheduleProto.Schedule.Builder schedule = ScheduleProto.Schedule.newBuilder();
        schedule
            .setId(jooqSchedule.getId())
            .setDisplayName(jooqSchedule.getDisplayName())
            .setStartTime(jooqSchedule.getStartTime().getTime())
            .setEndTime(jooqSchedule.getEndTime().getTime())
            .setTimezoneId(jooqSchedule.getTimeZoneId());
        if (jooqSchedule.getLastDate() != null) {
            schedule.setLastDate(jooqSchedule.getLastDate().getTime());
        } else {
            schedule.setPerpetual(Perpetual.getDefaultInstance());
        }
        if (StringUtils.isNotBlank(jooqSchedule.getRecurRule())) {
            schedule.setRecurRule(jooqSchedule.getRecurRule());
        } else {
            schedule.setOneTime(OneTime.getDefaultInstance());
        }
        if (jooqSchedule.getRecurrenceStartTime() != null) {
            schedule.setRecurrenceStart(RecurrenceStart.newBuilder()
                .setRecurrenceStartTime(jooqSchedule.getRecurrenceStartTime().getTime())
                .build());
        }
        return schedule.build();
    }

    /**
     * Generate new {@link Schedule} record {@link ScheduleProto.Schedule} message.
     *
     * @param scheduleMessage {@link ScheduleProto.Schedule} message to generate from
     * @return Generated {@link Schedule} record
     */
    @Nonnull
    private Schedule generateScheduleRecord(@Nonnull final ScheduleProto.Schedule scheduleMessage) {
        return new Schedule(
            identityProvider.next(),
            scheduleMessage.getDisplayName(),
            new Timestamp(scheduleMessage.getStartTime()),
            new Timestamp(scheduleMessage.getEndTime()),
            scheduleMessage.hasOneTime() || scheduleMessage.hasPerpetual() ?
                null : new Timestamp(scheduleMessage.getLastDate()),
            scheduleMessage.hasOneTime() ?
                null : scheduleMessage.getRecurRule(),
            scheduleMessage.getTimezoneId(),
            scheduleMessage.hasRecurrenceStart() ?
                new Timestamp(scheduleMessage.getRecurrenceStart().getRecurrenceStartTime()) : null
        );
    }

    /**
     * Update existing {@link Schedule} record from {@link ScheduleProto.Schedule} message.
     *
     * @param recordToUpdate jooq record to update
     * @param scheduleMessage {@link ScheduleProto.Schedule} message to generate from
     */
    private void updateScheduleRecord(@Nonnull final ScheduleRecord recordToUpdate,
                                      @Nonnull final ScheduleProto.Schedule scheduleMessage) {
        recordToUpdate.setDisplayName(scheduleMessage.getDisplayName());
        recordToUpdate.setStartTime(new Timestamp(scheduleMessage.getStartTime()));
        recordToUpdate.setEndTime(new Timestamp(scheduleMessage.getEndTime()));
        recordToUpdate.setLastDate(scheduleMessage.hasOneTime() || scheduleMessage.hasPerpetual() ?
            null : new Timestamp(scheduleMessage.getLastDate()));
        recordToUpdate.setRecurRule(scheduleMessage.hasOneTime() ?
            null : scheduleMessage.getRecurRule());
        recordToUpdate.setTimeZoneId(scheduleMessage.getTimezoneId());
        recordToUpdate.setRecurrenceStartTime(scheduleMessage.hasRecurrenceStart() ?
            new Timestamp(scheduleMessage.getRecurrenceStart().getRecurrenceStartTime()) : null);
    }
}
