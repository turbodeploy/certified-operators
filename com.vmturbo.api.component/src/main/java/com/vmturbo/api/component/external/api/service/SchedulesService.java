package com.vmturbo.api.component.external.api.service;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import io.grpc.StatusRuntimeException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.validation.Errors;

import com.vmturbo.api.component.external.api.mapper.ExceptionMapper;
import com.vmturbo.api.component.external.api.mapper.ScheduleMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.settingspolicy.RecurrenceApiDTO;
import com.vmturbo.api.dto.settingspolicy.ScheduleApiDTO;
import com.vmturbo.api.dto.settingspolicy.SettingsPolicyApiDTO;
import com.vmturbo.api.enums.RecurrenceType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.ISchedulesService;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.CreateScheduleRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.CreateScheduleResponse;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.DeleteScheduleRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.GetScheduleRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.GetScheduleResponse;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.GetSchedulesRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.UpdateScheduleRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.UpdateScheduleResponse;
import com.vmturbo.common.protobuf.schedule.ScheduleServiceGrpc;
import com.vmturbo.common.protobuf.schedule.ScheduleServiceGrpc.ScheduleServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPoliciesUsingScheduleRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;

/**
 * This class provides services for managing schedules. A schedule represents a number periods in time.
 * The operations include adding, editing, retrieving, deleting, and getting associated actions and policies.
 */
public class SchedulesService implements ISchedulesService {

    // 1970-01-01 00:00:01 UTC
    private static final long MIN_TIME_TS = 1000L;
    private static final ZonedDateTime MIN_TIME = ZonedDateTime.ofInstant(Instant.ofEpochMilli(MIN_TIME_TS),
        ZoneId.systemDefault());
    // 2038-01-19 03:14:07 UTC
    static final long MAX_TIME_TS = 2147483647000L;

    private final Logger logger = LogManager.getLogger();

    private final ScheduleServiceGrpc.ScheduleServiceBlockingStub scheduleService;
    private final SettingPolicyServiceBlockingStub settingPolicyService;
    private final ScheduleMapper scheduleMapper;
    private final SettingsMapper settingsMapper;
    private final ActionSearchUtil actionSearchUtil;

    /**
     * {@link SchedulesService} constructor.
     *
     * @param scheduleService RPC Schedule Service
     * @param settingPolicyService RPC Setting Policy Service
     * @param scheduleMapper Schedule Mapper for conversions between XL objects to their API counterparts
     * @param settingsMapper Setting Mapper for conversions between XL setting policy objects to
     * @param actionSearchUtil Used for searching actions
     */
    public SchedulesService(@Nonnull final ScheduleServiceBlockingStub scheduleService, @Nonnull final SettingPolicyServiceBlockingStub settingPolicyService,
            @Nonnull final ScheduleMapper scheduleMapper,
            @Nonnull final SettingsMapper settingsMapper, @Nonnull ActionSearchUtil actionSearchUtil) {
        this.scheduleService = Objects.requireNonNull(scheduleService);
        this.settingPolicyService = Objects.requireNonNull(settingPolicyService);
        this.scheduleMapper = Objects.requireNonNull(scheduleMapper);
        this.settingsMapper = Objects.requireNonNull(settingsMapper);
        this.actionSearchUtil = Objects.requireNonNull(actionSearchUtil);
    }

    /**
     * Returns a list of all existing schedules.
     *
     * @return the list of schedules.
     * @throws Exception in case of error during retrieval of existing schedules.
     */
    @Override
    public List<ScheduleApiDTO> getSchedules() throws Exception {
        final List<Schedule> schedules = new LinkedList<>();
        try {
            scheduleService.getSchedules(
                GetSchedulesRequest.newBuilder().build())
                .forEachRemaining(schedules::add);
        } catch (StatusRuntimeException e) {
            logger.error(e);
            throw ExceptionMapper.translateStatusException(e);
        }
        return scheduleMapper.convertSchedules(schedules);
    }

    /**
     * Looks up and returns a schedule based on its uuid.
     *
     * @param uuid the uuid for the schedule that is being looked up.
     * @return The schedule with the input uuid.
     * @throws Exception  in case of error during retrieval of the requested schedule.
     */
    @Override
    public ScheduleApiDTO getSchedule(final String uuid) throws Exception {
        GetScheduleResponse response;
        try {
            response = scheduleService.getSchedule(
                GetScheduleRequest.newBuilder()
                    .setOid(Long.valueOf(uuid))
                    .build());
        } catch (StatusRuntimeException e) {
            logger.error(e);
            throw ExceptionMapper.translateStatusException(e);
        }
        return scheduleMapper.convertSchedule(response.getSchedule());
    }

    /**
     * Adds an new schedule.
     *
     * @param scheduleApiDto The new schedule object.
     * @return  the new schedule object.
     * @throws Exception in case of error during creating of the requested schedule.
     */
    @Override
    public ScheduleApiDTO addSchedule(final ScheduleApiDTO scheduleApiDto) throws Exception {
        final Schedule schedule = scheduleMapper.convertInput(scheduleApiDto);
        CreateScheduleResponse response;
        try {
            response = scheduleService.createSchedule(CreateScheduleRequest.newBuilder()
                .setSchedule(schedule)
                .build());
        } catch (StatusRuntimeException e) {
            logger.error(e);
            throw ExceptionMapper.translateStatusException(e);
        }
        return scheduleMapper.convertSchedule(response.getSchedule());
    }

    /**
     * Edits an existing schedule using its uuid.
     *
     * @param uuid The uuid for the schedule that needs to be edited.
     * @param scheduleApiDto The updated schedule object.
     * @return  the updated schedule object.
     * @throws Exception in case of error during editing of the requested schedule.
     */
    @Override
    public ScheduleApiDTO editSchedule(final String uuid, ScheduleApiDTO scheduleApiDto) throws Exception {
        final Schedule schedule = scheduleMapper.convertInput(scheduleApiDto);
        UpdateScheduleResponse response;
        try {
            response = scheduleService.updateSchedule(UpdateScheduleRequest.newBuilder()
                .setOid(Long.valueOf(uuid))
                .setUpdatedSchedule(schedule)
                .build());
        } catch (StatusRuntimeException e) {
            logger.error(e);
            throw ExceptionMapper.translateStatusException(e);
        }
        return scheduleMapper.convertSchedule(response.getSchedule());
    }

    /**
     * Deletes an existing schedule based on its uuid.
     *
     * @param uuid the uuid for the schedule that being deleted.
     * @throws Exception in case of error during deleting of the requested schedule.
     */
    @Override
    public void deleteSchedule(final String uuid) throws Exception {
        try {
            scheduleService.deleteSchedule(DeleteScheduleRequest.newBuilder()
                .setOid(Long.valueOf(uuid))
                .build());
        } catch (StatusRuntimeException e) {
            logger.error(e);
            throw ExceptionMapper.translateStatusException(e);
        }
    }

    /**
     * Returns all the actions that are going to be executed in a schedule.
     *
     * @param uuid The uuid for the schedule that we getting the actions for.
     * @return the list of actions.
     * @throws Exception in case of error during retrieving the actions of the input schedule.
     */
    @Override
    public List<ActionApiDTO> getActionsToBeExecutedInSchedule(String uuid) throws Exception {
        final ActionQueryFilter queryFilter = ActionQueryFilter.newBuilder()
                .addAllStates(Arrays.asList(ActionState.READY, ActionState.ACCEPTED))
                .setAssociatedScheduleId(Long.parseLong(uuid))
                .build();
        return actionSearchUtil.callActionServiceWithNoPagination(queryFilter);
    }

    /**
     * Returns the list of policies which are scheduled and are using this schedule as their
     * schedule.
     *
     * @param uuid The uuid for the schedule that we getting the policies for.
     * @return the list of policies.
     * @throws Exception in case of error during retrieving the policies of the input schedule.
     */
    @Override
    public List<SettingsPolicyApiDTO> getPoliciesUsingTheSchedule(String uuid) throws Exception {
        final List<SettingPolicy> settingPolicies = new LinkedList<>();

        try {
            // verify the schedule is valid
            GetScheduleResponse response = scheduleService.getSchedule(
                    GetScheduleRequest.newBuilder()
                            .setOid(Long.valueOf(uuid))
                            .build());
            if (!response.hasSchedule()) {
                throw new UnknownObjectException("Schedule '" + uuid + "' not found");
            }

            settingPolicyService.getSettingPoliciesUsingSchedule(
                    GetSettingPoliciesUsingScheduleRequest.newBuilder()
                            .setScheduleId(Long.valueOf(uuid))
                            .build())
                    .forEachRemaining(settingPolicies::add);
        } catch (StatusRuntimeException e) {
            logger.error(e);
            throw ExceptionMapper.translateStatusException(e);
        }

        return settingsMapper.convertSettingPolicies(settingPolicies);
    }

    /**
     * Validates incoming schedule.
     * NOTE: Validations basically copied from OpsManager ScheduleService.
     * In case of any validation errors Runtime exception is thrown with all validated errors
     * combined in the exception message.
     *
     * @param obj Object to validate
     * @param e   Spring framework validation errors, not actually used in our validations
     */
    @Override
    public void validateInput(final Object obj, final Errors e) {
        if (!(obj instanceof ScheduleApiDTO)) {
            logger.error("Unexpected object type validating schedule input: {}",
                () -> obj.getClass().getCanonicalName());
            throw new IllegalArgumentException("Unexpected object type validating schedule input: "
                + obj.getClass());
        }
        final ScheduleApiDTO scheduleDto = (ScheduleApiDTO)obj;

        // Timezone
        final TimeZone timeZone = DateTimeUtil.getTimeZone(scheduleDto.getTimeZone());
        if (timeZone == null) {
            throw new IllegalArgumentException(String.format(ScheduleValidationMessages.INVALID_TZ_MESSAGE,
                scheduleDto.getTimeZone()));
        }

        final StringBuilder errors = new StringBuilder();

        //  Display name
        if (StringUtils.isBlank(scheduleDto.getDisplayName())) {
            errors.append(String.format(ScheduleValidationMessages.MISSING_PARAM_MESSAGE, "Schedule name"));
        }

        // Verify state and end date of schedule period
        verifyStartAndEndTime(scheduleDto, timeZone, errors);

        // recurrence
        if (scheduleDto.getRecurrence() != null) {
            verifyRecurrence(scheduleDto, timeZone, errors);

        } else {
            // If this is non recurring schedule, we should not set recurrence start date
            if (scheduleDto.getStartDate() != null) {
                errors.append(String.format(ScheduleValidationMessages.INVALID_PARAM_MESSAGE, ScheduleValidationMessages.START_DATE,
                    ScheduleValidationMessages.NON_RECURRING_SCHEDULES));
            }

            // If this is non recurring schedule, we should not set recurrence end date
            if (scheduleDto.getEndDate() != null) {
                errors.append(String.format(ScheduleValidationMessages.INVALID_PARAM_MESSAGE, ScheduleValidationMessages.END_DATE,
                    ScheduleValidationMessages.NON_RECURRING_SCHEDULES));
            }
        }
        final String errorMsg = errors.toString();
        if (StringUtils.isNotBlank(errorMsg)) {
            logger.error("Error validating schedule input: {}", () -> errorMsg);
            throw new IllegalArgumentException(errorMsg);
        }

    }

    /**
     * Checks the validity of start and end time for an input schedule.
     *
     * @param scheduleDto the schedule that that its validity is checked
     * @param timeZone    the timezone that dates are defined in
     * @param errors validation messages, if any
     */
    private void verifyStartAndEndTime(@Nonnull final ScheduleApiDTO scheduleDto,
                           @Nullable TimeZone timeZone, @Nonnull final StringBuilder errors) {
        // Window Start Time Verification
        if (scheduleDto.getStartTime() == null) {
            errors.append(String.format(ScheduleValidationMessages.MISSING_PARAM_MESSAGE, "Schedule start time"));
            return;
        }

        // Window End Time Verification
        if (scheduleDto.getEndTime() == null) {
            errors.append(String.format(ScheduleValidationMessages.MISSING_PARAM_MESSAGE, "Schedule end time"));
            return;
        }

        long startTime = scheduleDto.getStartTime().atZone(timeZone.toZoneId()).toInstant().toEpochMilli();
        if (startTime < MIN_TIME_TS) {
            errors.append(String.format(ScheduleValidationMessages.EARLIEST_START_TIME_MESSAGE,
                ScheduleValidationMessages.SCHEDULE_START_TIME));
        }
        long endTime = scheduleDto.getEndTime().atZone(timeZone.toZoneId()).toInstant().toEpochMilli();
        if (endTime > MAX_TIME_TS) {
            // round down the request to the valid range
            scheduleDto.setEndTime(Instant.ofEpochMilli(MAX_TIME_TS).atZone(timeZone.toZoneId())
                .toLocalDateTime());
            endTime = MAX_TIME_TS;
        }

        // The start date should be before end date
        if (startTime >= endTime) {
            errors.append(String.format(ScheduleValidationMessages.PRECENDENCE_MESSAGE, ScheduleValidationMessages.START_TIME,
                scheduleDto.getStartTime(), ScheduleValidationMessages.END_TIME, scheduleDto.getEndTime()));
        }

        if (scheduleDto.getRecurrence() == null && startTime < System.currentTimeMillis()) {
            errors.append(ScheduleValidationMessages.NON_RECURRENT_START_TIME_MUST_BE_IN_FUTURE);
        }
    }

    /**
     * Checks the validity of recurrence object for an input schedule.
     *
     * @param scheduleDto the schedule that that its validity is checked
     * @param timeZone    the timezone that dates are defined in
     * @param errors validation messages, if any
     */
    private void verifyRecurrence(@Nonnull final ScheduleApiDTO scheduleDto,
                                  @Nullable final TimeZone timeZone, @Nonnull final StringBuilder errors) {
        //Verify start and end date of recurrence
        verifyStartAndEndDate(scheduleDto, timeZone, errors);

        final RecurrenceApiDTO recurrence = scheduleDto.getRecurrence();

        if (recurrence.getType() == null) {
            errors.append(String.format(ScheduleValidationMessages.MISSING_PARAM_MESSAGE, "Recurrence type"));
            return;
        }
        if (recurrence.getInterval() != null && recurrence.getInterval() < 1) {
            errors.append(ScheduleValidationMessages.NEGATIVE_RECURRENCE_INTERVAL_MESSAGE);

        }

        if (RecurrenceType.DAILY == recurrence.getType()) {
            verifyDailyRecurrence(recurrence, errors);
        } else if (RecurrenceType.WEEKLY == recurrence.getType()) {
            verifyWeeklyRecurrence(recurrence, errors);
        } else if (RecurrenceType.MONTHLY == recurrence.getType()) {
            verifyMonthlyRecurrence(recurrence, errors);
        } else {
            errors.append(String.format(ScheduleValidationMessages.UNSUPPORTED_RECURRENCE_TYPE_MESSAGE, recurrence.getType().name()));
        }
    }

    /**
     * Checks the validity of start and end date for recurrence of an input schedule.
     *
     * @param scheduleDto the schedule that that its validity is checked
     * @param timeZone    the timezone that dates are defined in
     * @param errors validation messages, if any
     */
    private void verifyStartAndEndDate(@Nonnull final ScheduleApiDTO scheduleDto,
                                       @Nullable final TimeZone timeZone, @Nonnull final StringBuilder errors) {
        Long startDate = null;
        if (scheduleDto.getStartDate() != null) {
            startDate = scheduleDto.getStartDate().atZone(timeZone.toZoneId()).toInstant()
                .toEpochMilli();
            if (startDate < MIN_TIME_TS) {
                errors.append(String.format(ScheduleValidationMessages.EARLIEST_START_TIME_MESSAGE,
                    ScheduleValidationMessages.SCHEDULE_START_DATE));
            }
        }

        Long endDate = null;
        if (scheduleDto.getEndDate() != null) {
            endDate = scheduleDto.getEndDate().atTime(LocalTime.MAX).atZone(timeZone.toZoneId())
                .toInstant().toEpochMilli();
            if (endDate > MAX_TIME_TS) {
                // round down the request to the valid range
                scheduleDto.setEndDate(Instant.ofEpochMilli(MAX_TIME_TS).atZone(timeZone.toZoneId())
                    .toLocalDate().minusDays(1));
                endDate = MAX_TIME_TS;
            }
        }

        if (startDate != null && endDate != null && startDate > endDate) {
            errors.append(String.format(ScheduleValidationMessages.PRECENDENCE_MESSAGE, ScheduleValidationMessages.START_DATE,
                scheduleDto.getStartDate(), ScheduleValidationMessages.END_DATE, scheduleDto.getEndDate()));
        }

        if (endDate != null && System.currentTimeMillis() > endDate) {
            errors.append(String.format(ScheduleValidationMessages.INVALID_END_DATE_MESSAGE,
                scheduleDto.getEndDate()));
        }
    }

    /**
     * Checks the validity of recurrence object of an input recurrence object when recurrence is
     * daily.
     *
     * @param recurrence the recurrence object that is validity is checked
     * @param errors validation messages, if any
     */
    private void verifyDailyRecurrence(@Nonnull final RecurrenceApiDTO recurrence,
                                       @Nonnull final StringBuilder errors) {
        if (recurrence.getDaysOfMonth() != null) {
            errors.append(String.format(ScheduleValidationMessages.INVALID_PARAM_MESSAGE, ScheduleValidationMessages.DAYS_OF_MONTH,
                ScheduleValidationMessages.DAILY_RECURRING_SCHEDULES));
        }
        if (recurrence.getDaysOfWeek() != null) {
            errors.append(String.format(ScheduleValidationMessages.INVALID_PARAM_MESSAGE, ScheduleValidationMessages.DAYS_OF_WEEK,
                ScheduleValidationMessages.DAILY_RECURRING_SCHEDULES));
        }
        if (recurrence.getWeekOfTheMonth() != null) {
            errors.append(String.format(ScheduleValidationMessages.INVALID_PARAM_MESSAGE, ScheduleValidationMessages.WEEK_OF_THE_MONTH,
                ScheduleValidationMessages.DAILY_RECURRING_SCHEDULES));
        }
    }

    /**
     * Checks the validity of recurrence object of an input recurrence object when recurrence is
     * weekly.
     *
     * @param recurrence the recurrence object that is validity is checked
     * @param errors validation messages, if any
     */
    private void verifyWeeklyRecurrence(@Nonnull final RecurrenceApiDTO recurrence,
                                        @Nonnull final StringBuilder errors) {
        if (recurrence.getDaysOfMonth() != null) {
            errors.append(String.format(ScheduleValidationMessages.INVALID_PARAM_MESSAGE,
                ScheduleValidationMessages.DAYS_OF_MONTH,
                ScheduleValidationMessages.WEEKLY_RECURRING_SCHEDULES));
        }
        if (recurrence.getWeekOfTheMonth() != null) {
            errors.append(String.format(ScheduleValidationMessages.INVALID_PARAM_MESSAGE,
                ScheduleValidationMessages.WEEK_OF_THE_MONTH,
                ScheduleValidationMessages.WEEKLY_RECURRING_SCHEDULES));
        }

        if (recurrence.getDaysOfWeek() == null || CollectionUtils.isEmpty(recurrence.getDaysOfWeek())) {
            errors.append(String.format(ScheduleValidationMessages.EMPTY_ARRAY_MESSAGE,
                ScheduleValidationMessages.DAYS_OF_WEEK, "weekly"));
            return;
        }

        if ((new HashSet<>(recurrence.getDaysOfWeek())).size() != recurrence.getDaysOfWeek()
            .size()) {
            errors.append(String.format(ScheduleValidationMessages.REPEATED_ENTRIES_MESSAGE,
                ScheduleValidationMessages.DAYS_OF_WEEK));
        }
    }

    /**
     * Checks the validity of recurrence object of an input recurrence object when recurrence is
     * monthly.
     *
     * @param recurrence the recurrence object that is validity is checked
     * @param errors validation messages, if any
     */
    private void verifyMonthlyRecurrence(@Nonnull final RecurrenceApiDTO recurrence,
                                         @Nonnull final StringBuilder errors) {
        // the monthly recurrence is defined by the day of the month
        if (recurrence.getDaysOfMonth() != null) {
            verifyMonthlyRecurrenceByDayOfMonth(recurrence, errors);
        } else if (recurrence.getWeekOfTheMonth() != null && recurrence.getDaysOfWeek() != null) {
            verifyMonthlyRecurrenceByDayOfWeek(recurrence, errors);
        } else {
            errors.append(ScheduleValidationMessages.INVALID_INPUTS_FOR_MONTHLY_RECURRENCE_MESSAGE);
        }
    }

    private void verifyMonthlyRecurrenceByDayOfMonth(@Nonnull final RecurrenceApiDTO recurrence,
                                                     @Nonnull final StringBuilder errors) {
        if (recurrence.getDaysOfWeek() != null) {
            errors.append(String.format(ScheduleValidationMessages.INVALID_PARAM_MESSAGE,
                ScheduleValidationMessages.DAYS_OF_WEEK,
                ScheduleValidationMessages.MONTHLY_RECURRENCE_WITH_DAY_OF_MONTH));
        }
        if (recurrence.getWeekOfTheMonth() != null) {
            errors.append(String.format(ScheduleValidationMessages.INVALID_PARAM_MESSAGE,
                ScheduleValidationMessages.WEEK_OF_THE_MONTH,
                ScheduleValidationMessages.MONTHLY_RECURRENCE_WITH_DAY_OF_MONTH));
        }

        if (CollectionUtils.isEmpty(recurrence.getDaysOfMonth())) {
            errors.append(String.format(ScheduleValidationMessages.EMPTY_ARRAY_MESSAGE,
                ScheduleValidationMessages.DAYS_OF_MONTH, ScheduleValidationMessages.MONTHLY));
        }

        if ((new HashSet<>(recurrence.getDaysOfMonth())).size() != recurrence.getDaysOfMonth()
            .size()) {
            errors.append(String.format(ScheduleValidationMessages.REPEATED_ENTRIES_MESSAGE,
                ScheduleValidationMessages.DAYS_OF_MONTH));
        }

        final boolean hasInvalidValues = recurrence.getDaysOfMonth().stream()
            .anyMatch(val -> val == null || val > 31 || val < -31 || val == 0);

        if (hasInvalidValues) {
            errors.append("The values specified for days of month should be in range of 1 to 31 and -31 to -1.\n");
            // the monthly recurrence is defined by the week of the month and day of the week
        }

    }

    private void verifyMonthlyRecurrenceByDayOfWeek(@Nonnull final RecurrenceApiDTO recurrence,
                                                     @Nonnull final StringBuilder errors) {
        if (CollectionUtils.isEmpty(recurrence.getWeekOfTheMonth())) {
            errors.append(String.format(ScheduleValidationMessages.EMPTY_ARRAY_MESSAGE,
                ScheduleValidationMessages.WEEK_OF_THE_MONTH, ScheduleValidationMessages.MONTHLY));
        }

        if (CollectionUtils.isEmpty(recurrence.getDaysOfWeek())) {
            errors.append(String.format(ScheduleValidationMessages.EMPTY_ARRAY_MESSAGE,
                ScheduleValidationMessages.DAYS_OF_WEEK, ScheduleValidationMessages.MONTHLY));
        }

        final boolean weekOfTheMonthHasInvalidValues = recurrence.getWeekOfTheMonth().stream()
            .anyMatch(val -> !ScheduleValidationMessages.VALID_WEEKS_OF_MONTH.contains(val));

        if (weekOfTheMonthHasInvalidValues) {
            errors.append("The values specified for weeks of month should be in range of 1 to 5 and -5 to -1.\n");
        }

        if ((new HashSet<>(recurrence.getWeekOfTheMonth())).size() != recurrence
            .getWeekOfTheMonth().size()) {
            errors.append(String.format(ScheduleValidationMessages.REPEATED_ENTRIES_MESSAGE,
                ScheduleValidationMessages.WEEK_OF_THE_MONTH));
        }

        if ((new HashSet<>(recurrence.getDaysOfWeek())).size() != recurrence.getDaysOfWeek()
            .size()) {
            errors.append(String.format(ScheduleValidationMessages.REPEATED_ENTRIES_MESSAGE,
                ScheduleValidationMessages.DAYS_OF_WEEK));
        }
    }

    /**
     * Validation error messages.
     */
    private static final class ScheduleValidationMessages {

        private static final String MISSING_PARAM_MESSAGE = "%s should always be set.\n";
        private static final String INVALID_TZ_MESSAGE = "%s is invalid timezone.\n";
        private static final String SAMPLE_TIME_FORMAT_MESSAGE =
            "The provided %s `%s` has wrong format. Use local time format: YYYY-MM-DDThh:mm (e.g 2015-10-07T12:38 or 2015-10-07). ";
        private static final String PRECENDENCE_MESSAGE = "The %s `%s` is not before %s `%s`.\n";
        private static final String INVALID_END_DATE_MESSAGE =
            "The end date `%s` is not after current time.\n";
        private static final String INVALID_PARAM_MESSAGE = "The %s should not be set for %s.\n";
        private static final String EMPTY_ARRAY_MESSAGE =
            "The %s should be set and be non-empty when the recurrence is %s.\n";
        private static final Set<Integer> VALID_WEEKS_OF_MONTH = ImmutableSet.of(-5, -4, -3, -2, -1, 1, 2, 3, 4, 5);
        private static final String INVALID_INPUTS_FOR_MONTHLY_RECURRENCE_MESSAGE =
            "When the monthly recurrence is chosen, the days of the month `daysOfMonth` or combination " +
                "of week of the month `weekOfTheMonth` and `daysOfWeek` should be set.\n";
        private static final String REPEATED_ENTRIES_MESSAGE = "There are repeated entries in the values " +
            "provided for %s.\n";
        private static final String WEEK_OF_THE_MONTH = " week of the month `weekOfTheMonth`";
        private static final String DAYS_OF_WEEK = " days of week `daysOfWeek`";
        private static final String START_TIME = " start time `startTime`";
        private static final String END_TIME = " end time `endTime`";
        private static final String START_DATE = " start of recurrence series `startDate`";
        private static final String END_DATE = " end of recurrence series `endDate`";
        private static final String DAYS_OF_MONTH = " days of month `daysOfMonth`";
        private static final String NON_RECURRENT_START_TIME_MUST_BE_IN_FUTURE =
            "The non-recurring schedule start time `startTime` should be in the future. ";
        private static final String NEGATIVE_RECURRENCE_INTERVAL_MESSAGE =
            "Recurrence interval should be bigger than zero. ";
        private static final String UNSUPPORTED_RECURRENCE_TYPE_MESSAGE =
            "Unsupported recurrence type %s";
        private static final String DAILY_RECURRING_SCHEDULES = "daily recurring schedules";
        private static final String WEEKLY_RECURRING_SCHEDULES = "weekly recurring schedules";
        private static final String MONTHLY_RECURRENCE_WITH_DAY_OF_MONTH =
            "monthly recurring schedules that have days of month defined";
        private static final String MONTHLY = "monthly";
        private static final String NON_RECURRING_SCHEDULES = "non recurring schedules";
        private static final String SCHEDULE_START_TIME = "Start time";
        private static final String SCHEDULE_END_TIME = "End time";
        private static final String SCHEDULE_START_DATE = "Start date";
        private static final String SCHEDULE_END_DATE = "End date";
        private static final String EARLIEST_START_TIME = MIN_TIME.format(DateTimeFormatter
            .ofPattern(DateTimeUtil.DEFAULT_DATE_PATTERN));
        private static final String EARLIEST_START_TIME_MESSAGE = "%s cannot be earlier than " +
            EARLIEST_START_TIME + ".\n";
    }
}
