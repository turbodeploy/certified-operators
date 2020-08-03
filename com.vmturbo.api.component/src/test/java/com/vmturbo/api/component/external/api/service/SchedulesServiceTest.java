package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;

import io.grpc.StatusRuntimeException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.component.external.api.mapper.ScheduleMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.dto.settingspolicy.RecurrenceApiDTO;
import com.vmturbo.api.dto.settingspolicy.ScheduleApiDTO;
import com.vmturbo.api.dto.settingspolicy.SettingsPolicyApiDTO;
import com.vmturbo.api.enums.DayOfWeek;
import com.vmturbo.api.enums.RecurrenceType;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.CreateScheduleRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.CreateScheduleResponse;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.DeleteScheduleRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.DeleteScheduleResponse;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.GetScheduleRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.GetScheduleResponse;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.GetSchedulesRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.UpdateScheduleRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.UpdateScheduleResponse;
import com.vmturbo.common.protobuf.schedule.ScheduleProtoMoles.ScheduleServiceMole;
import com.vmturbo.common.protobuf.schedule.ScheduleServiceGrpc;
import com.vmturbo.common.protobuf.schedule.ScheduleServiceGrpc.ScheduleServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPoliciesUsingScheduleRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Unit tests for {@link SchedulesService}.
 */
public class SchedulesServiceTest {
    private static final String NOT_RECURRING_SCHED_DISPLAY_NAME = "Schedule A";
    private static final String NOT_RECURRING_SCHED_TIME_ZONE = "UTC";
    private static final LocalDateTime NOT_RECURRING_SCHED_START_TIME_LOCAL = LocalDateTime.of(2037, 4, 1, 16, 30);
    private static final LocalDateTime NOT_RECURRING_SCHED_END_TIME_LOCAL = LocalDateTime.of(2037, 4, 2, 10, 30);

    private static final String MONTHLY_RECURRING_SCHED_DISPLAY_NAME = "Schedule B";
    private static final String MONTHLY_RECURRING_SCHED_TIME_ZONE = "UTC";
    private static final LocalDateTime MONTHLY_SCHED_START_LOCAL = LocalDateTime.of(2037, 6, 5, 0, 0);
    private static final LocalDateTime MONTHLY_SCHED_START_TIME_LOCAL = LocalDateTime.of(2037, 6, 5, 10, 20);
    private static final LocalDate MONTHLY_SCHED_END_LOCAL = LocalDate.of(2037, 6, 6);
    private static final LocalDateTime MONTHLY_SCHED_END_TIME_LOCAL = LocalDateTime.of(2037, 6, 5, 11, 20);

    private static final String MONTHLY_RECURRING_SCHED2_DISPLAY_NAME = "Schedule C";
    private static final String MONTHLY_RECURRING_SCHED2_TIME_ZONE = "UTC";
    private static final LocalDateTime MONTHLY_SCHED2_START_LOCAL = LocalDateTime.of(2037, 6, 5, 0, 0);
    private static final LocalDateTime MONTHLY_SCHED2_START_TIME_LOCAL = LocalDateTime.of(2037, 6, 5, 10, 20);
    private static final LocalDate MONTHLY_SCHED2_END_LOCAL = LocalDate.of(2037, 6, 6);
    private static final LocalDateTime MONTHLY_SCHED2_END_TIME_LOCAL = LocalDateTime.of(2037, 6, 5, 11, 20);

    private static final String WEEKLY_RECURRING_SCHED_DISPLAY_NAME = "Schedule D";
    private static final String WEEKLY_RECURRING_SCHED_TIME_ZONE = "UTC";
    private static final LocalDateTime WEEKLY_SCHED_START_LOCAL = LocalDateTime.of(2037, 6, 5, 0, 0);
    private static final LocalDateTime WEEKLY_SCHED_START_TIME_LOCAL = LocalDateTime.of(2037, 6, 5, 10, 20);
    private static final LocalDate WEEKLY_SCHED_END_LOCAL = LocalDate.of(2037, 6, 6);
    private static final LocalDateTime WEEKLY_SCHED_END_TIME_LOCAL = LocalDateTime.of(2037, 6, 5, 11, 20);

    private static final String DAILY_RECURRING_SCHED_DISPLAY_NAME = "Schedule E";
    private static final String DAILY_RECURRING_SCHED_TIME_ZONE = "UTC";
    private static final LocalDateTime DAILY_SCHED_START_LOCAL = LocalDateTime.of(2037, 6, 5, 0, 0);
    private static final LocalDateTime DAILY_SCHED_START_TIME_LOCAL = LocalDateTime.of(2037, 6, 5, 10, 20);
    private static final LocalDateTime DAILY_SCHED_END_LOCAL = LocalDateTime.of(2037, 6, 6, 0, 0);
    private static final LocalDateTime DAILY_SCHED_END_TIME_LOCAL = LocalDateTime.of(2037, 6, 5, 11, 20);

    private static final LocalDateTime PAST_START_TIME_LOCAL = LocalDateTime.of(1984, 4, 1, 16, 30);
    private static final LocalDateTime PAST_DATE_LOCAL = LocalDateTime.of(1984, 6, 5, 0, 0);

    private static final long ID = 11L;
    private static final Schedule SCHEDULE = Schedule.getDefaultInstance();
    private static final SettingPolicy SETTING_POLICY = SettingPolicy.getDefaultInstance();
    private static final ScheduleApiDTO SCHEDULE_API_DTO = new ScheduleApiDTO();
    private static final SettingsPolicyApiDTO SETTINGS_POLICY_API_DTO = new SettingsPolicyApiDTO();
    private final ScheduleMapper scheduleMapper = mock(ScheduleMapper.class);
    private final SettingsMapper settingsMapper = mock(SettingsMapper.class);
    private final ActionSearchUtil actionSearchUtil = Mockito.mock(ActionSearchUtil.class);
    /** Expected exceptions to test against. */
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private ScheduleServiceMole grpcScheduleService = spy(new ScheduleServiceMole());
    private SettingPolicyServiceMole grpcSettingPolicyService = spy(new SettingPolicyServiceMole());
    /**
     * Grpc server mock.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(grpcScheduleService,
        grpcSettingPolicyService);
    private ScheduleServiceBlockingStub scheduleServiceStub;
    private SettingPolicyServiceBlockingStub settingPolicyServiceStub;
    private SchedulesService schedulesService;

    /**
     * Test setup.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        scheduleServiceStub = ScheduleServiceGrpc.newBlockingStub(grpcServer.getChannel());
        settingPolicyServiceStub = SettingPolicyServiceGrpc.newBlockingStub(grpcServer.getChannel());
        schedulesService = spy(new SchedulesService(scheduleServiceStub, settingPolicyServiceStub,
            scheduleMapper, settingsMapper, actionSearchUtil));
    }

    private ScheduleApiDTO createNonRecurringScheduleDTO() {
        ScheduleApiDTO scheduleDTO = new ScheduleApiDTO();
        scheduleDTO.setDisplayName(NOT_RECURRING_SCHED_DISPLAY_NAME);
        scheduleDTO.setStartTime(NOT_RECURRING_SCHED_START_TIME_LOCAL);
        scheduleDTO.setEndTime(NOT_RECURRING_SCHED_END_TIME_LOCAL);
        scheduleDTO.setTimeZone(NOT_RECURRING_SCHED_TIME_ZONE);

        return scheduleDTO;
    }

    private ScheduleApiDTO createMonthlyRecurringScheduleDTOBasedOnWeek() {
        ScheduleApiDTO scheduleDTO = new ScheduleApiDTO();
        scheduleDTO.setDisplayName(MONTHLY_RECURRING_SCHED_DISPLAY_NAME);
        scheduleDTO.setStartDate(MONTHLY_SCHED_START_LOCAL);
        scheduleDTO.setStartTime(MONTHLY_SCHED_START_TIME_LOCAL);
        scheduleDTO.setEndDate(MONTHLY_SCHED_END_LOCAL);
        scheduleDTO.setEndTime(MONTHLY_SCHED_END_TIME_LOCAL);
        scheduleDTO.setTimeZone(MONTHLY_RECURRING_SCHED_TIME_ZONE);

        RecurrenceApiDTO recurrenceDTO = new RecurrenceApiDTO();
        recurrenceDTO.setType(RecurrenceType.MONTHLY);
        recurrenceDTO.setInterval(2);
        recurrenceDTO.setWeekOfTheMonth(ImmutableList.of(3));
        recurrenceDTO.setDaysOfWeek(ImmutableList.of(DayOfWeek.Wed));
        scheduleDTO.setRecurrence(recurrenceDTO);

        return scheduleDTO;
    }

    private ScheduleApiDTO createMonthlyRecurringScheduleDTOBasedOnDay() {
        ScheduleApiDTO scheduleDTO = new ScheduleApiDTO();
        scheduleDTO.setDisplayName(MONTHLY_RECURRING_SCHED2_DISPLAY_NAME);
        scheduleDTO.setStartDate(MONTHLY_SCHED2_START_LOCAL);
        scheduleDTO.setStartTime(MONTHLY_SCHED2_START_TIME_LOCAL);
        scheduleDTO.setEndDate(MONTHLY_SCHED2_END_LOCAL);
        scheduleDTO.setEndTime(MONTHLY_SCHED2_END_TIME_LOCAL);
        scheduleDTO.setTimeZone(MONTHLY_RECURRING_SCHED2_TIME_ZONE);

        RecurrenceApiDTO recurrenceDTO = new RecurrenceApiDTO();
        recurrenceDTO.setType(RecurrenceType.MONTHLY);
        recurrenceDTO.setInterval(1);
        recurrenceDTO.setDaysOfMonth(ImmutableList.of(6));
        scheduleDTO.setRecurrence(recurrenceDTO);

        return scheduleDTO;
    }

    private ScheduleApiDTO createWeeklyRecurringScheduleDTO() {
        ScheduleApiDTO scheduleDTO = new ScheduleApiDTO();
        scheduleDTO.setDisplayName(WEEKLY_RECURRING_SCHED_DISPLAY_NAME);
        scheduleDTO.setStartDate(WEEKLY_SCHED_START_LOCAL);
        scheduleDTO.setStartTime(WEEKLY_SCHED_START_TIME_LOCAL);
        scheduleDTO.setEndDate(WEEKLY_SCHED_END_LOCAL);
        scheduleDTO.setEndTime(WEEKLY_SCHED_END_TIME_LOCAL);
        scheduleDTO.setTimeZone(WEEKLY_RECURRING_SCHED_TIME_ZONE);

        RecurrenceApiDTO recurrenceDTO = new RecurrenceApiDTO();
        recurrenceDTO.setType(RecurrenceType.WEEKLY);
        recurrenceDTO.setInterval(2);
        recurrenceDTO.setDaysOfWeek(ImmutableList.of(DayOfWeek.Sun, DayOfWeek.Tue));
        scheduleDTO.setRecurrence(recurrenceDTO);

        return scheduleDTO;
    }

    private ScheduleApiDTO createDailyRecurringScheduleDTO() {
        ScheduleApiDTO scheduleDTO = new ScheduleApiDTO();
        scheduleDTO.setDisplayName(DAILY_RECURRING_SCHED_DISPLAY_NAME);
        scheduleDTO.setStartDate(DAILY_SCHED_START_LOCAL);
        scheduleDTO.setStartTime(DAILY_SCHED_START_TIME_LOCAL);
        scheduleDTO.setEndDate(DAILY_SCHED_END_LOCAL.toLocalDate());
        scheduleDTO.setEndTime(DAILY_SCHED_END_TIME_LOCAL);
        scheduleDTO.setTimeZone(DAILY_RECURRING_SCHED_TIME_ZONE);

        RecurrenceApiDTO recurrenceDTO = new RecurrenceApiDTO();
        recurrenceDTO.setType(RecurrenceType.DAILY);
        recurrenceDTO.setInterval(3);
        scheduleDTO.setRecurrence(recurrenceDTO);

        return scheduleDTO;
    }


    /**
     * Test get schedules.
     *
     * @throws Exception If any unexpected exception.
     */
    @Test
    public void testGetSchedules() throws Exception {
        doReturn(Collections.singletonList(SCHEDULE)).when(grpcScheduleService)
            .getSchedules(any(GetSchedulesRequest.class));
        doReturn(Collections.singletonList(SCHEDULE_API_DTO)).when(scheduleMapper)
            .convertSchedules(any(List.class));

        List<ScheduleApiDTO> ret = schedulesService.getSchedules();
        assertEquals(1, ret.size());
        assertThat(ret, containsInAnyOrder(SCHEDULE_API_DTO));
    }

    /**
     * Test get schedules throws exception if grpc call fails.
     *
     * @throws Exception If any unexpected exception.
     */
    @Test
    public void testGetSchedulesThrowsException() throws Exception {
        doThrow(StatusRuntimeException.class).when(grpcScheduleService)
            .getSchedules(any(GetSchedulesRequest.class));
        doReturn(Collections.singletonList(SCHEDULE_API_DTO)).when(scheduleMapper)
            .convertSchedules(any(List.class));

        thrown.expect(OperationFailedException.class);
        schedulesService.getSchedules();
    }

    /**
     * Test get schedule.
     *
     * @throws Exception If any unexpected exception.
     */
    @Test
    public void testGetSchedule() throws Exception {
        doReturn(GetScheduleResponse.newBuilder().setSchedule(SCHEDULE).build())
            .when(grpcScheduleService).getSchedule(GetScheduleRequest.newBuilder().setOid(ID)
            .build());
        doReturn(SCHEDULE_API_DTO).when(scheduleMapper).convertSchedule(SCHEDULE);

        ScheduleApiDTO retDto = schedulesService.getSchedule(String.valueOf(ID));
        assertEquals(SCHEDULE_API_DTO, retDto);
    }

    /**
     * Test get schedule throws exception if grpc call fails.
     *
     * @throws Exception If any unexpected exception.
     */
    @Test
    public void testGetScheduleThrowsException() throws Exception {
        doThrow(StatusRuntimeException.class)
            .when(grpcScheduleService).getSchedule(GetScheduleRequest.newBuilder().setOid(ID)
            .build());
        doReturn(SCHEDULE_API_DTO).when(scheduleMapper).convertSchedule(SCHEDULE);

        thrown.expect(OperationFailedException.class);
        schedulesService.getSchedule(String.valueOf(ID));
    }

    /**
     * Test create schedule.
     *
     * @throws Exception If any unexpected exception.
     */
    @Test
    public void testAddSchedule() throws Exception {
        doReturn(CreateScheduleResponse.newBuilder().setSchedule(SCHEDULE).build())
            .when(grpcScheduleService).createSchedule(CreateScheduleRequest.newBuilder()
            .setSchedule(SCHEDULE)
            .build());
        doReturn(SCHEDULE).when(scheduleMapper).convertInput(SCHEDULE_API_DTO);
        doReturn(SCHEDULE_API_DTO).when(scheduleMapper).convertSchedule(SCHEDULE);

        ScheduleApiDTO retDto = schedulesService.addSchedule(SCHEDULE_API_DTO);
        assertEquals(SCHEDULE_API_DTO, retDto);
    }

    /**
     * Test create schedule throws exception if grpc call fails.
     *
     * @throws Exception If any unexpected exception.
     */
    @Test
    public void testAddScheduleThrowsException() throws Exception {
        doThrow(StatusRuntimeException.class)
            .when(grpcScheduleService).createSchedule(CreateScheduleRequest.newBuilder()
            .setSchedule(SCHEDULE)
            .build());
        doReturn(SCHEDULE).when(scheduleMapper).convertInput(SCHEDULE_API_DTO);
        doReturn(SCHEDULE_API_DTO).when(scheduleMapper).convertSchedule(SCHEDULE);

        thrown.expect(OperationFailedException.class);
        schedulesService.addSchedule(SCHEDULE_API_DTO);
    }

    /**
     * Test update schedule.
     *
     * @throws Exception If any unexpected exception.
     */
    @Test
    public void testUpdateSchedule() throws Exception {
        doReturn(UpdateScheduleResponse.newBuilder().setSchedule(SCHEDULE).build())
            .when(grpcScheduleService).updateSchedule(UpdateScheduleRequest.newBuilder()
            .setOid(ID)
            .setUpdatedSchedule(SCHEDULE)
            .build());
        doReturn(SCHEDULE).when(scheduleMapper).convertInput(SCHEDULE_API_DTO);
        doReturn(SCHEDULE_API_DTO).when(scheduleMapper).convertSchedule(SCHEDULE);

        ScheduleApiDTO retDto = schedulesService.editSchedule(String.valueOf(ID), SCHEDULE_API_DTO);
        assertEquals(SCHEDULE_API_DTO, retDto);
    }

    /**
     * Test update schedule throws exception if grpc call fails.
     *
     * @throws Exception If any unexpected exception.
     */
    @Test
    public void testUpdateScheduleThrowsException() throws Exception {
        doThrow(StatusRuntimeException.class)
            .when(grpcScheduleService).updateSchedule(UpdateScheduleRequest.newBuilder()
            .setOid(ID)
            .setUpdatedSchedule(SCHEDULE)
            .build());
        doReturn(SCHEDULE).when(scheduleMapper).convertInput(SCHEDULE_API_DTO);
        doReturn(SCHEDULE_API_DTO).when(scheduleMapper).convertSchedule(SCHEDULE);

        thrown.expect(OperationFailedException.class);
        schedulesService.editSchedule(String.valueOf(ID), SCHEDULE_API_DTO);
    }

    /**
     * Test delete schedule.
     *
     * @throws Exception If any unexpected exception.
     */
    @Test
    public void testDeleteSchedule() throws Exception {
        doReturn(DeleteScheduleResponse.newBuilder().setSchedule(SCHEDULE).build())
            .when(grpcScheduleService).deleteSchedule(DeleteScheduleRequest.newBuilder()
            .setOid(ID)
            .build());
        doReturn(SCHEDULE).when(scheduleMapper).convertInput(SCHEDULE_API_DTO);

        schedulesService.deleteSchedule(String.valueOf(ID));
    }

    /**
     * Test delete schedule.
     *
     * @throws Exception If any unexpected exception.
     */
    @Test
    public void testDeleteScheduleThrowsException() throws Exception {
        doThrow(StatusRuntimeException.class)
            .when(grpcScheduleService).deleteSchedule(DeleteScheduleRequest.newBuilder()
            .setOid(ID)
            .build());
        doReturn(SCHEDULE).when(scheduleMapper).convertInput(SCHEDULE_API_DTO);

        thrown.expect(OperationFailedException.class);
        schedulesService.deleteSchedule(String.valueOf(ID));
    }

    /**
     * Test get setting polciies using the schedule.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void testGetPoliciesUsingTheSchedule() throws Exception {
        List<SettingPolicy> settingPolicies = Arrays.asList(SETTING_POLICY, SETTING_POLICY);
        doReturn(settingPolicies)
            .when(grpcSettingPolicyService).getSettingPoliciesUsingSchedule(
                GetSettingPoliciesUsingScheduleRequest.newBuilder()
            .setScheduleId(ID)
            .build());
        doReturn(Arrays.asList(SETTINGS_POLICY_API_DTO, SETTINGS_POLICY_API_DTO))
            .when(settingsMapper).convertSettingPolicies(settingPolicies);
        doReturn(GetScheduleResponse.newBuilder().setSchedule(SCHEDULE).build())
                .when(grpcScheduleService).getSchedule(GetScheduleRequest.newBuilder().setOid(ID)
                .build());

        List<SettingsPolicyApiDTO> retList = schedulesService.getPoliciesUsingTheSchedule(
            String.valueOf(ID));
        assertEquals(2, retList.size());
    }


    /**
     * Validate non-recurring schedule.
     */
    @Test
    public void testValidateInputNonReccuringSchedule() {
        ScheduleApiDTO scheduleDTO = createNonRecurringScheduleDTO();
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate daily recurring schedule.
     */
    @Test
    public void testValidateInputDailyReccuringSchedule() {
        ScheduleApiDTO scheduleDTO = createDailyRecurringScheduleDTO();
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate weekly recurring schedule.
     */
    @Test
    public void testValidateInputWeeklyReccuringSchedule() {
        ScheduleApiDTO scheduleDTO = createWeeklyRecurringScheduleDTO();
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate monthly recurring schedule with week of month.
     */
    @Test
    public void testValidateInputMonthlyReccuringWeekOfMonthSchedule() {
        ScheduleApiDTO scheduleDTO = createMonthlyRecurringScheduleDTOBasedOnWeek();
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate monthly recurring schedule with day of month.
     */
    @Test
    public void testValidateInputMonthlyReccuringDayOfMonthSchedule() {
        ScheduleApiDTO scheduleDTO = createMonthlyRecurringScheduleDTOBasedOnDay();
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate display name is required.
     */
    @Test
    public void testValidateInputNullDisplayName() {
        ScheduleApiDTO scheduleDTO = createDailyRecurringScheduleDTO();
        scheduleDTO.setDisplayName(null);
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate display name is required.
     */
    @Test
    public void testValidateInputEmptyDisplayName() {
        ScheduleApiDTO scheduleDTO = createDailyRecurringScheduleDTO();
        scheduleDTO.setDisplayName("");
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate start time is required.
     */
    @Test
    public void testValidateInputEmptyStartTime() {
        ScheduleApiDTO scheduleDTO = createDailyRecurringScheduleDTO();
        scheduleDTO.setStartTime(null);
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate end time is required.
     */
    @Test
    public void testValidateInputEmptyEndTime() {
        ScheduleApiDTO scheduleDTO = createDailyRecurringScheduleDTO();
        scheduleDTO.setEndTime(null);
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate start time is before end time.
     */
    @Test
    public void testValidateInputEndTimeBeforeStartTime() {
        ScheduleApiDTO scheduleDTO = createDailyRecurringScheduleDTO();
        scheduleDTO.setStartTime(DAILY_SCHED_END_TIME_LOCAL);
        scheduleDTO.setEndTime(DAILY_SCHED_START_TIME_LOCAL);
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate start time is after allowed min time.
     */
    @Test
    public void testValidateInputMinStartTime() {
        ScheduleApiDTO scheduleDTO = createDailyRecurringScheduleDTO();
        scheduleDTO.setStartTime(LocalDateTime.of(1960,01,01,0,0,
            0).atOffset(ZoneOffset.UTC).toLocalDateTime());
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate start date is after allowed min time.
     */
    @Test
    public void testValidateInputMinStartDate() {
        ScheduleApiDTO scheduleDTO = createDailyRecurringScheduleDTO();
        scheduleDTO.setStartDate(LocalDateTime.of(1960,01,01,0,0,
            0).atOffset(ZoneOffset.UTC).toLocalDateTime());
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate end time is before allowed max time.
     */
    @Test
    public void testValidateInputMaxEndTime() {
        ScheduleApiDTO scheduleDTO = createDailyRecurringScheduleDTO();
        scheduleDTO.setEndTime(LocalDateTime.of(2050,01,01,0,0,
            0).atOffset(ZoneOffset.UTC).toLocalDateTime());
        schedulesService.validateInput(scheduleDTO, null);
        assertEquals(Instant.ofEpochMilli(SchedulesService.MAX_TIME_TS).atOffset(ZoneOffset.UTC)
            .toLocalDateTime(), scheduleDTO.getEndTime());
    }

    /**
     * Validate end time is before allowed max time.
     */
    @Test
    public void testValidateInputMaxEndDate() {
        ScheduleApiDTO scheduleDTO = createDailyRecurringScheduleDTO();
        scheduleDTO.setEndDate(LocalDateTime.of(2050,01,01,0,0,
            0).atOffset(ZoneOffset.UTC).toLocalDate());
        schedulesService.validateInput(scheduleDTO, null);
        assertEquals(Instant.ofEpochMilli(SchedulesService.MAX_TIME_TS).atOffset(ZoneOffset.UTC)
        .minusDays(1).toLocalDate(), scheduleDTO.getEndDate());
    }

    /**
     * Validate non-recurring schedules cannot start in the past.
     */
    @Test
    public void testValidateInputNonRecurringStartInThePast() {
        ScheduleApiDTO scheduleDTO = createNonRecurringScheduleDTO();
        scheduleDTO.setStartTime(PAST_START_TIME_LOCAL);
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate timezone is required.
     */
    @Test
    public void testValidateInputNoTimeZone() {
        ScheduleApiDTO scheduleDTO = createNonRecurringScheduleDTO();
        scheduleDTO.setTimeZone(null);
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate timezone is valid.
     */
    @Test
    public void testValidateInputInvalidTimezoneID() {
        ScheduleApiDTO scheduleDTO = createNonRecurringScheduleDTO();
        scheduleDTO.setTimeZone("Invalid TZ");
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate recurrence type.
     */
    @Test
    public void testValidateInputNullRecurrenceType() {
        ScheduleApiDTO scheduleDTO = createDailyRecurringScheduleDTO();
        scheduleDTO.getRecurrence().setType(null);
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate recurrence interval.
     */
    @Test
    public void testValidateInputZeroInterval() {
        ScheduleApiDTO scheduleDTO = createDailyRecurringScheduleDTO();
        scheduleDTO.getRecurrence().setInterval(0);
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Test validate recurrence start date is before end date.
     */
    @Test
    public void testValidateInputEndDateBeforeStartDate() {
        ScheduleApiDTO scheduleDTO = createDailyRecurringScheduleDTO();
        scheduleDTO.setStartDate(DAILY_SCHED_END_LOCAL);
        scheduleDTO.setEndDate(DAILY_SCHED_START_LOCAL.toLocalDate());
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate recurring schedule end date is not in the past.
     */
    @Test
    public void testValidateInputEndDateBeforeNow() {
        ScheduleApiDTO scheduleDTO = createDailyRecurringScheduleDTO();
        scheduleDTO.setStartTime(DAILY_SCHED_START_LOCAL);
        scheduleDTO.setEndTime(DAILY_SCHED_END_LOCAL);
        scheduleDTO.setEndDate(PAST_DATE_LOCAL.toLocalDate());
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate recurring days of month is valid.
     */
    @Test
    public void testValidateDailyRecurrenceWithDayOfMonthSet() {
        ScheduleApiDTO scheduleDTO = createDailyRecurringScheduleDTO();
        scheduleDTO.getRecurrence().setDaysOfMonth(Collections.emptyList());
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate recurring weeks of month is valid.
     */
    @Test
    public void testValidateDailyRecurrenceWithWeekOfMonthSet() {
        ScheduleApiDTO scheduleDTO = createDailyRecurringScheduleDTO();
        scheduleDTO.getRecurrence().setWeekOfTheMonth(Collections.emptyList());
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate recurring days of week is valid.
     */
    @Test
    public void testValidateDailyRecurrenceWithDayOfWeekSet() {
        ScheduleApiDTO scheduleDTO = createDailyRecurringScheduleDTO();
        scheduleDTO.getRecurrence().setDaysOfWeek(Collections.emptyList());
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate recurring days of week is valid.
     */
    @Test
    public void testValidateWeekRecurrenceWithNoDaysOfWeek() {
        ScheduleApiDTO scheduleDTO = createWeeklyRecurringScheduleDTO();
        scheduleDTO.getRecurrence().setDaysOfWeek(null);
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate recurring days of week is valid.
     */
    @Test
    public void testValidateWeekRecurrenceWithEmptyDaysOfWeek() {
        ScheduleApiDTO scheduleDTO = createWeeklyRecurringScheduleDTO();
        scheduleDTO.getRecurrence().setDaysOfWeek(Collections.emptyList());
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate recurring days of week is valid.
     */
    @Test
    public void testValidateWeekRecurrenceWithRepeatedDaysOfWeek() {
        ScheduleApiDTO scheduleDTO = createWeeklyRecurringScheduleDTO();
        scheduleDTO.getRecurrence().setDaysOfWeek(ImmutableList.of(DayOfWeek.Sun, DayOfWeek.Sun));
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate days of month is valid.
     */
    @Test
    public void testValidateWeeklyRecurrenceWithDayOfMonthSet() {
        ScheduleApiDTO scheduleDTO = createWeeklyRecurringScheduleDTO();
        scheduleDTO.getRecurrence().setDaysOfMonth(Collections.emptyList());
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate weeks of month is valid.
     */
    @Test
    public void testValidateWeeklyRecurrenceWithWeekOfMonthSet() {
        ScheduleApiDTO scheduleDTO = createWeeklyRecurringScheduleDTO();
        scheduleDTO.getRecurrence().setWeekOfTheMonth(Collections.emptyList());
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate weeks of month is valid.
     */
    @Test
    public void testValidateMonthlyRecurrenceBasedOnDayWithWeekOfMonthSet() {
        ScheduleApiDTO scheduleDTO = createMonthlyRecurringScheduleDTOBasedOnDay();
        scheduleDTO.getRecurrence().setWeekOfTheMonth(Collections.emptyList());
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate weeks of month is valid.
     */
    @Test
    public void testValidateMonthlyRecurrenceBasedOnDayWithDaysOfWeekSet() {
        ScheduleApiDTO scheduleDTO = createMonthlyRecurringScheduleDTOBasedOnDay();
        scheduleDTO.getRecurrence().setDaysOfWeek(Collections.emptyList());
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate days of month is valid.
     */
    @Test
    public void testValidateMonthlyRecurrenceBasedOnDayWithEmptyDaysOfMonth() {
        ScheduleApiDTO scheduleDTO = createMonthlyRecurringScheduleDTOBasedOnDay();
        scheduleDTO.getRecurrence().setDaysOfMonth(Collections.emptyList());
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate days of month is valid for monthly recurring.
     */
    @Test
    public void testValidateMonthlyRecurrenceBasedOnDayWithNullDaysOfMonth() {
        ScheduleApiDTO scheduleDTO = createMonthlyRecurringScheduleDTOBasedOnDay();
        scheduleDTO.getRecurrence().setDaysOfMonth(Collections.singletonList(null));
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate days of month is valid for monthly recurring.
     */
    @Test
    public void testValidateMonthlyRecurrenceBasedOnDayWithInvalidDaysOfMonth() {
        ScheduleApiDTO scheduleDTO = createMonthlyRecurringScheduleDTOBasedOnDay();
        scheduleDTO.getRecurrence().setDaysOfMonth(ImmutableList.of(35));
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate days of month is valid for monthly recurring.
     */
    @Test
    public void testValidateMonthlyRecurrenceBasedOnDayWithRepeatedDaysOfMonth() {
        ScheduleApiDTO scheduleDTO = createMonthlyRecurringScheduleDTOBasedOnDay();
        scheduleDTO.getRecurrence().setDaysOfMonth(ImmutableList.of(2, 2));
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate weeks of month is valid for monthly recurring.
     */
    @Test
    public void testValidateMonthlyRecurrenceBasedWeekWithNoWeekOfMonth() {
        ScheduleApiDTO scheduleDTO = createMonthlyRecurringScheduleDTOBasedOnWeek();
        scheduleDTO.getRecurrence().setWeekOfTheMonth(null);
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate weeks of month is valid for monthly recurring.
     */
    @Test
    public void testValidateMonthlyRecurrenceBasedWeekWithEmptyWeekOfMonth() {
        ScheduleApiDTO scheduleDTO = createMonthlyRecurringScheduleDTOBasedOnWeek();
        scheduleDTO.getRecurrence().setWeekOfTheMonth(Collections.emptyList());
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate weeks of month is valid for monthly recurring.
     */
    @Test
    public void testValidateMonthlyRecurrenceBasedWeekWithEmptyDaysOfWeek() {
        ScheduleApiDTO scheduleDTO = createMonthlyRecurringScheduleDTOBasedOnWeek();
        scheduleDTO.getRecurrence().setDaysOfWeek(Collections.emptyList());
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate weeks of month is valid for monthly recurring.
     */
    @Test
    public void testValidateMonthlyRecurrenceBasedWeekWithInvalidWeekOfMonth() {
        ScheduleApiDTO scheduleDTO = createMonthlyRecurringScheduleDTOBasedOnWeek();
        scheduleDTO.getRecurrence().setWeekOfTheMonth(ImmutableList.of(0));
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate weeks of month is valid for monthly recurring.
     */
    @Test
    public void testValidateMonthlyRecurrenceBasedWeekWithRepeatedWeekOfMonth() {
        ScheduleApiDTO scheduleDTO = createMonthlyRecurringScheduleDTOBasedOnWeek();
        scheduleDTO.getRecurrence().setWeekOfTheMonth(ImmutableList.of(1, 1));
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate days of week is valid for monthly recurring.
     */
    @Test
    public void testValidateMonthlyRecurrenceBasedWeekWithRepeatedDaysOfWeek() {
        ScheduleApiDTO scheduleDTO = createMonthlyRecurringScheduleDTOBasedOnWeek();
        scheduleDTO.getRecurrence().setDaysOfWeek(ImmutableList.of(DayOfWeek.Sun, DayOfWeek.Sun));
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate non-recurring with start date.
     */
    @Test
    public void testValidateNonRecurringScheduleWithStartDate() {
        ScheduleApiDTO scheduleDTO = createNonRecurringScheduleDTO();
        scheduleDTO.setStartDate(NOT_RECURRING_SCHED_START_TIME_LOCAL);
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate non-recurring with end date.
     */
    @Test
    public void testValidateNonRecurringScheduleWithEndDate() {
        ScheduleApiDTO scheduleDTO = createNonRecurringScheduleDTO();
        scheduleDTO.setEndDate(NOT_RECURRING_SCHED_END_TIME_LOCAL.toLocalDate());
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(scheduleDTO, null);
    }

    /**
     * Validate invalid ScheduleApiDto object type.
     */
    @Test
    public void testValidateInvalidObjectType() {
        thrown.expect(IllegalArgumentException.class);
        schedulesService.validateInput(new Object(), null);
    }
}
