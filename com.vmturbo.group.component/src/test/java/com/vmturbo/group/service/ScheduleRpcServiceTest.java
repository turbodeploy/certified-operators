package com.vmturbo.group.service;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;

import com.vmturbo.common.protobuf.schedule.ScheduleProto.CreateScheduleRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.CreateScheduleResponse;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.DeleteScheduleRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.DeleteScheduleResponse;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.DeleteSchedulesRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.DeleteSchedulesResponse;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.GetScheduleRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.GetScheduleResponse;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.GetSchedulesRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.Schedule.OneTime;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.UpdateScheduleRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.UpdateScheduleResponse;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.InvalidItemException;
import com.vmturbo.group.common.ItemDeleteException.ScheduleInUseDeleteException;
import com.vmturbo.group.common.ItemNotFoundException.ScheduleNotFoundException;
import com.vmturbo.group.schedule.ScheduleStore;

/**
 * Unit test for {@link ScheduleRpcService}.
 */
public class ScheduleRpcServiceTest {
    private static final long SCHEDULE_ID = 1L;
    private static final long SCHEDULE_ID_2 = 2L;
    private static final long START_TIME = Instant.parse("2010-01-10T09:30:00Z").toEpochMilli();
    private static final long END_TIME = Instant.parse("2010-01-10T15:00:00Z").toEpochMilli();
    private static  final Schedule SCHEDULE = Schedule.newBuilder()
        .setId(SCHEDULE_ID)
        .setStartTime(START_TIME)
        .setEndTime(END_TIME)
        .setOneTime(OneTime.newBuilder().build())
        .setTimezoneId(ZoneId.systemDefault().getId())
        .build();

    /** Expected exceptions to test against. */
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private ScheduleStore scheduleStore = mock(ScheduleStore.class);

    private ScheduleRpcService scheduleRpcService;

    /**
     * Tests setup.
     */
    @Before
    public void setup() {
        scheduleRpcService = new ScheduleRpcService(scheduleStore);
    }

    private void verifyResults(final Schedule expected, final Schedule actual) {
        assertEquals(expected.getId(), actual.getId());
        assertEquals(expected.getStartTime(), actual.getStartTime());
        assertEquals(expected.getEndTime(), actual.getEndTime());
    }

    /**
     * Test get schedule.
     */
    @Test
    public void testGetSchedule() {
        when(scheduleStore.getSchedule(SCHEDULE_ID)).thenReturn(Optional.of(SCHEDULE));
        final StreamObserver<GetScheduleResponse> responseObserver =
            (StreamObserver<GetScheduleResponse>)mock(StreamObserver.class);

        scheduleRpcService.getSchedule(GetScheduleRequest.newBuilder()
            .setOid(SCHEDULE_ID).build(), responseObserver);

        final ArgumentCaptor<GetScheduleResponse> responseCaptor =
            ArgumentCaptor.forClass(GetScheduleResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        verifyResults(SCHEDULE, responseCaptor.getValue().getSchedule());
    }

    /**
     * Test get schedule.
     */
    @Test
    public void testGetScheduleWitRefTime() {
        when(scheduleStore.getSchedule(SCHEDULE_ID)).thenReturn(Optional.of(SCHEDULE));
        final StreamObserver<GetScheduleResponse> responseObserver =
            (StreamObserver<GetScheduleResponse>)mock(StreamObserver.class);

        scheduleRpcService.getSchedule(GetScheduleRequest.newBuilder()
            .setOid(SCHEDULE_ID)
            .setRefTime(Instant.parse("2010-01-10T09:30:00Z").toEpochMilli())
            .build(), responseObserver);

        final ArgumentCaptor<GetScheduleResponse> responseCaptor =
            ArgumentCaptor.forClass(GetScheduleResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        verifyResults(SCHEDULE, responseCaptor.getValue().getSchedule());
    }

    /**
     * Test get missing schedule generates error.
     */
    @Test
    public void testGetScheduleNotFound() {
        when(scheduleStore.getSchedule(SCHEDULE_ID)).thenReturn(Optional.empty());
        final StreamObserver<GetScheduleResponse> responseObserver =
            (StreamObserver<GetScheduleResponse>)mock(StreamObserver.class);

        scheduleRpcService.getSchedule(GetScheduleRequest.newBuilder()
            .setOid(SCHEDULE_ID).build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());
    }

    /**
     * Test get schedule with missing ID in request generates error.
     */
    @Test
    public void testGetScheduleMissingId() {
        when(scheduleStore.getSchedule(SCHEDULE_ID)).thenReturn(Optional.of(SCHEDULE));
        final StreamObserver<GetScheduleResponse> responseObserver =
            (StreamObserver<GetScheduleResponse>)mock(StreamObserver.class);

        scheduleRpcService.getSchedule(GetScheduleRequest.newBuilder().build(), responseObserver);
        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());
    }

    /**
     * Test get schedule with invalid recurring rule generates error.
     */
    @Test
    public void testGetScheduleParseException() {
        Schedule testSchedule = SCHEDULE.toBuilder()
            .clearOneTime()
            .setRecurRule("FREQ=DAILY;INTERVAL=2;UNTIL=INVALID").build();
        when(scheduleStore.getSchedule(SCHEDULE_ID)).thenReturn(Optional.of(testSchedule));
        final StreamObserver<GetScheduleResponse> responseObserver =
            (StreamObserver<GetScheduleResponse>)mock(StreamObserver.class);

        scheduleRpcService.getSchedule(GetScheduleRequest.newBuilder()
            .setOid(SCHEDULE_ID).build(), responseObserver);
        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());
    }

    /**
     * Test get all schedules.
     */
    @Test
    public void testGetAllSchedules() {
        when(scheduleStore.getSchedules(Collections.emptySet())).thenReturn(Arrays.asList(SCHEDULE,
            SCHEDULE));
        final StreamObserver<Schedule> responseObserver =
            (StreamObserver<Schedule>)mock(StreamObserver.class);

        scheduleRpcService.getSchedules(GetSchedulesRequest.getDefaultInstance(),
            responseObserver);

        final ArgumentCaptor<Schedule> responseCaptor =
            ArgumentCaptor.forClass(Schedule.class);
        verify(responseObserver, times(2)).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        verifyResults(SCHEDULE, responseCaptor.getValue());
    }

    /**
     * Test get schedules by IDs.
     */
    @Test
    public void testGetSchedulesByIds() {
        final ImmutableSet<Long> idSet = ImmutableSet.of(11L, 22L);
        when(scheduleStore.getSchedules(idSet)).thenReturn(Arrays.asList(SCHEDULE, SCHEDULE));
        final StreamObserver<Schedule> responseObserver =
            (StreamObserver<Schedule>)mock(StreamObserver.class);

        scheduleRpcService.getSchedules(GetSchedulesRequest.newBuilder()
            .addAllOid(idSet)
            .build(),
            responseObserver);

        final ArgumentCaptor<Schedule> responseCaptor =
            ArgumentCaptor.forClass(Schedule.class);
        verify(responseObserver, times(2)).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        verifyResults(SCHEDULE, responseCaptor.getValue());
    }

    /**
     * Test get all schedules.
     */
    @Test
    public void testGetAllSchedulesWithRefTime() {
        when(scheduleStore.getSchedules(Collections.emptySet())).thenReturn(Arrays.asList(SCHEDULE, SCHEDULE));
        final StreamObserver<Schedule> responseObserver =
            (StreamObserver<Schedule>)mock(StreamObserver.class);

        scheduleRpcService.getSchedules(GetSchedulesRequest.newBuilder()
            .setRefTime(Instant.parse("2010-01-10T09:30:00Z").toEpochMilli())
            .build(),
            responseObserver);

        final ArgumentCaptor<Schedule> responseCaptor =
            ArgumentCaptor.forClass(Schedule.class);
        verify(responseObserver, times(2)).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        verifyResults(SCHEDULE, responseCaptor.getValue());
    }

    /**
     * Test get all schedules with invalid recurrence rule generates error.
     */
    @Test
    public void testGetAllSchedulesParseException() {
        Schedule testSchedule = SCHEDULE.toBuilder()
            .clearOneTime()
            .setRecurRule("FREQ=DAILY;INTERVAL=2;UNTIL=INVALID").build();
        when(scheduleStore.getSchedules(Collections.emptySet())).thenReturn(Collections.singletonList(testSchedule));
        final StreamObserver<Schedule> responseObserver =
            (StreamObserver<Schedule>)mock(StreamObserver.class);

        scheduleRpcService.getSchedules(GetSchedulesRequest.getDefaultInstance(),
            responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());
    }

    /**
     * Test create schedule.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void testCreateSchedule() throws Exception {
        when(scheduleStore.createSchedule(SCHEDULE)).thenReturn(SCHEDULE);
        final StreamObserver<CreateScheduleResponse> responseObserver =
            (StreamObserver<CreateScheduleResponse>)mock(StreamObserver.class);

        scheduleRpcService.createSchedule(CreateScheduleRequest.newBuilder()
            .setSchedule(SCHEDULE).build(), responseObserver);

        final ArgumentCaptor<CreateScheduleResponse> responseCaptor =
            ArgumentCaptor.forClass(CreateScheduleResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        verifyResults(SCHEDULE, responseCaptor.getValue().getSchedule());
    }

    /**
     * Test create schedule with missing body in request generates error.
     */
    @Test
    public void testCreateScheduleMissingRequestBody() {
        final StreamObserver<CreateScheduleResponse> responseObserver =
            (StreamObserver<CreateScheduleResponse>)mock(StreamObserver.class);

        scheduleRpcService.createSchedule(CreateScheduleRequest.newBuilder().build(),
            responseObserver);
        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());
    }

    /**
     * Test create schedule with invalid recurrence rule generates error.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void testCreateScheduleParseException() throws Exception {
        Schedule invalidSchedule = SCHEDULE.toBuilder()
            .clearOneTime()
            .setRecurRule("FREQ=DAILY;INTERVAL=2;UNTIL=INVALID").build();
        when(scheduleStore.createSchedule(SCHEDULE)).thenReturn(invalidSchedule);
        final StreamObserver<CreateScheduleResponse> responseObserver =
            (StreamObserver<CreateScheduleResponse>)mock(StreamObserver.class);

        scheduleRpcService.createSchedule(CreateScheduleRequest.newBuilder()
            .setSchedule(SCHEDULE).build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());
    }

    /**
     * Test create invalid schedule generates error.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void testCreateInvalidSchedule() throws Exception {
        when(scheduleStore.createSchedule(SCHEDULE)).thenThrow(InvalidItemException.class);
        final StreamObserver<CreateScheduleResponse> responseObserver =
            (StreamObserver<CreateScheduleResponse>)mock(StreamObserver.class);

        scheduleRpcService.createSchedule(CreateScheduleRequest.newBuilder()
            .setSchedule(SCHEDULE).build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());
    }

    /**
     * Test create invalid schedule generates error.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void testCreateScheduleWithDuplicateName() throws Exception {
        when(scheduleStore.createSchedule(SCHEDULE)).thenThrow(DuplicateNameException.class);
        final StreamObserver<CreateScheduleResponse> responseObserver =
            (StreamObserver<CreateScheduleResponse>)mock(StreamObserver.class);

        scheduleRpcService.createSchedule(CreateScheduleRequest.newBuilder()
            .setSchedule(SCHEDULE).build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());
    }

    /**
     * Test update schedule.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void testUpdateSchedule() throws Exception {
        when(scheduleStore.updateSchedule(SCHEDULE_ID, SCHEDULE)).thenReturn(SCHEDULE);
        final StreamObserver<UpdateScheduleResponse> responseObserver =
            (StreamObserver<UpdateScheduleResponse>)mock(StreamObserver.class);

        scheduleRpcService.updateSchedule(UpdateScheduleRequest.newBuilder()
            .setOid(SCHEDULE_ID).setUpdatedSchedule(SCHEDULE).build(), responseObserver);

        final ArgumentCaptor<UpdateScheduleResponse> responseCaptor =
            ArgumentCaptor.forClass(UpdateScheduleResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        verifyResults(SCHEDULE, responseCaptor.getValue().getSchedule());
    }

    /**
     * Test update schedule with missing schedule id in request generates error.
     */
    @Test
    public void testUpdateScheduleMissingId() {
        final StreamObserver<UpdateScheduleResponse> responseObserver =
            (StreamObserver<UpdateScheduleResponse>)mock(StreamObserver.class);

        scheduleRpcService.updateSchedule(UpdateScheduleRequest.newBuilder()
                .setUpdatedSchedule(SCHEDULE).build(),
            responseObserver);
        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());
    }

    /**
     * Test update schedule with missing body in request generates error.
     */
    @Test
    public void testUpdateScheduleMissingRequestBody() {
        final StreamObserver<UpdateScheduleResponse> responseObserver =
            (StreamObserver<UpdateScheduleResponse>)mock(StreamObserver.class);

        scheduleRpcService.updateSchedule(UpdateScheduleRequest.newBuilder()
                .setOid(SCHEDULE_ID).build(),
            responseObserver);
        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());
    }

    /**
     * Test update schedule with invalid recurrence rule.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void testUpdateScheduleParseException() throws Exception {
        Schedule invalidSchedule = SCHEDULE.toBuilder()
            .clearOneTime()
            .setRecurRule("FREQ=DAILY;INTERVAL=2;UNTIL=INVALID").build();
        when(scheduleStore.updateSchedule(SCHEDULE_ID, invalidSchedule)).thenReturn(invalidSchedule);
        final StreamObserver<UpdateScheduleResponse> responseObserver =
            (StreamObserver<UpdateScheduleResponse>)mock(StreamObserver.class);

        scheduleRpcService.updateSchedule(UpdateScheduleRequest.newBuilder()
            .setOid(SCHEDULE_ID).setUpdatedSchedule(invalidSchedule).build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());
    }

    /**
     * Test update schedule with invalid recurrence rule generates error.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void testUpdateInvalidSchedule() throws Exception {
        when(scheduleStore.updateSchedule(SCHEDULE_ID, SCHEDULE)).thenThrow(InvalidItemException.class);
        final StreamObserver<UpdateScheduleResponse> responseObserver =
            (StreamObserver<UpdateScheduleResponse>)mock(StreamObserver.class);

        scheduleRpcService.updateSchedule(UpdateScheduleRequest.newBuilder()
            .setOid(SCHEDULE_ID).setUpdatedSchedule(SCHEDULE).build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());
    }

    /**
     * Test update missing schedule generates error.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void testUpdateMissingSchedule() throws Exception {
        when(scheduleStore.updateSchedule(SCHEDULE_ID, SCHEDULE)).thenThrow(ScheduleNotFoundException.class);
        final StreamObserver<UpdateScheduleResponse> responseObserver =
            (StreamObserver<UpdateScheduleResponse>)mock(StreamObserver.class);

        scheduleRpcService.updateSchedule(UpdateScheduleRequest.newBuilder()
            .setOid(SCHEDULE_ID).setUpdatedSchedule(SCHEDULE).build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());
    }

    /**
     * Test update schedule with duplicate name generates error.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void testUpdateDuplicateSchedule() throws Exception {
        when(scheduleStore.updateSchedule(SCHEDULE_ID, SCHEDULE)).thenThrow(DuplicateNameException.class);
        final StreamObserver<UpdateScheduleResponse> responseObserver =
            (StreamObserver<UpdateScheduleResponse>)mock(StreamObserver.class);

        scheduleRpcService.updateSchedule(UpdateScheduleRequest.newBuilder()
            .setOid(SCHEDULE_ID).setUpdatedSchedule(SCHEDULE).build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());
    }

    /**
     * Test delete schedule.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void testDeleteSchedule() throws Exception {
        when(scheduleStore.deleteSchedule(SCHEDULE_ID)).thenReturn(SCHEDULE);
        final StreamObserver<DeleteScheduleResponse> responseObserver =
            (StreamObserver<DeleteScheduleResponse>)mock(StreamObserver.class);

        scheduleRpcService.deleteSchedule(DeleteScheduleRequest.newBuilder()
            .setOid(SCHEDULE_ID).build(), responseObserver);

        final ArgumentCaptor<DeleteScheduleResponse> responseCaptor =
            ArgumentCaptor.forClass(DeleteScheduleResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
    }

    /**
     * Test delete schedule with ID missing in request generates error.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void tesDeleteScheduleMissingId() throws Exception {
        final StreamObserver<DeleteScheduleResponse> responseObserver =
            (StreamObserver<DeleteScheduleResponse>)mock(StreamObserver.class);

        scheduleRpcService.deleteSchedule(DeleteScheduleRequest.newBuilder()
            .build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());
    }

    /**
     * Test delete missing schedule generates error.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void tesDeleteMissingSchedule() throws Exception {
        when(scheduleStore.deleteSchedule(SCHEDULE_ID)).thenThrow(ScheduleNotFoundException.class);
        final StreamObserver<DeleteScheduleResponse> responseObserver =
            (StreamObserver<DeleteScheduleResponse>)mock(StreamObserver.class);

        scheduleRpcService.deleteSchedule(DeleteScheduleRequest.newBuilder()
            .setOid(SCHEDULE_ID).build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());
    }

    /**
     * Test delete schedule in use generates error.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void tesDeleteScheduleInUse() throws Exception {
        when(scheduleStore.deleteSchedule(SCHEDULE_ID)).thenThrow(ScheduleInUseDeleteException.class);
        final StreamObserver<DeleteScheduleResponse> responseObserver =
            (StreamObserver<DeleteScheduleResponse>)mock(StreamObserver.class);

        scheduleRpcService.deleteSchedule(DeleteScheduleRequest.newBuilder()
            .setOid(SCHEDULE_ID).build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());
    }

    /**
     * Test delete schedules.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void testDeleteSchedules() throws Exception {
        when(scheduleStore.deleteSchedules(Sets.newHashSet(SCHEDULE_ID, SCHEDULE_ID_2)))
            .thenReturn(2);

        final StreamObserver<DeleteSchedulesResponse> responseObserver =
            (StreamObserver<DeleteSchedulesResponse>)mock(StreamObserver.class);

        scheduleRpcService.deleteSchedules(DeleteSchedulesRequest.newBuilder().addAllOid(
            Arrays.asList(SCHEDULE_ID, SCHEDULE_ID_2)).build(), responseObserver);

        final ArgumentCaptor<DeleteSchedulesResponse> responseCaptor =
            ArgumentCaptor.forClass(DeleteSchedulesResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        assertEquals(2, responseCaptor.getValue().getNumDeleted());
    }

    /**
     * Test delete missing schedules generates error.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void tesDeleteMissingSchedules() throws Exception {
        when(scheduleStore.deleteSchedules(
            Sets.newHashSet(Arrays.asList(SCHEDULE_ID)))).thenThrow(ScheduleNotFoundException.class);
        final StreamObserver<DeleteSchedulesResponse> responseObserver =
            (StreamObserver<DeleteSchedulesResponse>)mock(StreamObserver.class);

        scheduleRpcService.deleteSchedules(DeleteSchedulesRequest.newBuilder()
            .addAllOid(Arrays.asList(SCHEDULE_ID)).build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());
    }

    /**
     * Test delete schedules in use generates error.
     *
     * @throws Exception If any unexpected exception
     */
    @Test
    public void tesDeleteSchedulesInUse() throws Exception {
        when(scheduleStore.deleteSchedules(
            Sets.newHashSet(Arrays.asList(SCHEDULE_ID)))).thenThrow(ScheduleInUseDeleteException.class);
        final StreamObserver<DeleteSchedulesResponse> responseObserver =
            (StreamObserver<DeleteSchedulesResponse>)mock(StreamObserver.class);

        scheduleRpcService.deleteSchedules(DeleteSchedulesRequest.newBuilder()
            .addAllOid(Arrays.asList(SCHEDULE_ID)).build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());
    }
}
