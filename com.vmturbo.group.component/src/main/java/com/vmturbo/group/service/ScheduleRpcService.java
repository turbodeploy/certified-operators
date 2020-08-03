package com.vmturbo.group.service;

import java.text.ParseException;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
import com.vmturbo.common.protobuf.schedule.ScheduleProto.UpdateScheduleRequest;
import com.vmturbo.common.protobuf.schedule.ScheduleProto.UpdateScheduleResponse;
import com.vmturbo.common.protobuf.schedule.ScheduleServiceGrpc.ScheduleServiceImplBase;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.InvalidItemException;
import com.vmturbo.group.common.ItemDeleteException.ScheduleInUseDeleteException;
import com.vmturbo.group.common.ItemNotFoundException.ScheduleNotFoundException;
import com.vmturbo.group.schedule.ScheduleStore;
import com.vmturbo.group.schedule.ScheduleUtils;

/**
 * This class provides RPC's for CRUD-type operations related to {@link Schedule} objects.
 */
public class ScheduleRpcService extends ScheduleServiceImplBase {

    private static final String PARSE_ERROR_MESSAGE = "Exception parsing schedule recurrence rule";
    private static final String MISSING_SCHED_ID_ERROR_MSG = "Missing ID in Schedule request";
    private static final String MISSING_SCHEDULE_ERROR_MSG = "Missing Schedule object in Create Schedule request";
    private static final String MISSING_ID_OR_SCHEDULE_MSG = "Missing ID or schedules in Update Schedule request";
    private static final String SCHEDULE_NOT_FOUND_MSG = "Schedule not found";
    private static final Logger logger = LogManager.getLogger();

    private final ScheduleStore scheduleStore;

    /**
     * Construct ScheduleRpcService instance.
     *
     * @param scheduleStore {@link ScheduleStore} instance
     */
    public ScheduleRpcService(@Nonnull final ScheduleStore scheduleStore) {
        this.scheduleStore = Objects.requireNonNull(scheduleStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getSchedules(final GetSchedulesRequest request,
                             final StreamObserver<Schedule> responseObserver) {
        final long periodStartTime =
                request.hasRefTime() ? request.getRefTime() : Instant.now().toEpochMilli();
        final Set<Long> scheduleIds = Sets.newHashSet();
        if (!request.getOidList().isEmpty()) {
            scheduleIds.addAll(request.getOidList());
        }
        scheduleStore.getSchedules(scheduleIds).forEach(schedule -> {
            try {
                responseObserver.onNext(ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
                    schedule.toBuilder(), periodStartTime));
            } catch (ParseException e) {
                logger.error(PARSE_ERROR_MESSAGE, e);
                responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage()).asException());
                return;
            }
        });
        responseObserver.onCompleted();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getSchedule(final GetScheduleRequest request,
                            final StreamObserver<GetScheduleResponse> responseObserver) {
        if (!request.hasOid()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription(MISSING_SCHED_ID_ERROR_MSG)
                .asException());
            return;
        }
        GetScheduleResponse response = null;
        final long periodStartTime =
                request.hasRefTime() ? request.getRefTime() : Instant.now().toEpochMilli();
        try {
            final Optional<Schedule> foundSchedule = scheduleStore.getSchedule(request.getOid());
            if (foundSchedule.isPresent()) {
                final Schedule schedule = foundSchedule.get();
                final Schedule updatedSchedule = ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
                    schedule.toBuilder(), periodStartTime);
                response = GetScheduleResponse.newBuilder()
                    .setSchedule(updatedSchedule)
                .build();
            } else {
                responseObserver.onError(Status.NOT_FOUND
                    .withDescription(SCHEDULE_NOT_FOUND_MSG)
                    .asException());
                return;
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (ParseException e) {
            logger.error(PARSE_ERROR_MESSAGE, e);
            responseObserver.onError(Status.INTERNAL
                .withDescription(e.getMessage()).asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createSchedule(final CreateScheduleRequest request,
                               final StreamObserver<CreateScheduleResponse> responseObserver) {
        if (!request.hasSchedule()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription(MISSING_SCHEDULE_ERROR_MSG)
                .asException());
            return;
        }
        try {
            final Schedule createdSchedule = scheduleStore.createSchedule(request.getSchedule());
            responseObserver.onNext(CreateScheduleResponse.newBuilder()
                .setSchedule(ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
                    createdSchedule.toBuilder(), Instant.now().toEpochMilli()))
                .build());
            responseObserver.onCompleted();
        } catch (InvalidItemException e) {
            logger.error(e);
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription(e.getMessage()).asException());
        } catch (DuplicateNameException e) {
            logger.error(e);
            responseObserver.onError(Status.ALREADY_EXISTS
                .withDescription(e.getMessage()).asException());
        } catch (ParseException e) {
            logger.error(PARSE_ERROR_MESSAGE, e);
            responseObserver.onError(Status.INTERNAL
                .withDescription(e.getMessage()).asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateSchedule(final UpdateScheduleRequest request, final StreamObserver<UpdateScheduleResponse> responseObserver) {
        if (!request.hasOid() || !request.hasUpdatedSchedule()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription(MISSING_ID_OR_SCHEDULE_MSG)
                .asException());
            return;
        }
        try {
            final Schedule updatedSchedule = scheduleStore.updateSchedule(request.getOid(),
                request.getUpdatedSchedule());
            responseObserver.onNext(UpdateScheduleResponse.newBuilder()
                .setSchedule(ScheduleUtils.calculateNextOccurrenceAndRemainingTimeActive(
                    updatedSchedule.toBuilder(), Instant.now().toEpochMilli()))
                .build());
            responseObserver.onCompleted();
        } catch (InvalidItemException e) {
            logger.error(e);
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription(e.getMessage()).asException());
        } catch (ScheduleNotFoundException e) {
            logger.error(e);
            responseObserver.onError(Status.NOT_FOUND
                .withDescription(e.getMessage()).asException());
        } catch (DuplicateNameException e) {
            logger.error(e);
            responseObserver.onError(Status.ALREADY_EXISTS
                .withDescription(e.getMessage()).asException());
        } catch (ParseException e) {
            logger.error(PARSE_ERROR_MESSAGE, e);
            responseObserver.onError(Status.INTERNAL
                .withDescription(e.getMessage()).asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteSchedule(final DeleteScheduleRequest request, final StreamObserver<DeleteScheduleResponse> responseObserver) {
        if (!request.hasOid()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription(MISSING_SCHED_ID_ERROR_MSG)
                .asException());
            return;
        }
        try {
            final Schedule deletedSchedule = scheduleStore.deleteSchedule(request.getOid());
            responseObserver.onNext(DeleteScheduleResponse.newBuilder()
                .setSchedule(deletedSchedule)
                .build());
            responseObserver.onCompleted();
        } catch (ScheduleNotFoundException e) {
            logger.error(e);
            responseObserver.onError(Status.NOT_FOUND
                .withDescription(e.getMessage()).asException());
        } catch (ScheduleInUseDeleteException e) {
            logger.error(e);
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription(e.getMessage()).asException());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteSchedules(final DeleteSchedulesRequest request,
                                final StreamObserver<DeleteSchedulesResponse> responseObserver) {
        try {
            final int numDeleted = scheduleStore.deleteSchedules(Sets.newHashSet(request.getOidList()));
            responseObserver.onNext(DeleteSchedulesResponse.newBuilder()
                .setNumDeleted(numDeleted).build());
            responseObserver.onCompleted();
        } catch (ScheduleNotFoundException e) {
            logger.error(e);
            responseObserver.onError(Status.NOT_FOUND
                .withDescription(e.getMessage()).asException());
        } catch (ScheduleInUseDeleteException e) {
            logger.error(e);
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription(e.getMessage()).asException());
        }
    }

}
