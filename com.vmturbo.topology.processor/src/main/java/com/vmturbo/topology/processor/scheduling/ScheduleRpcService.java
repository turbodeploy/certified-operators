package com.vmturbo.topology.processor.scheduling;


import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.topology.ScheduleServiceGrpc;
import com.vmturbo.common.protobuf.topology.Scheduler;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;

/**
 * Implementation of the SchedulerService defined in topology/Scheduler.proto.
 */
public class ScheduleRpcService extends ScheduleServiceGrpc.ScheduleServiceImplBase {
    private final com.vmturbo.topology.processor.scheduling.Scheduler scheduler;

    ScheduleRpcService(@Nonnull final com.vmturbo.topology.processor.scheduling.Scheduler scheduler) {
        this.scheduler = Objects.requireNonNull(scheduler);
    }

    /**
     * Set discovery schedule for a target. Both target id and schedule are specified in request.
     *
     * @param request The {@link Scheduler.SetDiscoveryScheduleRequest} target id whose discovery schedule
     *                will be set.
     * @param responseObserver The {@link StreamObserver} is defined in GRPC framework.
     */
    public void setDiscoverySchedule(Scheduler.SetDiscoveryScheduleRequest request,
                                     StreamObserver<Scheduler.DiscoveryScheduleResponse> responseObserver) {
        try {
            TargetDiscoverySchedule schedule = request.hasIntervalMinutes() ?
                    scheduler.setDiscoverySchedule(request.getTargetId(), request.getIntervalMinutes(), TimeUnit.MINUTES) :
                    scheduler.setBroadcastSynchedDiscoverySchedule(request.getTargetId());

            final Scheduler.DiscoveryScheduleResponse response = Scheduler.DiscoveryScheduleResponse.newBuilder()
                    .setInfo(Scheduler.ScheduleInfo.newBuilder()
                            .setIntervalMinutes(schedule.getScheduleInterval(TimeUnit.MINUTES))
                            .setTimeToNextMillis(schedule.getDelay(TimeUnit.MILLISECONDS)))
                    .setSynchedToBroadcastSchedule(schedule.isSynchedToBroadcast())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (TargetNotFoundException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(
                    Long.toString(request.getTargetId())).asException());
        }
    }

    /**
     * Get discovery schedule for a given target whose id is specified in request.
     *
     * @param request The {@link Scheduler.GetDiscoveryScheduleRequest} target id whose discovery schedule
     *                will be retrieved.
     * @param responseObserver The {@link StreamObserver} is defined in GRPC framework.
     */
    public void getDiscoverySchedule(Scheduler.GetDiscoveryScheduleRequest request,
                                     StreamObserver<Scheduler.DiscoveryScheduleResponse> responseObserver) {

        Optional<TargetDiscoverySchedule> schedule = scheduler.getDiscoverySchedule(request.getTargetId());

        if (schedule.isPresent()) {
            final Scheduler.DiscoveryScheduleResponse response = Scheduler.DiscoveryScheduleResponse.newBuilder()
                    .setInfo(Scheduler.ScheduleInfo.newBuilder()
                            .setIntervalMinutes(schedule.get().getScheduleInterval(TimeUnit.MINUTES))
                            .setTimeToNextMillis(schedule.get().getDelay(TimeUnit.MILLISECONDS)))
                    .setSynchedToBroadcastSchedule(schedule.get().isSynchedToBroadcast())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(Status.NOT_FOUND.withDescription(
                    Long.toString(request.getTargetId())).asException());
        }
    }

    /**
     * Set broadcast schedule. Broadcast schedule to be set is specified in request.
     *
     * @param request The {@link Scheduler.SetBroadcastScheduleRequest} Broadcast schedule to be set
     * @param responseObserver The {@link StreamObserver} is defined in GRPC framework.
     */
    public void setBroadcastSchedule(Scheduler.SetBroadcastScheduleRequest request,
                                     StreamObserver<Scheduler.BroadcastScheduleResponse> responseObserver) {
        TopologyBroadcastSchedule schedule = scheduler.setBroadcastSchedule(
                request.getIntervalMinutes(), TimeUnit.MINUTES);

        final Scheduler.BroadcastScheduleResponse response = Scheduler.BroadcastScheduleResponse.newBuilder()
                .setInfo(Scheduler.ScheduleInfo.newBuilder()
                        .setIntervalMinutes(schedule.getScheduleInterval(TimeUnit.MINUTES))
                        .setTimeToNextMillis(schedule.getDelay(TimeUnit.MILLISECONDS)))
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Get broadcast schedule.
     *
     * @param request The {@link Scheduler.GetBroadcastScheduleRequest} which is empty in this case
     * @param responseObserver The {@link StreamObserver} is defined in GRPC framework.
     */
    public void getBroadcastSchedule(Scheduler.GetBroadcastScheduleRequest request,
                                     StreamObserver<Scheduler.BroadcastScheduleResponse> responseObserver) {

        Optional<TopologyBroadcastSchedule> schedule = scheduler.getBroadcastSchedule();

        final Scheduler.BroadcastScheduleResponse.Builder responseBuilder = Scheduler.BroadcastScheduleResponse.newBuilder();
        if (schedule.isPresent()) {
            responseBuilder.setInfo(Scheduler.ScheduleInfo.newBuilder()
                    .setIntervalMinutes(schedule.get().getScheduleInterval(TimeUnit.MINUTES))
                    .setTimeToNextMillis(schedule.get().getDelay(TimeUnit.MILLISECONDS)));
        }
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}

