package com.vmturbo.topology.processor.scheduling;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.topology.ScheduleServiceGrpc;
import com.vmturbo.common.protobuf.topology.Scheduler;
import com.vmturbo.common.protobuf.topology.Scheduler.DiscoveryScheduleResponse;
import com.vmturbo.common.protobuf.topology.Scheduler.ScheduleInfo;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
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
    @Override
    public void setDiscoverySchedule(Scheduler.SetDiscoveryScheduleRequest request,
                                     StreamObserver<Scheduler.DiscoveryScheduleResponse> responseObserver) {
        try {
            final DiscoveryScheduleResponse.Builder builder = DiscoveryScheduleResponse.newBuilder();
            if (!request.hasFullIntervalMinutes() && !request.hasIncrementalIntervalSeconds()) {
                // set broadcast synched schedule if no interval provided
                final Map<DiscoveryType, TargetDiscoverySchedule> scheduleMap =
                    scheduler.setDiscoverySchedules(request.getTargetId(), true);

                TargetDiscoverySchedule fullSchedule = scheduleMap.get(DiscoveryType.FULL);
                if (fullSchedule != null) {
                    builder.setFullSchedule(ScheduleInfo.newBuilder()
                        .setIntervalMinutes(fullSchedule.getScheduleInterval(TimeUnit.MINUTES))
                        .setTimeToNextMillis(fullSchedule.getDelay(TimeUnit.MILLISECONDS)));
                    builder.setSynchedToBroadcastSchedule(fullSchedule.isSynchedToBroadcast());
                }

                TargetDiscoverySchedule incrementalSchedule = scheduleMap.get(DiscoveryType.INCREMENTAL);
                if (incrementalSchedule != null) {
                    builder.setIncrementalSchedule(ScheduleInfo.newBuilder()
                        .setIntervalSeconds(incrementalSchedule.getScheduleInterval(TimeUnit.SECONDS))
                        .setTimeToNextMillis(incrementalSchedule.getDelay(TimeUnit.MILLISECONDS)));
                }
            } else {
                if (request.hasFullIntervalMinutes()) {
                    if (request.getFullIntervalMinutes() == -1) {
                        // disable full discovery
                        scheduler.disableDiscoverySchedule(request.getTargetId(), DiscoveryType.FULL);
                        builder.setFullSchedule(ScheduleInfo.newBuilder().setIntervalMinutes(-1).setTimeToNextMillis(-1));
                    } else {
                        TargetDiscoverySchedule schedule =
                            scheduler.setDiscoverySchedule(request.getTargetId(), DiscoveryType.FULL,
                                request.getFullIntervalMinutes(), TimeUnit.MINUTES, false);
                        builder.setFullSchedule(ScheduleInfo.newBuilder()
                            .setIntervalMinutes(schedule.getScheduleInterval(TimeUnit.MINUTES))
                            .setTimeToNextMillis(schedule.getDelay(TimeUnit.MILLISECONDS)));
                        builder.setSynchedToBroadcastSchedule(schedule.isSynchedToBroadcast());
                    }
                }

                if (request.hasIncrementalIntervalSeconds()) {
                    if (request.getIncrementalIntervalSeconds() == -1) {
                        // disable incremental discovery
                        scheduler.disableDiscoverySchedule(request.getTargetId(), DiscoveryType.INCREMENTAL);
                        builder.setIncrementalSchedule(ScheduleInfo.newBuilder()
                            .setIntervalSeconds(-1)
                            .setTimeToNextMillis(-1));
                    } else {
                        TargetDiscoverySchedule schedule =
                            scheduler.setDiscoverySchedule(request.getTargetId(), DiscoveryType.INCREMENTAL,
                                request.getIncrementalIntervalSeconds(), TimeUnit.SECONDS, false);
                        builder.setIncrementalSchedule(ScheduleInfo.newBuilder()
                            .setIntervalSeconds(schedule.getScheduleInterval(TimeUnit.SECONDS))
                            .setTimeToNextMillis(schedule.getDelay(TimeUnit.MILLISECONDS)));
                    }
                }
            }
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        } catch (TargetNotFoundException e) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(
                    Long.toString(request.getTargetId())).asException());
        } catch (UnsupportedDiscoveryTypeException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(
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
    @Override
    public void getDiscoverySchedule(Scheduler.GetDiscoveryScheduleRequest request,
                                     StreamObserver<Scheduler.DiscoveryScheduleResponse> responseObserver) {

        final Map<DiscoveryType, TargetDiscoverySchedule> schedules =
            scheduler.getDiscoverySchedule(request.getTargetId());

        if (schedules.isEmpty()) {
            responseObserver.onError(Status.NOT_FOUND.withDescription(
                Long.toString(request.getTargetId())).asException());
        } else {
            final DiscoveryScheduleResponse.Builder builder = DiscoveryScheduleResponse.newBuilder();
            schedules.forEach(((discoveryType, schedule) -> {
                ScheduleInfo.Builder scheduleInfo = ScheduleInfo.newBuilder()
                    .setTimeToNextMillis(schedule.getDelay(TimeUnit.MILLISECONDS));

                if (discoveryType == DiscoveryType.FULL) {
                    scheduleInfo.setIntervalMinutes(schedule.getScheduleInterval(TimeUnit.MINUTES));
                    builder.setFullSchedule(scheduleInfo)
                        .setSynchedToBroadcastSchedule(schedule.isSynchedToBroadcast());
                } else if (discoveryType == DiscoveryType.INCREMENTAL) {
                    scheduleInfo.setIntervalSeconds(schedule.getScheduleInterval(TimeUnit.SECONDS));
                    builder.setIncrementalSchedule(scheduleInfo);
                }
            }));

            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }
    }

    /**
     * Set broadcast schedule. Broadcast schedule to be set is specified in request.
     *
     * @param request The {@link Scheduler.SetBroadcastScheduleRequest} Broadcast schedule to be set
     * @param responseObserver The {@link StreamObserver} is defined in GRPC framework.
     */
    @Override
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
    @Override
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

