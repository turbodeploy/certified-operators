package com.vmturbo.topology.event.library.uptime;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.EntityUptime.ForceFullUptimeCalculationRequest;
import com.vmturbo.common.protobuf.cost.EntityUptime.ForceFullUptimeCalculationResponse;
import com.vmturbo.common.protobuf.cost.EntityUptime.GetEntityUptimeByFilterRequest;
import com.vmturbo.common.protobuf.cost.EntityUptime.GetEntityUptimeByFilterResponse;
import com.vmturbo.common.protobuf.cost.EntityUptime.GetEntityUptimeRequest;
import com.vmturbo.common.protobuf.cost.EntityUptime.GetEntityUptimeResponse;
import com.vmturbo.common.protobuf.cost.EntityUptimeServiceGrpc.EntityUptimeServiceImplBase;

/**
 * An RPC service for handling entity uptime requests.
 */
public class EntityUptimeRpcService extends EntityUptimeServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final EntityUptimeStore entityUptimeStore;

    private final EntityUptimeManager entityUptimeManager;

    /**
     * Constructs a new RPC service instance.
     * @param entityUptimeStore The {@link EntityUptimeStore}.
     * @param entityUptimeManager The {@link EntityUptimeManager}.
     */
    public EntityUptimeRpcService(@Nonnull EntityUptimeStore entityUptimeStore,
                                  @Nonnull EntityUptimeManager entityUptimeManager) {
        this.entityUptimeStore = Objects.requireNonNull(entityUptimeStore);
        this.entityUptimeManager = Objects.requireNonNull(entityUptimeManager);
    }

    /**
     * Forces a full uptime calculation for the entire topology.
     * @param request The {@link ForceFullUptimeCalculationRequest}.
     * @param responseObserver Observer for {@link ForceFullUptimeCalculationResponse}.
     */
    @Override
    public void forceFullUptimeCalculation(final ForceFullUptimeCalculationRequest request,
                                           final StreamObserver<ForceFullUptimeCalculationResponse> responseObserver) {
        try {

            logger.info("Forcing full entity uptime calculation");

            entityUptimeManager.updateUptimeForTopology(true);

            responseObserver.onNext(ForceFullUptimeCalculationResponse.getDefaultInstance());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error forcing full entity uptime calculation", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to force full entity uptime calculation.")
                    .asException());
        }
    }

    /**
     * Retrieves the entity uptime for a single entity.
     * @param request The {@link GetEntityUptimeRequest}.
     * @param responseObserver Observer for {@link GetEntityUptimeResponse}.
     */
    @Override
    public void getEntityUptime(final GetEntityUptimeRequest request,
                                final StreamObserver<GetEntityUptimeResponse> responseObserver) {
        try {

            logger.debug("Responding to request for '{}' entity uptime", request.getEntityOid());

            final EntityUptime entityUptime = entityUptimeStore.getEntityUptime(request.getEntityOid());

            final GetEntityUptimeResponse response = GetEntityUptimeResponse.newBuilder()
                    .setEntityUptime(entityUptime.toProtobuf())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error handling entity uptime request", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to get entity uptime.")
                    .asException());
        }
    }

    /**
     * Retrieves entity uptime for a set of entities, based on the provided filter.
     * @param request The {@link GetEntityUptimeByFilterRequest}.
     * @param responseObserver Observer for {@link GetEntityUptimeByFilterResponse}.
     */
    @Override
    public void getEntityUptimeByFilter(final GetEntityUptimeByFilterRequest request,
                                        final StreamObserver<GetEntityUptimeByFilterResponse> responseObserver) {
        try {

            final Map<Long, EntityUptime> entityUptimeMap = entityUptimeStore.getUptimeByFilter(request.getFilter());

            final GetEntityUptimeByFilterResponse.Builder response = GetEntityUptimeByFilterResponse.newBuilder()
                    .setDefaultUptime(entityUptimeStore.getDefaultUptime().toProtobuf());
            entityUptimeMap.forEach((entityOid, entityUptime) ->
                    response.putEntityUptimeByOid(entityOid, entityUptime.toProtobuf()));

            responseObserver.onNext(response.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error handling entity uptime by filter request", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to get entity uptime by filter.")
                    .asException());
        }
    }
}
