package com.vmturbo.topology.processor.entity;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.GetHostInfoRequest;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.GetHostInfoResponse;
import com.vmturbo.common.protobuf.topology.EntityInfoOuterClass.HostInfo;
import com.vmturbo.common.protobuf.topology.EntityServiceGrpc;
import com.vmturbo.topology.processor.entity.Entity.PerTargetInfo;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Implementation of the EntityService defined in topology/EntityInfo.proto.
 */
public class EntityRpcService extends EntityServiceGrpc.EntityServiceImplBase {
    private final EntityStore entityStore;

    private final TargetStore targetStore;

    public EntityRpcService(@Nonnull final EntityStore entityStore,
                            @Nonnull final TargetStore targetStore) {
        this.entityStore = Objects.requireNonNull(entityStore);
        this.targetStore = Objects.requireNonNull(targetStore);
    }

    /**
     * Get entityInfo list from given entity Ids that is specified in request.
     *
     * @param request The {@link EntityInfoOuterClass.GetEntitiesInfoRequest} all entity ids are stored in.
     * @param responseObserver The {@link StreamObserver} is defined in GRPC framework.
     */
    @Override
    public void getEntitiesInfo(EntityInfoOuterClass.GetEntitiesInfoRequest request,
                                StreamObserver<EntityInfoOuterClass.EntityInfo> responseObserver) {
        for (final Long entityId : request.getEntityIdsList()) {
            Optional<Entity> entity = entityStore.getEntity(entityId);
            if (entity.isPresent()) {
                responseObserver.onNext(success(entity.get(), targetStore));
            }
        }

        responseObserver.onCompleted();
    }

    @Override
    public void getHostsInfo(GetHostInfoRequest request, StreamObserver<GetHostInfoResponse> responseObserver) {
        for (final Long virtualMachineId : request.getVirtualMachineIdsList()) {
            // Note that this is a hack and does not correctly handle the case when a VM was discovered by multiple
            // targets.
            Optional<HostInfo> hostInfo = entityStore.getEntity(virtualMachineId)
                .flatMap(entity -> entity.allTargetInfo().stream()
                    .filter(vm -> vm.getHost() != 0) // A host where the host == 0 indicates the host was not set.
                    .findFirst())
                .map(PerTargetInfo::getHost)
                .flatMap(this::getHostInfo);

            final GetHostInfoResponse.Builder responseBuilder = GetHostInfoResponse.newBuilder()
                .setVirtualMachineId(virtualMachineId);
            hostInfo.ifPresent(responseBuilder::setHostInfo);

            responseObserver.onNext(responseBuilder.build());
        }

        responseObserver.onCompleted();
    }
    
    private EntityInfoOuterClass.EntityInfo success(@Nonnull final Entity entity,
                                                    @Nonnull final TargetStore targetStore) {
        Objects.requireNonNull(entity);
        final EntityInfoOuterClass.EntityInfo.Builder entityInfoBuilder =
                EntityInfoOuterClass.EntityInfo.newBuilder()
                    .setEntityId(entity.getId());
        entity.getTargets().stream()
                .map(targetStore::getTarget)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(target -> entityInfoBuilder.putTargetIdToProbeId(target.getId(), target.getProbeId()));

        return entityInfoBuilder.build();
    }

    /**
     * Fetch HostInfo for a host by its OID.
     * Note that if multiple targets discovered the host, we make no guarantee about which target's
     * info will be selected.
     * TODO: (David Blinn) The topology processor should probably not be providing this information. Solve
     * properly after figuring out how to handle entity properties.
     *
     * @param hostOid The OID of the host whose info should be fetched.
     * @return The HostInfo for the host.
     */
    private Optional<HostInfo> getHostInfo(final long hostOid) {
        return entityStore.getEntity(hostOid)
            .flatMap(host -> host.allTargetInfo().stream().findFirst())
            .map(hostTargetInfo -> hostTargetInfo.getEntityInfo().getPhysicalMachineData())
            .map(pmData -> HostInfo.newBuilder()
                .setHostId(hostOid)
                .setCpuCoreMhz(pmData.getCpuCoreMhz())
                .setNumCpuCores(pmData.getNumCpuCores())
                .setNumCpuSockets(pmData.getNumCpuSockets())
                .setNumCpuThreads(pmData.getNumCpuThreads())
                .build()
            );
    }
}
