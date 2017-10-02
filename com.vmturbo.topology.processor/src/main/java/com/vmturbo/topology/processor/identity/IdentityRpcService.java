package com.vmturbo.topology.processor.identity;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.topology.Identity.EntityIdentityMetadata;
import com.vmturbo.common.protobuf.topology.Identity.GetAllProbeIdentityMetadataRequest;
import com.vmturbo.common.protobuf.topology.Identity.GetProbeIdentityMetadataRequest;
import com.vmturbo.common.protobuf.topology.Identity.ProbeIdentityMetadata;
import com.vmturbo.common.protobuf.topology.IdentityServiceGrpc;
import com.vmturbo.platform.sdk.common.IdentityMetadata.EntityIdentityMetadata.PropertyMetadata;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.probes.ProbeStore;

/**
 * Implements the RPC calls for retrieving Probe Identity Metadata
 */
public class IdentityRpcService extends IdentityServiceGrpc.IdentityServiceImplBase {

    private final ProbeStore probeStore;

    /**
     * Create a Identity RPC service
     * @param probeStore Store for probe information
     */
    IdentityRpcService(@Nonnull final ProbeStore probeStore) {
        this.probeStore = Objects.requireNonNull(probeStore);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getProbeIdentityMetadata(GetProbeIdentityMetadataRequest request,
                                         StreamObserver<ProbeIdentityMetadata> responseObserver) {
        ProbeIdentityMetadata probeIdentityMetadata = probeStore.getProbe(request.getProbeId())
                .map(probeInfo -> ProbeIdentityMetadata.newBuilder()
                        .setProbeId(request.getProbeId())
                        .addAllEntityIdentityMetadata(buildEntityIdentityMetadata(probeInfo))
                        .build())
                .orElse(ProbeIdentityMetadata.newBuilder()
                        .setProbeId(request.getProbeId())
                        .setError("Probe not registered.")
                        .build());
        responseObserver.onNext(probeIdentityMetadata);
        responseObserver.onCompleted();

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getAllProbeIdentityMetadata(GetAllProbeIdentityMetadataRequest request,
                                            StreamObserver<ProbeIdentityMetadata> responseObserver) {
        probeStore.getRegisteredProbes().entrySet().stream()
                .forEach(entry -> responseObserver.onNext(
                        ProbeIdentityMetadata.newBuilder()
                                .setProbeId(entry.getKey())
                                .addAllEntityIdentityMetadata(buildEntityIdentityMetadata(entry.getValue()))
                                .build()
                        )
                );
        responseObserver.onCompleted();
    }

    /**
     * Copy the list Entity Identity Metadata from probeInfo
     * @param probeInfo contains probe information.
     */
    private List<EntityIdentityMetadata> buildEntityIdentityMetadata(ProbeInfo probeInfo) {
        final ImmutableList.Builder<EntityIdentityMetadata> metadataBuilder = new ImmutableList.Builder<>();
        probeInfo.getEntityMetadataList().stream()
                .map(entityMetadata -> EntityIdentityMetadata.newBuilder()
                        .setEntityType(entityMetadata.getEntityType().toString())
                        .addAllNonVolatileProperties(processPropertyList(entityMetadata.getNonVolatilePropertiesList()))
                        .addAllVolatileProperties(processPropertyList(entityMetadata.getVolatilePropertiesList()))
                        .addAllHeuristicProperties(processPropertyList(entityMetadata.getHeuristicPropertiesList()))
                        .setHeuristicThreshold(entityMetadata.getHeuristicThreshold())
                        .build())
                .forEach(metadataBuilder::add);
        return metadataBuilder.build();

    }

    /**
     * Get the names from PropertyMetadata list
     * @param input A list of PropertyMetadata
     */
    private static List<String> processPropertyList(@Nonnull final List<PropertyMetadata> input) {
        final ImmutableList.Builder<String> retBuilder = new ImmutableList.Builder<>();
        input.stream()
                .map(PropertyMetadata::getName)
                .forEach(retBuilder::add);
        return retBuilder.build();
    }

}
