package com.vmturbo.topology.processor.actions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.collect.Iterators;
import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.AtomicActionSpec;
import com.vmturbo.common.protobuf.action.ActionMergeSpecDTO.UploadAtomicActionSpecsRequest;
import com.vmturbo.common.protobuf.action.AtomicActionSpecsUploadServiceGrpc.AtomicActionSpecsUploadServiceStub;
import com.vmturbo.platform.common.dto.ActionExecutionREST.ActionMergePolicyDTO;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.TargetStore;

/**
 * Uploads the {@link AtomicActionSpec} created for entities belonging to probes which sent
 * the {@link ActionMergePolicyDTO} objects to the topology processor.
 */
public class ActionMergeSpecsUploader {
    private static final Logger logger = LogManager.getLogger();

    private final ProbeStore probeStore;
    private final TargetStore targetStore;
    private final ActionMergeSpecsRepository actionMergeSpecsRepository;

    // This is an async stub.
    private final AtomicActionSpecsUploadServiceStub atomicActionSpecsUploadServiceClient;

    /**
     * Construct an {@link ActionMergeSpecsUploader} which is used to upload action merge specs.
     *
     * @param actionMergeSpecsRepository    repository with the entity level action merge policy DTOs
     *                                      used to create the merge specs for entities
     * @param probeStore                    {@link ProbeStore}
     * @param targetStore                   {@link TargetStore}
     * @param atomicActionSpecsUploadServiceClient service for uploading action merge specs
     *
     */
    ActionMergeSpecsUploader(
            @Nonnull final ActionMergeSpecsRepository actionMergeSpecsRepository,
            @Nonnull final ProbeStore probeStore,
            @Nonnull final TargetStore targetStore,
            @Nonnull final AtomicActionSpecsUploadServiceStub atomicActionSpecsUploadServiceClient) {
        this.actionMergeSpecsRepository = actionMergeSpecsRepository;
        this.probeStore = Objects.requireNonNull(probeStore);
        this.targetStore = Objects.requireNonNull(targetStore);
        this.atomicActionSpecsUploadServiceClient = Objects.requireNonNull(atomicActionSpecsUploadServiceClient);
    }

    /**
     * This method is used to create and upload atomic action specs to action orchestrator.
     *
     * @param topologyGraph {@link TopologyGraph}
     *
     */
    public void uploadAtomicActionSpecsInfo(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph) {
        final StreamObserver<UploadAtomicActionSpecsRequest> requestObserver =
                atomicActionSpecsUploadServiceClient.uploadAtomicActionSpecs(new StreamObserver<Empty>() {
                    @Override
                    public void onNext(final Empty empty) {}

                    @Override
                    public void onError(final Throwable throwable) {}

                    @Override
                    public void onCompleted() {}
                });

        buildAtomicActionSpecsMessage(topologyGraph, requestObserver);

        requestObserver.onCompleted();
    }

    static final int NUMBER_OF_ATOMIC_ATOMIC_SPECS_PER_CHUNK = 10;

    void buildAtomicActionSpecsMessage(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                       @Nonnull final StreamObserver<UploadAtomicActionSpecsRequest> requestObserver) {

        // Create the atomic action specs for the entities belonging to probes which
        // sent the action merge policy DTOs.
        List<AtomicActionSpec> specs = new ArrayList<>();
        probeStore.getProbes().entrySet().stream()
                .filter(entry -> entry.getValue().getActionMergePolicyCount() > 0)
                .map(entry -> entry.getKey())
                .forEach(probeId -> targetStore.getProbeTargets(probeId).stream()
                                    .forEach(target -> specs.addAll(
                                            actionMergeSpecsRepository.createAtomicActionSpecs(probeId, target.getId(),
                                                                                                topologyGraph)
                                    ))
                );

        // Chunk messages in order not to exceed gRPC message maximum size, which is 4MB by default.
        Iterators.partition(specs.iterator(), NUMBER_OF_ATOMIC_ATOMIC_SPECS_PER_CHUNK)
                .forEachRemaining(chunk -> {
                    requestObserver.onNext(UploadAtomicActionSpecsRequest.newBuilder()
                            .addAllAtomicActionSpecsInfo(chunk)
                            .build());

                    }
                );

        logger.debug("Upload of atomic action specs completed");
    }
}
