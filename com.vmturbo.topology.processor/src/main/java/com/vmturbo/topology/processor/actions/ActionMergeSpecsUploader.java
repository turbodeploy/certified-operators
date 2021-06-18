package com.vmturbo.topology.processor.actions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.collect.Iterators;
import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

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

    static final int NUMBER_OF_ATOMIC_ATOMIC_SPECS_PER_CHUNK = 1000;

    void buildAtomicActionSpecsMessage(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph,
                                       @Nonnull final StreamObserver<UploadAtomicActionSpecsRequest> requestObserver) {

        // Create the atomic action specs for the entities belonging to probes which
        // sent the action merge policy DTOs.
        List<AtomicActionSpec> specs = new ArrayList<>();
        final TargetEntityCache targetEntityCache = new TargetEntityCache(topologyGraph);
        probeStore.getProbes().entrySet().stream()
                .filter(entry -> entry.getValue().getActionMergePolicyCount() > 0)
                .map(entry -> entry.getKey())
                .forEach(probeId -> targetStore.getProbeTargets(probeId).stream()
                                    .forEach(target -> specs.addAll(
                                            actionMergeSpecsRepository.createAtomicActionSpecs(probeId, target.getId(),
                                                                                               targetEntityCache, topologyGraph)
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

    /**
     * A small helper class to avoid having to rebuild the association between entities of a given
     * type and the target(s) that discovered them.
     */
    public static class TargetEntityCache {
        private final Map<Integer, Long2ObjectMap<List<TopologyEntity>>> cache = new HashMap<>();
        private final TopologyGraph<TopologyEntity> topologyGraph;

        /**
         * Create a new {@link TargetEntityCache}.
         *
         * @param topologyGraph The topology graph backing the cache.
         */
        public TargetEntityCache(@Nonnull final TopologyGraph<TopologyEntity> topologyGraph) {
            this.topologyGraph = Objects.requireNonNull(topologyGraph);
        }

        /**
         * Get the entities of a given type discovered by a given target. If the target does not
         * exist or did not discover any entities of the given type, returns an empty list.
         * <p/>
         * Since the topologyGraph does not index entities by target type, we cache associations
         * we build to provide answers in order to avoid having to repeatedly rebuild the same
         * information when multiple queries are made. This is done as a performance optimization.
         *
         * @param entityType The entity type of the entities to be retrieved.
         * @param targetId The target discovering the entities. Only entities discovered by
         *                 this target of the given type will be returned in the list.
         * @return the entities of the given type discovered by the given target, or an empty
         *         list if there are no such entities. No particular order is guaranteed for the
         *         entities in the list.
         */
        @Nonnull
        public List<TopologyEntity> entitiesOfTypeForTarget(final int entityType, final long targetId) {
            return cache.computeIfAbsent(entityType, (type) -> {
                final Long2ObjectMap<List<TopologyEntity>> mapForType = new Long2ObjectOpenHashMap<>();
                topologyGraph.entitiesOfType(type).forEach(entity ->
                    entity.getDiscoveringTargetIds().forEach(discoveringId ->
                        mapForType.computeIfAbsent(discoveringId.longValue(), id -> new ArrayList<>())
                            .add(entity)));
                return mapForType;
            }).getOrDefault(targetId, Collections.emptyList());
        }
    }
}
