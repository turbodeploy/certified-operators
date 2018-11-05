package com.vmturbo.repository.service;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javaslang.collection.HashMap;
import javaslang.collection.Iterator;
import javaslang.collection.List;
import javaslang.control.Either;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.components.common.mapping.UIEnvironmentType;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.executor.ReactiveGraphDBExecutor;
import com.vmturbo.repository.graph.parameter.GraphCmd;
import com.vmturbo.repository.graph.result.GlobalSupplyChainFluxResult;
import com.vmturbo.repository.graph.result.ScopedEntity;
import com.vmturbo.repository.graph.result.SupplyChainResultsConverter;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyRelationshipRecorder;

/**
 * The service for handling supply chain request.
 */
public class SupplyChainService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SupplyChainService.class);
    public static final String GLOBAL_SCOPE = "Market";
    private static final int OID_CHUNK_SIZE = 1000;

    private final ReactiveGraphDBExecutor executor;
    private final GraphDefinition graphDefinition;
    private final TopologyRelationshipRecorder globalSupplyChainRecorder;
    private final GraphDBService graphDBService;
    private final TopologyLifecycleManager lifecycleManager;

    public SupplyChainService(final ReactiveGraphDBExecutor executorArg,
                              final GraphDBService graphDBServiceArg,
                              final GraphDefinition graphDefinitionArg,
                              final TopologyRelationshipRecorder globalSupplyChainRecorderArg,
                              final TopologyLifecycleManager lifecycleManager) {
        executor = Objects.requireNonNull(executorArg);
        graphDBService = Objects.requireNonNull(graphDBServiceArg);
        graphDefinition = Objects.requireNonNull(graphDefinitionArg);
        globalSupplyChainRecorder = Objects.requireNonNull(globalSupplyChainRecorderArg);
        this.lifecycleManager = Objects.requireNonNull(lifecycleManager);
    }

    /**
     * Compute the global supply chain.
     *
     * @param contextID The topology from which the global supply chain is computed.
     *                   If <code>empty</code>, use the real-time database.
     *
     * @return A map of entity type names to {@link SupplyChainNode}s.
     */
    public Mono<Map<String, SupplyChainNode>> getGlobalSupplyChain(final Optional<Long> contextID,
                                               final Optional<UIEnvironmentType> environmentType) {

        Optional<TopologyID> targetTopology = contextID
            .map(context -> lifecycleManager.getTopologyId(context, TopologyType.SOURCE))
            .orElse(lifecycleManager.getRealtimeTopologyId());

        if (!targetTopology.isPresent()) {
            contextID.ifPresent(id -> LOGGER.error("Requested topology context {} not found." +
                    " Returning empty supply chain.", id));
            return Mono.just(new java.util.HashMap<>());
        }

        final GraphCmd.GetGlobalSupplyChain cmd = new GraphCmd.GetGlobalSupplyChain(
                targetTopology.get().database(),
                graphDefinition.getServiceEntityVertex(),
                environmentType);

        final GlobalSupplyChainFluxResult results = executor.executeGlobalSupplyChainCmd(cmd);

        // Using the mapping, construct a supply chain.
        return SupplyChainResultsConverter.toSupplyChainNodes(results,
                globalSupplyChainRecorder.getGlobalSupplyChainProviderStructures())
            .doOnError(err -> LOGGER.error("Error while computing supply chain", err));
    }

    /**
     * When getting the list of the scoped entities, it is required to get the supply chain first.
     * This is to support that functionality.
     *
     * @param contextID The topology to compute the supply chain from.
     * @param startId The starting entity ID.
     * @return A mapping between entity types and OIDs.
     */
    public Mono<Map<String, Set<Long>>> getSupplyChainInternal(final long contextID,
                                                               final String startId,
                                                               Optional<UIEnvironmentType> envType) {
        if (startId.equals(GLOBAL_SCOPE)) {
            return getGlobalSupplyChain(Optional.of(contextID), Optional.empty())
                .map(nodeMap -> nodeMap.entrySet().stream()
                    .collect(Collectors.toMap(
                        Entry::getKey, entry -> RepositoryDTOUtil.getAllMemberOids(entry.getValue())
                    )));
        } else {
            return Mono.fromCallable(() -> {
                final Either<String, Stream<SupplyChainNode>> supplyChain = graphDBService.getSupplyChain(
                    Optional.of(contextID), envType, startId);

                final Either<String, Map<String, Set<Long>>> e = supplyChain
                    .map(nodeStream -> nodeStream.collect(Collectors.toMap(
                        SupplyChainNode::getEntityType, RepositoryDTOUtil::getAllMemberOids)));

                return e.getOrElse(Collections.emptyMap());
            });
        }
    }

    /**
     * Return the entities related to the given scoped entity ID.
     *
     * @param contextID The topology to compute the scoped entities.
     * @param startId The entity id to scope for.
     * @param entityTypes The types that we are interested in.
     *
     * @return The {@link ScopedEntity}.
     */
    public Flux<ScopedEntity> scopedEntities(final long contextID,
                                             final String startId,
                                             final Collection<String> entityTypes) {
        return lifecycleManager.databaseOf(contextID, TopologyType.SOURCE)
            .map(database -> getSupplyChainInternal(contextID, startId, Optional.empty())
                .flatMap(m -> {
                    final Map<String, Set<Long>> filteredMap = HashMap.ofAll(m)
                            .filter(entry -> entityTypes.contains(entry._1)).toJavaMap();
                    final List<Long> oids = List.ofAll(filteredMap.values()).flatMap(List::ofAll);
                    final Iterator<Flux<ScopedEntity>> oidPublishers = oids.grouped(OID_CHUNK_SIZE)
                            .map(oidChunk -> executor.fetchScopedEntities(database,
                                graphDefinition.getServiceEntityVertex(), oidChunk));
                    return Flux.merge(oidPublishers);
                }))
            .orElseGet(() -> {
                LOGGER.error("Unable to find database with topology context {}." +
                        " Returning empty list of scoped entities.", contextID);
                return Flux.empty();
            });
    }
}
