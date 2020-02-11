package com.vmturbo.repository.service;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javaslang.control.Either;
import reactor.core.publisher.Mono;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.UIEnvironmentType;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.executor.ReactiveGraphDBExecutor;
import com.vmturbo.repository.topology.GlobalSupplyChainFilter;
import com.vmturbo.repository.topology.GlobalSupplyChainManager;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.GlobalSupplyChain;

/**
 * The service for handling supply chain request.
 */
public class SupplyChainService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SupplyChainService.class);
    public static final String GLOBAL_SCOPE = "Market";
    private static final int OID_CHUNK_SIZE = 1000;

    private final ReactiveGraphDBExecutor executor;
    private final GraphDefinition graphDefinition;
    private final GraphDBService graphDBService;
    private final TopologyLifecycleManager lifecycleManager;
    private final GlobalSupplyChainManager globalSupplyChainManager;
    private final UserSessionContext userSessionContext;

    public SupplyChainService(@Nonnull final ReactiveGraphDBExecutor executorArg,
                              @Nonnull final GraphDBService graphDBServiceArg,
                              @Nonnull final GraphDefinition graphDefinitionArg,
                              @Nonnull final TopologyLifecycleManager lifecycleManager,
                              @Nonnull final GlobalSupplyChainManager globalSupplyChainManager,
                              final UserSessionContext userSessionContext) {
        this.executor = Objects.requireNonNull(executorArg);
        this.graphDBService = Objects.requireNonNull(graphDBServiceArg);
        this.graphDefinition = Objects.requireNonNull(graphDefinitionArg);
        this.lifecycleManager = Objects.requireNonNull(lifecycleManager);
        this.globalSupplyChainManager = Objects.requireNonNull(globalSupplyChainManager);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
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
                                               final Optional<UIEnvironmentType> environmentType,
                                               final Set<Integer> ignoredEntityTypes) {

        Optional<TopologyID> targetTopology = contextID
                .map(context -> lifecycleManager.getTopologyId(context, TopologyType.SOURCE))
                .orElse(lifecycleManager.getRealtimeTopologyId());

        if (!targetTopology.isPresent()) {
            contextID.ifPresent(id -> LOGGER.error("Requested topology context {} not found." +
                    " Returning empty supply chain.", id));
            return Mono.just(new java.util.HashMap<>());
        }

        Optional<GlobalSupplyChain> globalSupplyChain =
                globalSupplyChainManager.getGlobalSupplyChain(targetTopology.get());

        if (!globalSupplyChain.isPresent()) {
            LOGGER.warn("No global supply chain present for topology: {}", targetTopology.get());
            return Mono.fromCallable(() -> Collections.emptyMap());
        }

        final GlobalSupplyChainFilter globalSupplyChainFilter =
                new GlobalSupplyChainFilter(ignoredEntityTypes,
                        userSessionContext.getUserAccessScope().hasRestrictions() ?
                                userSessionContext.getUserAccessScope().accessibleOids().toSet()
                                : Collections.emptySet(),
                        environmentType.isPresent() ?  environmentType.get().toEnvType()
                                : Optional.empty());

        return Mono.fromCallable(() -> globalSupplyChain.get().toSupplyChainNodes(globalSupplyChainFilter));
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
            // do not specify ignored entity types, since this is used by Search API in UI,
            // where we want all entities to be searchable. For example: we don't want to show
            // BusinessAccount in supply chain, but we want it to be searchable in UI.
            return getGlobalSupplyChain(Optional.of(contextID), Optional.empty(), Collections.emptySet())
                .map(nodeMap -> nodeMap.entrySet().stream()
                    .collect(Collectors.toMap(
                        Entry::getKey, entry -> RepositoryDTOUtil.getAllMemberOids(entry.getValue())
                    )));
        } else {
            return Mono.fromCallable(() -> {
                // do not specify inclusionEntityTypes or exclusionEntityTypes, since this is used
                // by Search API in UI, where we want all entities to be searchable.
                // For example: we don't want to show BusinessAccount in supply chain, but we want
                // it to be searchable in UI.
                final Either<Throwable, Stream<SupplyChainNode>> supplyChain = graphDBService.getSupplyChain(
                        Optional.of(contextID), envType, startId,
                        Optional.of(userSessionContext.getUserAccessScope()),
                        Collections.emptySet(), Collections.emptySet());

                final Either<Throwable, Map<String, Set<Long>>> e = supplyChain
                    .map(nodeStream -> nodeStream.collect(Collectors.toMap(
                        SupplyChainNode::getEntityType, RepositoryDTOUtil::getAllMemberOids)));

                return e.getOrElse(Collections.emptyMap());
            });
        }
    }
}
