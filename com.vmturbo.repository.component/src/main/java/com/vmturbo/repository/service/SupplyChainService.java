package com.vmturbo.repository.service;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Mono;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
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
    private final TopologyLifecycleManager lifecycleManager;
    private final GlobalSupplyChainManager globalSupplyChainManager;
    private final UserSessionContext userSessionContext;

    public SupplyChainService(@Nonnull final TopologyLifecycleManager lifecycleManager,
                              @Nonnull final GlobalSupplyChainManager globalSupplyChainManager,
                              final UserSessionContext userSessionContext) {
        this.lifecycleManager = Objects.requireNonNull(lifecycleManager);
        this.globalSupplyChainManager = Objects.requireNonNull(globalSupplyChainManager);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
    }

    /**
     * Compute the global supply chain.
     *
     * @param contextID The topology from which the global supply chain is computed.
     *                   If <code>empty</code>, use the real-time database.
     * @param environmentType optional environment type filter
     * @param ignoredEntityTypes optional set of entity types that should not be
     *                           returned in the final result
     * @return A map of entity type names to {@link SupplyChainNode}s.
     */
    public Mono<Map<String, SupplyChainNode>> getGlobalSupplyChain(final Optional<Long> contextID,
                                               final Optional<EnvironmentType> environmentType,
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
                        environmentType);

        return Mono.fromCallable(() -> globalSupplyChain.get().toSupplyChainNodes(globalSupplyChainFilter));
    }
}
