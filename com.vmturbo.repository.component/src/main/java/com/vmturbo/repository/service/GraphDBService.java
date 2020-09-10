package com.vmturbo.repository.service;

import static com.google.common.base.Preconditions.checkNotNull;
import static javaslang.API.$;
import static javaslang.API.Case;
import static javaslang.API.Match;
import static javaslang.Patterns.Failure;
import static javaslang.Patterns.Success;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javaslang.control.Either;
import javaslang.control.Option;
import javaslang.control.Try;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.graph.parameter.GraphCmd;
import com.vmturbo.repository.graph.result.ResultsConverter;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology;
import com.vmturbo.repository.topology.ServiceEntityRepoDTOConverter;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyLifecycleManager;

public class GraphDBService {
    private final Logger logger = LoggerFactory.getLogger(GraphDBService.class);
    private final LiveTopologyStore liveTopologyStore;
    private final GraphDBExecutor executor;
    private final GraphDefinition graphDefinition;
    private final TopologyLifecycleManager topologyManager;

    private static final DataMetricSummary SINGLE_SOURCE_SUPPLY_CHAIN_CONVERSION_DURATION_SUMMARY = DataMetricSummary
        .builder()
        .withName("repo_single_source_supply_chain_conversion_duration_seconds")
        .withHelp("Duration in seconds it takes repository to convert single source supply chain.")
        .build()
        .register();
    private static final DataMetricSummary SEARCH_CONVERSION_DURATION_SUMMARY = DataMetricSummary
        .builder()
        .withName("repo_search_conversion_duration_seconds")
        .withHelp("Duration in seconds it takes repository to convert search results.")
        .build()
        .register();

    /**
     * Construct a new GraphDBService instance.
     *
     * @param liveTopologyStore The injected {@link LiveTopologyStore} to support real-time queries
     * @param executor Implements data retrieval queries in AQL
     * @param graphDefinition Metadata about the service entity graph stored in a graph database
     * @param topologyManager Manages the life cycle of topologies
     */
    public GraphDBService(@Nonnull final LiveTopologyStore liveTopologyStore,
                          final GraphDBExecutor executor,
                          final GraphDefinition graphDefinition,
                          final TopologyLifecycleManager topologyManager) {
        this.liveTopologyStore = checkNotNull(liveTopologyStore);
        this.executor = checkNotNull(executor);
        this.graphDefinition = checkNotNull(graphDefinition);
        this.topologyManager = checkNotNull(topologyManager);
    }

    /**
     * Construct a supply chain starting from the given service entity.
     *
     * @param contextID The name of the database to create the supply chain from.
     *                   If <code>empty</code>, use the real-time database.
     * @param envType optional environment type filter
     * @param startId The identifier of the starting service entity.
     * @param entityAccessScope optional restriction related to user entity access
     * @param inclusionEntityTypes entity types to be included in the result
     * @param exclusionEntityTypes entity types to be excluded from the result
     * @return Either a {@link Throwable} describing an error, or a stream of {@link SupplyChainNode}s.
     *         If the start ID is not found, the {@link Throwable} will be a {@link java.util.NoSuchElementException}.
     */
    public Either<Throwable, java.util.stream.Stream<SupplyChainNode>> getSupplyChain(
            final Optional<Long> contextID,
            final Optional<EnvironmentType> envType,
            final String startId,
            final Optional<EntityAccessScope> entityAccessScope,
            final Set<Integer> inclusionEntityTypes,
            final Set<Integer> exclusionEntityTypes) {

        final Optional<TopologyID> targetTopologyId = contextID
                .map(id -> topologyManager.getTopologyId(id, TopologyType.PROJECTED))
                .orElse(topologyManager.getRealtimeTopologyId());
        if (!targetTopologyId.isPresent()) {
            logger.warn("No topology database available to use. Returning an empty supply chain");
            return Either.right(java.util.stream.Stream.empty());
        }
        TopologyID topologyID = targetTopologyId.get();
        final GraphCmd.GetSupplyChain cmd = new GraphCmd.GetSupplyChain(
            startId,
            envType,
            graphDefinition.getProviderRelationship(topologyID),
            graphDefinition.getSEVertexCollection(topologyID),
            entityAccessScope,
            inclusionEntityTypes,
            exclusionEntityTypes);
        logger.debug("Constructed command, {}", cmd);

        final Try<SupplyChainSubgraph> supplyChainResults = executor.executeSupplyChainCmd(cmd);
        logger.debug("Values returned by the GraphExecutor {}", supplyChainResults);

        final Option<Long> contextIDToUse =
                Option.ofOptional(targetTopologyId.map(TopologyID::getContextId));

        final Option<Either<Throwable, java.util.stream.Stream<SupplyChainNode>>> supplyChainResult =
                contextIDToUse.map(cid ->
                Match(supplyChainResults).of(
                    Case(Success($()), v -> timedValue(() -> Either.right(v.toSupplyChainNodes().stream()),
                        SINGLE_SOURCE_SUPPLY_CHAIN_CONVERSION_DURATION_SUMMARY)),
                    Case(Failure($()), exc -> Either.left(exc))
                ));

        return supplyChainResult.getOrElse(Either.right(java.util.stream.Stream.empty()));
    }

    /**
     * Given an OID, retrieve its {@link RepoGraphEntity} form.
     *
     * @param id The OID of an entity to retrieve
     * @return The {@link RepoGraphEntity} representation
     */
    public Optional<RepoGraphEntity> searchServiceEntityById(Long id) {
        // Return empty result if current topology doesn't exist.
        Optional<SourceRealtimeTopology> topologyGraphOpt = liveTopologyStore.getSourceTopology();
        return topologyGraphOpt.isPresent()
                ? topologyGraphOpt.get()
                    .entityGraph()
                    .getEntity(id)
                : Optional.empty();
    }

    public Either<String, Collection<ServiceEntityApiDTO>> searchServiceEntity(
           final Optional<Long> contextId,
           final String field,
           final String query,
           final GraphCmd.SearchType searchType) {

        final Optional<TopologyID> targetTopologyId = contextId
                .map(id -> topologyManager.getTopologyId(id, TopologyType.SOURCE))
                .orElse(topologyManager.getRealtimeTopologyId());
        if (!targetTopologyId.isPresent()) {
            logger.warn("No topology database available to use. Returning an empty result");
            return Either.right(Collections.emptyList());
        }
        TopologyID topologyID = targetTopologyId.get();
        final GraphCmd.SearchServiceEntity cmd = new GraphCmd.SearchServiceEntity(
            graphDefinition.getSEVertexCollection(topologyID),
            field, query, searchType);
        logger.debug("Constructed search command {}", cmd);

        final Try<Collection<ServiceEntityRepoDTO>> seResults = executor.executeSearchServiceEntityCmd(cmd);
        logger.debug("Search results {}", seResults);

        return Match(seResults).of(
                Case(Success($()), v -> timedValue(
                        () -> Either.right(v.stream()
                            .map(ResultsConverter::toServiceEntityApiDTO)
                            .collect(Collectors.toList())),
                    SEARCH_CONVERSION_DURATION_SUMMARY)),
                Case(Failure($()), exc -> Either.left(exc.getMessage()))
        );

    }

    /**
     * Retrieve TopologyEntityDTO from repository graph. Note that, right now, returned TopologyEntityDTO
     * only contains partial fields, because {@link ServiceEntityRepoDTO} only keep partial fields,
     * and TopologyEntityDTO are converted back from stored {@link ServiceEntityRepoDTO}.
     *
     * @param topologyID id of topology.
     * @param entitiesToFind a set of entity ids.
     * @return Either of String or Collection of {@link TopologyEntityDTO}.
     */
    public Either<String, Collection<TopologyEntityDTO>> retrieveTopologyEntities(
            @Nonnull final TopologyID topologyID,
            @Nonnull final Set<Long> entitiesToFind) {
        final GraphCmd.ServiceEntityMultiGet cmd = new GraphCmd.ServiceEntityMultiGet(
            graphDefinition.getSEVertexCollection(topologyID),
            entitiesToFind
        );
        final Try<Collection<ServiceEntityRepoDTO>> seResults = executor.executeServiceEntityMultiGetCmd(cmd);
        logger.debug("Multi-entity search results {}", seResults);

        return Match(seResults).of(
                Case(Success($()), repoDtos -> timedValue(
                        () -> Either.right(ServiceEntityRepoDTOConverter
                            .convertToTopologyEntityDTOs(repoDtos)),
                        SEARCH_CONVERSION_DURATION_SUMMARY)),
                Case(Failure($()), exc -> Either.left(exc.getMessage()))
        );
    }

    private <T> T timedValue(final Supplier<T> supplier, final DataMetricSummary summary) {
        DataMetricTimer timer = summary.startTimer();
        final T result = supplier.get();
        timer.observe();

        return result;
    }
}
