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
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UIEnvironmentType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.graph.parameter.GraphCmd;
import com.vmturbo.repository.graph.result.ResultsConverter;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph;
import com.vmturbo.repository.topology.ServiceEntityRepoDTOConverter;
import com.vmturbo.repository.topology.TopologyDatabase;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyLifecycleManager;

public class GraphDBService {
    private final Logger logger = LoggerFactory.getLogger(GraphDBService.class);
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

    public GraphDBService(final GraphDBExecutor executor,
                          final GraphDefinition graphDefinition,
                          final TopologyLifecycleManager topologyManager) {
        this.executor = checkNotNull(executor);
        this.graphDefinition = checkNotNull(graphDefinition);
        this.topologyManager = checkNotNull(topologyManager);
    }

    /**
     * Construct a supply chain starting from the given service entity.
     *
     * @param contextID The name of the database to create the supply chain from.
     *                   If <code>empty</code>, use the real-time database.
     * @param startId The identifier of the starting service entity.
     * @return Either a {@link Throwable} describing an error, or a stream of {@link SupplyChainNode}s.
     *         If the start ID is not found, the {@link Throwable} will be a {@link java.util.NoSuchElementException}.
     */
    public Either<Throwable, java.util.stream.Stream<SupplyChainNode>> getSupplyChain(
            final Optional<Long> contextID,
            final Optional<UIEnvironmentType> envType,
            final String startId,
            final Optional<EntityAccessScope> entityAccessScope,
            final Set<Integer> inclusionEntityTypes,
            final Set<Integer> exclusionEntityTypes) {

        final Optional<TopologyID> targetTopologyId = contextID
                .map(id -> topologyManager.getTopologyId(id, TopologyType.SOURCE))
                .orElse(topologyManager.getRealtimeTopologyId());
        if (!targetTopologyId.isPresent()) {
            logger.warn("No topology database available to use. Returning an empty supply chain");
            return Either.right(java.util.stream.Stream.empty());
        }
        TopologyID topologyID = targetTopologyId.get();
        final GraphCmd.GetSupplyChain cmd = new GraphCmd.GetSupplyChain(
            startId,
            envType,
            graphDefinition.getProviderRelationship(),
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


    public Either<String, Collection<ServiceEntityApiDTO>> searchServiceEntityByName(final String query) {
        // The '-' and '+' are two special characters that are not indexed by ArangoDB
        // They are replaced by ',' so ArangoDB will search the display names that contain
        // substrings in the query separated by ','.
        // TODO: The searching service is not fully implemented. This will be done in OM-11450.
        final String queryString = query.replaceAll("[\\+\\-]", ",");
        return searchServiceEntity(Optional.empty(), "displayName", queryString, GraphCmd.SearchType.FULLTEXT);
    }

    public Either<String, Collection<ServiceEntityApiDTO>> searchServiceEntityById(final String id) {
        return searchServiceEntity(Optional.empty(), "uuid", id, GraphCmd.SearchType.STRING);
    }

    /**
     * Fetch service entities from the specified topology.
     *
     * @param contextId The ID of topology to fetch service entities from.
     *                  If <code>empty</code>, use the real-time topology.
     * @param entitiesToFind The IDs of the service entities we want to fetch.
     * @return The list of  {@link ServiceEntityApiDTO} or an error message.
     */
    @Nonnull
    public Either<String, Collection<ServiceEntityApiDTO>> findMultipleEntities(
            @Nonnull final Optional<Long> contextId,
            @Nonnull final Set<Long> entitiesToFind,
            @Nonnull final TopologyType targetType) {

        final Optional<TopologyID> targetTopologyId = contextId
                .map(id -> topologyManager.getTopologyId(id, targetType))
                .orElse(topologyManager.getRealtimeTopologyId());
        if (!targetTopologyId.isPresent()) {
            logger.warn("No topology database available to use. Returning an empty result");
            return Either.right(Collections.emptyList());
        }
        TopologyID topologyID = targetTopologyId.get();
        final GraphCmd.ServiceEntityMultiGet cmd = new GraphCmd.ServiceEntityMultiGet(
            graphDefinition.getSEVertexCollection(topologyID),
            entitiesToFind);
        final Try<Collection<ServiceEntityRepoDTO>> seResults = executor.executeServiceEntityMultiGetCmd(cmd);
        logger.debug("Multi-entity search results {}", seResults);

        return Match(seResults).of(
                Case(Success($()), repoDtos -> timedValue(
                        () -> Either.right(repoDtos.stream()
                            .map(ResultsConverter::toServiceEntityApiDTO)
                            .collect(Collectors.toList())),
                    SEARCH_CONVERSION_DURATION_SUMMARY)),
                Case(Failure($()), exc -> Either.left(exc.getMessage()))
        );
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

    /**
     * Retrieve TopologyEntityDTO from repository graph. Note that, right now, returned TopologyEntityDTO
     * only contains partial fields, because {@link ServiceEntityRepoDTO} only keep partial fields,
     * and TopologyEntityDTO are converted back from stored {@link ServiceEntityRepoDTO}.
     *
     * @param entitiesToFind a set of entity ids.
     * @return Either of String or Collection of {@link TopologyEntityDTO}.
     */
    public Either<String, Collection<TopologyEntityDTO>> retrieveRealTimeTopologyEntities(
            @Nonnull final Set<Long> entitiesToFind) {
        final Optional<TopologyID> topologyID = topologyManager.getRealtimeTopologyId();
        if (topologyID.isPresent()) {
            final GraphCmd.ServiceEntityMultiGet cmd = new GraphCmd.ServiceEntityMultiGet(
                graphDefinition.getSEVertexCollection(topologyID.get()),
                entitiesToFind);
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
        return Either.left("Failed to find real time topology Id");
    }


    private <T> T timedValue(final Supplier<T> supplier, final DataMetricSummary summary) {
        DataMetricTimer timer = summary.startTimer();
        final T result = supplier.get();
        timer.observe();

        return result;
    }
}
