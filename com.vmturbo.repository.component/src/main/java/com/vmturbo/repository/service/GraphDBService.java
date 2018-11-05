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
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.mapping.UIEnvironmentType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.graph.parameter.GraphCmd;
import com.vmturbo.repository.graph.result.ResultsConverter;
import com.vmturbo.repository.graph.result.SupplyChainSubgraph;
import com.vmturbo.repository.topology.TopologyConverter;
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
     * @return Either a String describing an error, or a stream of {@link SupplyChainNode}s.
     */
    public Either<String, java.util.stream.Stream<SupplyChainNode>> getSupplyChain(
                final Optional<Long> contextID,
                final Optional<UIEnvironmentType> envType,
                final String startId) {
        final Optional<TopologyID> targetTopologyId = contextID
                .map(id -> topologyManager.getTopologyId(id, TopologyType.SOURCE))
                .orElse(topologyManager.getRealtimeTopologyId());

        final Optional<TopologyDatabase> databaseToUse = targetTopologyId.map(TopologyID::database);

        // Use the topology associated with the `contextID`, or the real-time topology.
        return databaseToUse.map(topologyDB -> {
            final GraphCmd.GetSupplyChain cmd = new GraphCmd.GetSupplyChain(
                startId,
                envType,
                topologyDB,
                graphDefinition.getProviderRelationship(),
                graphDefinition.getServiceEntityVertex());
            logger.debug("Constructed command, {}", cmd);

            final Try<SupplyChainSubgraph> supplyChainResults = executor.executeSupplyChainCmd(cmd);
            logger.debug("Values returned by the GraphExecutor {}", supplyChainResults);

            final Option<Long> contextIDToUse =
                    Option.ofOptional(targetTopologyId.map(TopologyID::getContextId));

            final Option<Either<String, java.util.stream.Stream<SupplyChainNode>>> supplyChainResult =
                contextIDToUse.map(cid ->
                    Match(supplyChainResults).of(
                        Case(Success($()), v -> timedValue(() -> Either.right(v.toSupplyChainNodes().stream()),
                            SINGLE_SOURCE_SUPPLY_CHAIN_CONVERSION_DURATION_SUMMARY)),
                        Case(Failure($()), exc -> Either.left(exc.getMessage()))
                    ));

            return supplyChainResult.getOrElse(Either.right(java.util.stream.Stream.empty()));
        })
        // Real-time topology database is not yet created.
        .orElseGet(() -> {
            logger.warn("No topology database available to use. Returning an empty supply chain");
            return Either.right(java.util.stream.Stream.empty());
        });
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
        final Option<TopologyDatabase> databaseToUse =
                Option.ofOptional(targetTopologyId.map(TopologyID::database));

        // Use the topology associated with the `contextID`, or the real-time topology.
        final Option<Either<String, Collection<ServiceEntityApiDTO>>> results = databaseToUse.map(topologyDB -> {
            final GraphCmd.ServiceEntityMultiGet cmd = new GraphCmd.ServiceEntityMultiGet(
                    graphDefinition.getServiceEntityVertex(),
                    entitiesToFind,
                    topologyDB);
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
        });

        return results.getOrElse(() -> {
            logger.warn("No topology database available to use. Returning an empty result");
            return Either.right(Collections.emptyList());
        });
    }

    public Either<String, Collection<ServiceEntityApiDTO>> searchServiceEntity(
           final Optional<Long> contextId,
           final String field,
           final String query,
           final GraphCmd.SearchType searchType) {

        final Optional<TopologyID> targetTopologyId = contextId
                .map(id -> topologyManager.getTopologyId(id, TopologyType.SOURCE))
                .orElse(topologyManager.getRealtimeTopologyId());
        final Option<TopologyDatabase> databaseToUse =
                Option.ofOptional(targetTopologyId.map(TopologyID::database));

        final Option<Either<String, Collection<ServiceEntityApiDTO>>> results = databaseToUse.map(topologyDB -> {
            final GraphCmd.SearchServiceEntity cmd = new GraphCmd.SearchServiceEntity(
                    graphDefinition.getServiceEntityVertex(),
                    field, query, searchType,
                    topologyDB);
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
        });

        return results.getOrElse(() -> {
            logger.warn("No topology database available to use. Returning an empty result");
            return Either.right(Collections.emptyList());
        });
    }

    /**
     * Retrieve TopologyEntityDTO from repository graph. Note that, right now, returned TopologyEntityDTO
     * only contains partial fields, because {@link ServiceEntityRepoDTO} only keep partial fields,
     * and TopologyEntityDTO are converted back from stored {@link ServiceEntityRepoDTO}.
     *
     * @param contextId context id of topology.
     * @param topologyId id of topology.
     * @param entitiesToFind a set of entity ids.
     * @param topologyType type of topology need to search.
     * @return Either of String or Collection of {@link TopologyEntityDTO}.
     */
    public Either<String, Collection<TopologyEntityDTO>> retrieveTopologyEntities(
            final long contextId,
            final long topologyId,
            @Nonnull final Set<Long> entitiesToFind,
            @Nonnull TopologyType topologyType) {
        final TopologyID topologyID = new TopologyID(contextId, topologyId, topologyType);
        final TopologyDatabase databaseToUse = topologyID.database();

        final GraphCmd.ServiceEntityMultiGet cmd = new GraphCmd.ServiceEntityMultiGet(
                graphDefinition.getServiceEntityVertex(),
                entitiesToFind,
                databaseToUse);
        final Try<Collection<ServiceEntityRepoDTO>> seResults = executor.executeServiceEntityMultiGetCmd(cmd);
        logger.debug("Multi-entity search results {}", seResults);

        return Match(seResults).of(
                Case(Success($()), repoDtos -> timedValue(
                        () -> Either.right(TopologyConverter.convertToTopologyEntity(repoDtos)),
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
            final TopologyDatabase databaseToUse = topologyID.get().database();
            final GraphCmd.ServiceEntityMultiGet cmd = new GraphCmd.ServiceEntityMultiGet(
                    graphDefinition.getServiceEntityVertex(),
                    entitiesToFind,
                    databaseToUse);
            final Try<Collection<ServiceEntityRepoDTO>> seResults = executor.executeServiceEntityMultiGetCmd(cmd);
            logger.debug("Multi-entity search results {}", seResults);

            return Match(seResults).of(
                    Case(Success($()), repoDtos -> timedValue(
                            () -> Either.right(TopologyConverter.convertToTopologyEntity(repoDtos)),
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
