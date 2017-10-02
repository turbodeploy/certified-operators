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

import com.vmturbo.api.dto.ServiceEntityApiDTO;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.graph.parameter.GraphCmd;
import com.vmturbo.repository.graph.result.ResultsConverter;
import com.vmturbo.repository.graph.result.SupplyChainExecutorResult;
import com.vmturbo.repository.topology.TopologyDatabase;
import com.vmturbo.repository.topology.TopologyIDManager;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyID;

public class GraphDBService {
    private final Logger logger = LoggerFactory.getLogger(GraphDBService.class);
    private final GraphDBExecutor executor;
    private final GraphDefinition graphDefinition;
    private final TopologyIDManager topologyIDManager;

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
                          final TopologyIDManager topologyIDManager) {
        this.executor = checkNotNull(executor);
        this.graphDefinition = checkNotNull(graphDefinition);
        this.topologyIDManager = checkNotNull(topologyIDManager);
    }

    /**
     * Construct a supply chain starting from the given service entity.
     *
     * @param contextID The name of the database to create the supply chain from.
     *                   If <code>empty</code>, use the real-time database.
     * @param startId The identifier of the starting service entity.
     * @return Either a String describing an error, or a stream of {@link SupplyChainNode}s.
     */
    public Either<String, java.util.stream.Stream<SupplyChainNode>> getSupplyChain(final Optional<String> contextID,
                                                                                   final String startId) {
        final Option<String> maybeContextID = Option.ofOptional(contextID);
        final Option<TopologyDatabase> databaseToUse =
            maybeContextID.map(topologyIDManager::databaseOf)
                .orElse(Option.ofOptional(topologyIDManager.currentRealTimeDatabase()));

        // Use the topology associated with the `contextID`, or the real-time topology.
        return databaseToUse.map(topologyDB -> {
            final GraphCmd.GetSupplyChain cmd = new GraphCmd.GetSupplyChain(
                startId,
                topologyDB,
                graphDefinition.getProviderRelationship(),
                graphDefinition.getServiceEntityVertex());
            logger.debug("Constructed command, {}", cmd);

            final Try<SupplyChainExecutorResult> supplyChainResults = executor.executeSupplyChainCmd(cmd);
            logger.debug("Values returned by the GraphExecutor {}", supplyChainResults);

            final Option<TopologyID> maybeRealTimeID = Option.ofOptional(
                topologyIDManager.getCurrentRealTimeTopologyId());
            final Option<Long> contextIDToUse = maybeContextID.map(Long::parseLong)
                .orElse(maybeRealTimeID.map(TopologyID::getContextId));

            final Option<Either<String, java.util.stream.Stream<SupplyChainNode>>> supplyChainResult =
                contextIDToUse.map(cid ->
                    Match(supplyChainResults).of(
                        Case(Success($()), v -> timedValue(() -> Either.right(
                                // Prune the supply chain before conversion.
                                ResultsConverter.toSupplyChainNodes(v.prune(startId))),
                            SINGLE_SOURCE_SUPPLY_CHAIN_CONVERSION_DURATION_SUMMARY)),
                        Case(Failure($()), exc -> Either.left(exc.getMessage()))
                    ));

            return supplyChainResult.getOrElse(Either.right(java.util.stream.Stream.empty()));
        })
        // Real-time topology database is not yet created.
        .getOrElse(() -> {
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
            @Nonnull final Optional<String> contextId,
            @Nonnull final Set<Long> entitiesToFind) {

        final Option<String> maybeContextID = Option.ofOptional(contextId);
        final Option<TopologyDatabase> databaseToUse =
                maybeContextID.map(topologyIDManager::databaseOf)
                              .orElse(Option.ofOptional(topologyIDManager.currentRealTimeDatabase()));

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
                            () -> Either.right(repoDtos.stream().map(ResultsConverter::toServiceEntityApiDTO)
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
           final Optional<String> contextId,
           final String field,
           final String query,
           final GraphCmd.SearchType searchType) {

        final Option<String> maybeContextID = Option.ofOptional(contextId);
        final Option<TopologyDatabase> databaseToUse =
                maybeContextID.map(topologyIDManager::databaseOf)
                              .orElse(Option.ofOptional(topologyIDManager.currentRealTimeDatabase()));

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
                            () -> Either.right(v.stream().map(ResultsConverter::toServiceEntityApiDTO)
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

    private <T> T timedValue(final Supplier<T> supplier, final DataMetricSummary summary) {
        DataMetricTimer timer = summary.startTimer();
        final T result = supplier.get();
        timer.observe();

        return result;
    }
}
