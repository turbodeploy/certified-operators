package com.vmturbo.repository.graph.executor;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;
import com.vmturbo.repository.graph.parameter.GraphCmd;
import com.vmturbo.repository.graph.parameter.GraphCmd.SupplyChainDirection;
import com.vmturbo.repository.graph.result.GlobalSupplyChainFluxResult;
import com.vmturbo.repository.graph.result.ImmutableGlobalSupplyChainFluxResult;
import com.vmturbo.repository.graph.result.ImmutableSupplyChainFluxResult;
import com.vmturbo.repository.graph.result.ScopedEntity;
import com.vmturbo.repository.graph.result.SupplyChainFluxResult;
import com.vmturbo.repository.graph.result.TypeAndOids;
import com.vmturbo.repository.topology.TopologyDatabase;
import com.vmturbo.repository.topology.TopologyDatabases;
import javaslang.collection.List;
import javaslang.control.Try.CheckedSupplier;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static javaslang.API.Case;
import static javaslang.API.Match;

/**
 * An ArangoDB executor with async support.
 */
public class ReactiveArangoDBExecutor implements ReactiveGraphDBExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReactiveArangoDBExecutor.class);

    private final ArangoDatabaseFactory arangoDatabaseFactory;
    private final ObjectMapper objectMapper;
    private final Scheduler aqlQueryScheduler;

    public ReactiveArangoDBExecutor(final ArangoDatabaseFactory arangoDatabaseFactoryArg,
                                    final ObjectMapper objectMapperArg) {
        arangoDatabaseFactory = Objects.requireNonNull(arangoDatabaseFactoryArg);
        objectMapper = Objects.requireNonNull(objectMapperArg);
        aqlQueryScheduler = Schedulers.newParallel("AQL-Executor", 20);
    }

    private static String getSupplyChainQuery(final SupplyChainDirection direction,
                                              final GraphCmd.GetSupplyChain supplyChainCmd) {

        final String startingId = Joiner.on("/").join(supplyChainCmd.getVertexCollection(),
                                                      supplyChainCmd.getStartingVertex());

        final String arangoTraverseDirection = Match(direction).of(
                Case(SupplyChainDirection.PROVIDER, "INBOUND"),
                Case(SupplyChainDirection.CONSUMER, "OUTBOUND")
        );

        final Map<String, String> valuesMap = new ImmutableMap.Builder<String, String>()
                .put("edgeCollection", supplyChainCmd.getGraphName())
                .put("startingId", startingId)
                .put("vertexCollection", supplyChainCmd.getVertexCollection())
                .put("direction", arangoTraverseDirection)
                .build();

        final StrSubstitutor substitutor = new StrSubstitutor(valuesMap);

        return substitutor.replace(ArangoDBQueries.SUPPLY_CHAIN_REACTIVE_QUERY_STRING);
    }

    private static String getGlobalSupplyChainQuery(final GraphCmd.GetGlobalSupplyChain globalSupplyChainCmd) {
        final Map<String, String> valuesMap = new ImmutableMap.Builder<String, String>()
                .put("seCollection", globalSupplyChainCmd.getVertexCollection())
                .build();

        final StrSubstitutor substitutor = new StrSubstitutor(valuesMap);

        return substitutor.replace(ArangoDBQueries.GLOBAL_SUPPLY_CHAIN_REACTIVE_QUERY_STRING);
    }

    private static String getSupplyChainOriginQuery(final GraphCmd.GetSupplyChain supplyChainCmd) {
        final Map<String, String> valuesMap = new ImmutableMap.Builder<String, String>()
                .put("collection", supplyChainCmd.getVertexCollection())
                .put("commaSepLongs", supplyChainCmd.getStartingVertex())
                .build();

        final StrSubstitutor substitutor = new StrSubstitutor(valuesMap);

        return substitutor.replace(ArangoDBQueries.GET_ENTITIES_BY_OID);

    }

    private static String scopedEntitiesQuery(final String collection, final List<Long> oids) {
        final String oidStrings = oids.map(l -> Long.toString(l)).mkString(", ");

        final Map<String, String> valuesMap = new ImmutableMap.Builder<String, String>()
                .put("collection", collection)
                .put("oids", oidStrings)
                .build();
        final StrSubstitutor substitutor = new StrSubstitutor(valuesMap);

        return substitutor.replace(ArangoDBQueries.SCOPED_ENTITIES_BY_OID);
    }

    @Override
    public SupplyChainFluxResult executeSupplyChainCmd(final GraphCmd.GetSupplyChain supplyChainCmd) {
        final String providerQuery = getSupplyChainQuery(SupplyChainDirection.PROVIDER, supplyChainCmd);
        final String consumerQuery = getSupplyChainQuery(SupplyChainDirection.CONSUMER, supplyChainCmd);
        final String originQuery   = getSupplyChainOriginQuery(supplyChainCmd);
        final String databaseName  = TopologyDatabases.getDbName(supplyChainCmd.getTopologyDatabase());

        final Flux<TypeAndOids> providerStream = fluxify(databaseName, providerQuery, TypeAndOids.class)
                .doOnError(err -> LOGGER.error("Provider query failed with query " + providerQuery, err));
        final Flux<TypeAndOids> consumerStream = fluxify(databaseName, consumerQuery, TypeAndOids.class)
                .doOnError(err -> LOGGER.error("Consumer query failed with query " + consumerQuery, err));
        final Flux<ServiceEntityRepoDTO> originStream = fluxify(databaseName, originQuery, ServiceEntityRepoDTO.class)
                .doOnError(err -> LOGGER.error("Origin query failed with query " + originQuery, err));

        final SupplyChainFluxResult fluxResults = ImmutableSupplyChainFluxResult.builder()
                .providerResults(providerStream)
                .consumerResults(consumerStream)
                .origin(originStream.next())
                .build();

        return fluxResults;
    }

    @Override
    public GlobalSupplyChainFluxResult executeGlobalSupplyChainCmd(final GraphCmd.GetGlobalSupplyChain globalSupplyChainCmd) {
        final String globalQuery = getGlobalSupplyChainQuery(globalSupplyChainCmd);
        final String databaseName = TopologyDatabases.getDbName(globalSupplyChainCmd.getTopologyDatabase());

        final Flux<TypeAndOids> stream = fluxify(databaseName, globalQuery, TypeAndOids.class)
                .doOnError(err -> LOGGER.error("Error encountered while executing query: " + globalQuery, err));

        final GlobalSupplyChainFluxResult result = ImmutableGlobalSupplyChainFluxResult.builder()
                .entities(stream)
                .build();

        return result;
    }

    @Override
    public Flux<ScopedEntity> fetchScopedEntities(final TopologyDatabase database,
                                                  final String collection,
                                                  final List<Long> oids) {
        final String databaseName = TopologyDatabases.getDbName(database);
        final String query = scopedEntitiesQuery(collection, oids);

        return fluxify(databaseName, query, ScopedEntity.class);
    }

    private <R> Flux<R> fluxify(final String databaseName,
                                final String query,
                                final Class<R> klass) {
        return Flux.using(
                arangoDatabaseFactory::getArangoDriver,
                driver -> fluxify(() -> driver.db(databaseName).query(query, Collections.emptyMap(), null, String.class),
                                  klass),
                ArangoDB::shutdown);
    }

    private <R> Flux<R> fluxify(final CheckedSupplier<ArangoCursor<String>> cursorSupplier, Class<R> klass) {
        return Flux.<R>create(fluxSink -> {
            try {
                final ArangoCursor<String> resultCursor = cursorSupplier.get();
                while (resultCursor.hasNext() && !fluxSink.isCancelled()) {
                    final String next = resultCursor.next();
                    final R obj = objectMapper.readValue(next, klass);
                    fluxSink.next(obj);
                }
                fluxSink.complete();
            } catch (Throwable e) {
                fluxSink.error(e);
            }
        }).publishOn(aqlQueryScheduler);
    }
}
