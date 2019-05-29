package com.vmturbo.repository.search;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoCursor;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

import javaslang.collection.Iterator;
import javaslang.collection.Seq;
import javaslang.collection.Stream;
import javaslang.concurrent.Future;

import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.OptScope;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.dto.ServiceEntityRepoDTO;
import com.vmturbo.repository.graph.executor.AQL;
import com.vmturbo.repository.graph.executor.AQLs;

/**
 * A {@link SearchStageComputation} for running ArangoDB queries.
 */
public class ArangoDBSearchComputation implements SearchStageComputation<SearchComputationContext,
                                                                         Collection<String>,
                                                                         Collection<String>> {

    private static final Logger LOG = LoggerFactory.getLogger(ArangoDBSearchComputation.class);
    private static final int INPUT_CHUNK_SIZE = 50000;

    private final AQL aqlQuery;

    private final static Map<String, Function<SearchComputationContext, Object>> bindValuesMapper =
            ImmutableMap.of("graph", SearchComputationContext::graphName,
                            "allKeyword", SearchComputationContext::allKeyword,
                            "@serviceEntityCollection", SearchComputationContext::entityCollectionName);

    public ArangoDBSearchComputation(final AQL query) {
        aqlQuery = Objects.requireNonNull(query, "AQL cannot be null");
    }

    /**
     * Execute the AQL query and return the results inside a {@link Future}.
     *
     * @param context The context for the query.
     * @param inputs The inputs for the query.
     * @return A {@link Future}.
     */
    private Future<Collection<String>> executeAQL(final SearchComputationContext context,
                                                  final Collection<String> inputs) {
        return Future.of(context.executorService(), () -> {
            try (OptScope scope = Tracing.addOpToTrace("Arango - " + context.traceID())) {
                final String queryString = AQLs.getQuery(aqlQuery);
                final Collection<String> vars = AQLs.getBindVars(aqlQuery);
                final Map<String, Object> bindVars = vars.stream()
                    .filter(v -> !v.equals("inputs")) // `inputs` does not come from the context. Skip it.
                    .collect(Collectors.toMap(Function.identity(), v -> bindValuesMapper.get(v).apply(context)));
                bindVars.put("inputs", inputs);

                LOG.debug("pipeline ({}) running {} with first 10 inputs {}",
                    context.traceID(),
                    queryString,
                    Stream.ofAll(inputs).take(10).toJavaList());
                LOG.debug("pipeline ({}) running {} with bindVars {}", context.traceID(), queryString, bindVars);

                // We don't close the cursor because it gets auto-closed by the server
                // get all the results.
                final ArangoCursor<String> cursor = context.arangoDB().db(context.databaseName())
                    .query(queryString, bindVars, null, String.class);
                final List<String> results = cursor.asListRemaining();
                return (results == null) ? Collections.emptyList() : deduplicateWithOrder(results);
            }
        });
    }

    /**
     * Deduplicate the input list and also preserve the original order.
     *
     * @param input a list of string
     * @return a list of string after deduplicate and keep original order.
     */
    private List<String> deduplicateWithOrder(@Nonnull List<String> input) {
        final List<String> results = new ArrayList<>();
        final Set<String> distinctResults = new LinkedHashSet<>(input);
        results.addAll(distinctResults);
        return results;
    }

    @Override
    public Collection<String> apply(final SearchComputationContext context,
                                    final Collection<String> ids) {
        try(final DataMetricTimer ignored = context.summary().startTimer()) {
            // Chunk the input
            final Iterator<javaslang.collection.List<String>> idChunks =
                javaslang.collection.List.ofAll(ids).grouped(INPUT_CHUNK_SIZE);

            // Turn each chunk into a future
            final Iterator<Future<Collection<String>>> futures = idChunks.map(
                    idChunk -> executeAQL(context, idChunk.toJavaList()));

            // Combine the futures into one
            final Future<Seq<Collection<String>>> combineResults = Future.sequence(context.executorService(), futures);

            // Flatten the results
            final Future<java.util.List<String>> results = combineResults
                    .map(seqColl -> seqColl.flatMap(coll -> coll).toJavaList());

            results.onFailure(err -> LOG.error("Exception encountered when running a search query " + aqlQuery, err));

            return results.get();
        }
    }

    /**
     * Perform an extra step to convert the list of IDs to actual {@link ServiceEntityRepoDTO} objects.
     *
     * The converted {@link ServiceEntityRepoDTO} objects do not contain the commodities data.
     *
     * This should be used as the second parameter of {@link SearchPipeline#run(Object, Function)}.
     */
    public static Function<Collection<String>, SearchStage<SearchComputationContext,
                                                           Collection<String>,
                                                           List<ServiceEntityRepoDTO>>> toEntities =
            // The IDs that come in are qualified with the collection name.
            vertexIds -> () -> (ctx, in) -> {
                final String query;
                if (ctx.fullEntity()) {
                    // retrieve full ServiceEntityRepoDTO
                    query = "FOR se IN @@serviceEntityCollection\n" +
                            "FILTER se._id IN @inputs\n" +
                            "RETURN { \"vertexId\" : se._id, " +
                                     "\"dto\" : se" +
                                    "}";
                } else {
                    query = "FOR se IN @@serviceEntityCollection\n" +
                            "FILTER se._id IN @inputs\n" +
                            "RETURN { \"vertexId\" : se._id, " +
                                     "\"dto\" : {" +
                                           "uuid: se.uuid," +
                                           "oid: se.oid," +
                                           "displayName: se.displayName," +
                                           "state: se.state," +
                                           "severity: se.severity," +
                                           "entityType: se.entityType" +
                                       "}" +
                                     "}";
                }

                final Map<String, Object> bindVars = ImmutableMap.of(
                        "@serviceEntityCollection", ctx.entityCollectionName(),
                        "inputs", vertexIds);

                final Future<ArangoCursor<ToEntityQueryReturn>> cursor =
                        Future.of(ctx.executorService(), () -> {
                            try (OptScope scope = Tracing.addOpToTrace("to entities")) {
                                LOG.debug("pipeline ({}) converting to entities using {}", ctx.traceID(), query);

                                return ctx.arangoDB()
                                    .db(ctx.databaseName())
                                    .query(query, bindVars, null, ToEntityQueryReturn.class);
                            }
                        });

                cursor.onFailure(err ->
                        LOG.error("Encountered exception while getting to ServiceEntities", err));

                // To preserve the results of pagination, we need to return the ServiceEntityRepoDTOs
                // in the same order that the vertexIds were in.

                // Need to arrange results by the vertex ID (the _id field) in order to
                // match them against the input ids.
                final Map<String, ServiceEntityRepoDTO> repoDtosByVertexId = new HashMap<>();
                cursor.get().forEachRemaining(queryReturn -> repoDtosByVertexId.put(queryReturn.getVertexId(), queryReturn.getDto()));

                return vertexIds.stream()
                        .map(repoDtosByVertexId::get)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
            };

    /**
     * Utility class for the {@link ArangoDBSearchComputation#toEntities} AQL query, to allow
     * returning the "_id" field alongside the {@link ServiceEntityRepoDTO}.
     */
    public static class ToEntityQueryReturn {

        /**
         * The ID of the vertex in arango. This is the "_id" field.
         */
        private String vertexId;

        private ServiceEntityRepoDTO dto;

        public String getVertexId() {
            return vertexId;
        }

        public void setVertexId(final String id) {
            this.vertexId = id;
        }

        public ServiceEntityRepoDTO getDto() {
            return dto;
        }

        public void setDto(final ServiceEntityRepoDTO dto) {
            this.dto = dto;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ArangoDBSearchComputation)) return false;
        ArangoDBSearchComputation that = (ArangoDBSearchComputation) o;
        return Objects.equals(aqlQuery, that.aqlQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aqlQuery);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("aqlQuery", aqlQuery)
                .toString();
    }
}
