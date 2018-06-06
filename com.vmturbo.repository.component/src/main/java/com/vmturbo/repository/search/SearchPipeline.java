package com.vmturbo.repository.search;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javaslang.control.Either;

/**
 * A pipeline of {@link SearchStage}s.
 *
 * @param <CTX> The context type for the pipeline.
 */
public class SearchPipeline<CTX> {

    private static final String ALL_ENTITIES = "ALL";

    private List<SearchStage<CTX, Collection<String>, Collection<String>>> stages;

    // The prefix of service entity vertex key, it is used for helping convert vertex key back to entity oid.
    private String serviceEntityVertex;

    public SearchPipeline(final List<SearchStage<CTX, Collection<String>, Collection<String>>> stagesArg,
                          final String serviceEntityVertex) {
        this.stages = Objects.requireNonNull(stagesArg, "The list of SearchStage cannot be null");
        this.serviceEntityVertex = Objects.requireNonNull(serviceEntityVertex);

    }

    /**
     * Run the pipeline.
     *
     * Each {@link SearchStage} will be executed in sequence.
     *
     * @param context The context for the pipeline and {@link SearchStage}.
     * @param oids A list of entity oid, if it is not empty, the search query will only apply on those
     *             entity oids, the search results will be subset of those entity oids.
     * @return The output of the pipeline or an error.
     */
    public Either<Throwable, Collection<String>> run(final CTX context, final Collection<String> oids) {
        return run(context, ids -> new SearchStage<CTX, Collection<String>, Collection<String>>() {
            @Override
            public SearchStageComputation<CTX, Collection<String>, Collection<String>> computation() {
                return (ctx, in) -> {
                    // Strip the vertex prefix with the end of a '/'
                    final int prefixIndex = serviceEntityVertex.length() + 1;
                    return ids.stream().map(id -> id.substring(prefixIndex)).collect(Collectors.toList());
                };
            }
        }, oids);
    }

    /**
     * Run the pipeline and use the given to transform the final value of the pipeline.
     *
     * @param context The context for the pipeline and {@link SearchStage}.
     * @param fn The function to transform the output of the pipeline.
     * @param <RET> The return type after the transformation.
     * @param oids A list of entity oid, if it is not empty, the search query will only apply on those
     *             entity oids, the search results will be subset of those entity oids.
     * @return The final output of type <code>RET</code> or an error.
     */
    public <RET> Either<Throwable, RET> run(
            final CTX context,
            final Function<Collection<String>, SearchStage<CTX, Collection<String>, RET>> fn,
            final Collection<String> oids) {
        try {
            final Collection<String> inputs = oids.isEmpty() ? Collections.singleton(ALL_ENTITIES) : oids;
            final SearchStage<CTX, Collection<String>, RET> pipeline = SearchStage.fold(stages).flatMap(fn);
            final RET results = pipeline.run(context, inputs);
            return Either.right(results);
        } catch (RuntimeException e) {
            return Either.left(e);
        }
    }
}
