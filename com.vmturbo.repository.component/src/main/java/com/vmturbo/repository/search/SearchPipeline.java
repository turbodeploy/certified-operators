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

    public SearchPipeline(final List<SearchStage<CTX, Collection<String>, Collection<String>>> stagesArg) {
        this.stages = Objects.requireNonNull(stagesArg, "The list of SearchStage cannot be null");
    }

    /**
     * Run the pipeline.
     *
     * Each {@link SearchStage} will be executed in sequence.
     *
     * @param context The context for the pipeline and {@link SearchStage}.
     *
     * @return The output of the pipeline or an error.
     */
    public Either<Throwable, Collection<String>> run(final CTX context) {
        return run(context, ids -> new SearchStage<CTX, Collection<String>, Collection<String>>() {
            @Override
            public SearchStageComputation<CTX, Collection<String>, Collection<String>> computation() {
                return (ctx, in) ->
                        // Strip from the keys everything all the way up to a '/'. '/' is the collection separator.
                        ids.stream().map(id -> id.replaceFirst(".*/", "")).collect(Collectors.toList());
            }
        });
    }

    /**
     * Run the pipeline and use the given to transform the final value of the pipeline.
     *
     * @param context The context for the pipeline and {@link SearchStage}.
     * @param fn The function to transform the output of the pipeline.
     * @param <RET> The return type after the transformation.
     * @return The final output of type <code>RET</code> or an error.
     */
    public <RET> Either<Throwable, RET> run(
            final CTX context,
            final Function<Collection<String>, SearchStage<CTX, Collection<String>, RET>> fn) {
        try {
            final SearchStage<CTX, Collection<String>, RET> pipeline = SearchStage.fold(stages).flatMap(fn);
            final RET results = pipeline.run(context, Collections.singleton(ALL_ENTITIES));
            return Either.right(results);
        } catch (RuntimeException e) {
            return Either.left(e);
        }
    }
}
