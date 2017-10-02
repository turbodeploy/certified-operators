package com.vmturbo.repository.search;

import javaslang.collection.Stream;

import java.util.List;
import java.util.function.Function;

/**
 * A stage encapsulates a {@link SearchStageComputation}, and provides the structure/glue to compose multiple
 * {@link SearchStageComputation}s together.
 *
 * On a high level, executing a computation can be separated out to two levels. One level deals with
 * <em>WHAT</em> to run and the other level deals with <em>HOW</em> to run the computation.
 * A {@link SearchStage} handles the <em>how</em>, and the <em>what</em> is handled by {@link SearchStage#computation()}
 * which is of type {@link SearchStageComputation}.
 *
 * Only dealing with how to run a computation, a {@link SearchStage} provides the minimal structure for composing multiple
 * {@link SearchStage}s together using {@link SearchStage#andThen(SearchStage)} -- the output of the current {@link SearchStage}
 * becomes the input the subsequent {@link SearchStage}.
 *
 * @param <CTX> The type of the context needed by the {@link SearchStageComputation}.
 * @param <INPUT> The input type of the {@link SearchStageComputation}.
 * @param <OUTPUT> The output type of the {@link SearchStageComputation}.
 */
@FunctionalInterface
public interface SearchStage<CTX, INPUT, OUTPUT> {

    /**
     * The computation for this stage.
     *
     * @return a {@link SearchStageComputation}.
     */
    SearchStageComputation<CTX,INPUT,OUTPUT> computation();

    /**
     * Execute the {@link SearchStageComputation}.
     *
     * @param context The context needed by the {@link SearchStageComputation}.
     * @param input The input data for the {@link SearchStageComputation}.
     * @return The result of the {@link SearchStageComputation}.
     */
    default OUTPUT run(final CTX context, final INPUT input) {
        return computation().apply(context, input);
    }

    /**
     * Compose a {@link SearchStage} with the current {@link SearchStage}.
     *
     * @param nextSearchStage The {@link SearchStage} we want to compose.
     * @param <O2> The output type for the final composed {@link SearchStage}.
     * @return A composed {@link SearchStage}.
     */
    default <O2> SearchStage<CTX, INPUT, O2> andThen(final SearchStage<CTX, OUTPUT, O2> nextSearchStage) {
        return () -> (ctx, input) -> nextSearchStage.run(ctx, run(ctx, input));
    }

    /**
     * Map over the result of the {@link SearchStageComputation}.
     *
     * @param fn The mapping function.
     * @param <O2> The new output type of the {@link SearchStageComputation}.
     * @return A new {@link SearchStage}.
     */
    default <O2> SearchStage<CTX, INPUT, O2> fmap(final Function<OUTPUT, O2> fn) {
        return () -> (ctx, input) -> fn.apply(run(ctx, input));
    }

    /**
     * Flat map over the result of the {@link SearchStageComputation}.
     *
     * @param mb The function that takes the result of the current {@link SearchStageComputation}.
     * @param <OUT2> The output type of the {@link SearchStage} after flat map.
     * @return A new {@link SearchStage}.
     */
    default <OUT2> SearchStage<CTX, INPUT, OUT2> flatMap(Function<OUTPUT, SearchStage<CTX, INPUT, OUT2>> mb) {
        return () -> (ctx, i) -> mb.apply(run(ctx, i)).run(ctx, i);
    }

    /**
     * The identity of a {@link SearchStage}.
     *
     * @param <C> The context type.
     * @param <I> The input type.
     * @return An identity {@link SearchStage}.
     */
    static <C,I> SearchStage<C,I,I> identity() {
        return () -> (ctx, i) -> i;
    }

    /**
     * Lift a normal function into the {@link SearchStage} context.
     *
     * @param fn The function to be lifted.
     * @param <C> The context type.
     * @param <I> The input type.
     * @param <O1> The output type of the mapping {@link SearchStage}.
     * @param <O2> The output type of the mapped {@link SearchStage}.
     * @return A lifted function that takes a {@link SearchStage} and returns a {@link SearchStage}.
     */
    static <C,I,O1,O2> Function<SearchStage<C,I,O1>, SearchStage<C,I,O2>> lift(final Function<O1,O2> fn) {
        return s -> s.fmap(fn);
    }

    /**
     * Fold a collection of {@link SearchStage}s into a single {@link SearchStage}.
     *
     * @param searchStages The searchStages to be folded.
     * @param <C> The context type.
     * @param <I> The input type.
     * @return A folded {@link SearchStage}.
     */
    static <C,I> SearchStage<C,I,I> fold(final List<SearchStage<C,I,I>> searchStages) {
        return Stream.ofAll(searchStages).foldLeft(identity(), SearchStage::andThen);
    }

    /**
     * Lift a value in the {@link SearchStage} context.
     *
     * @param v The value to be lifted.
     * @param <C> The context type.
     * @param <I> The input type.
     * @param <O> The output type.
     * @return A new {@link SearchStage} when run will return the given value.
     */
    static <C,I,O> SearchStage<C,I,O> pure(final O v) {
        return () -> (ctx, i) -> v;
    }
}
