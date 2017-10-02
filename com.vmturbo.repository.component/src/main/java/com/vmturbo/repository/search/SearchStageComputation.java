package com.vmturbo.repository.search;

/**
 * A computation within a {@link SearchStage}.
 *
 * A stage computation can be generalized as a 2-arity function.
 * The first argument to the function is a <code>context</code>, which can be anything needed
 * when running the computation. The second argument is the input data for the computation.
 *
 * @param <CTX> The type of the context needed by the computation.
 * @param <INPUT> The input type of the computation.
 * @param <OUTPUT> The output type of the computation
 */
@FunctionalInterface
public interface SearchStageComputation<CTX, INPUT, OUTPUT> {

    /**
     * Run the computation.
     *
     * @param context The context needed by the computation.
     * @param input The input needed by the computation.
     * @return The result of the computation.
     */
    OUTPUT apply(final CTX context, final INPUT input);
}
