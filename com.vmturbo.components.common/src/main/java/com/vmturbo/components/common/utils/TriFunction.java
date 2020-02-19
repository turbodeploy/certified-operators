package com.vmturbo.components.common.utils;

/**
 * A function with three arguments that returns a result.
 *
 * @param <FirstT> first argument type
 * @param <SecondT> second argument type
 * @param <ThirdT> third argument type
 * @param <ResultT> result type
 */
@FunctionalInterface
public interface TriFunction<FirstT, SecondT, ThirdT, ResultT> {
    /**
     * Apply the function.
     *
     * @param a first argument
     * @param b second argument
     * @param c third argument
     * @return result
     */
    ResultT apply(FirstT a, SecondT b, ThirdT c);
}
