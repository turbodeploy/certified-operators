/**
 * (C) Turbonomic 2019.
 */

package com.vmturbo.components.common.utils;

/**
 * A function that can throw.
 *
 * @param <R> function return value
 * @param <T> function argument
 * @param <E> exception
 */
@FunctionalInterface
public interface ThrowingFunction<T, R, E extends Exception> {
    /**
     * Process the argument, possibly throwing an exception.
     *
     * @param t argument
     * @return result of function call.
     * @throws E exception to throw
     */
    R apply(T t) throws E;
}
