/*
 * (C) Turbonomic 2020.
 */

package com.vmturbo.components.common.utils;

/**
 * A function that can throw.
 *
 * @param <V> type of the input parameter.
 * @param <T> function return value type
 * @param <E> exception
 */
@FunctionalInterface
public interface ThrowingFunction<V, T, E extends Exception> {
    /**
     * Gets a result.
     *
     * @param v input parameter.
     * @return a result
     * @throws E when failed
     * @throws InterruptedException when interrupted
     */
    T apply(V v) throws E, InterruptedException;
}
