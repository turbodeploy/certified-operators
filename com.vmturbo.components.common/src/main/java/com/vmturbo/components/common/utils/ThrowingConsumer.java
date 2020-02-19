package com.vmturbo.components.common.utils;

/**
 * A consumer that can throw.
 *
 * @param <T> consumer argument
 * @param <E> exception
 */
@FunctionalInterface
public interface ThrowingConsumer<T, E extends Exception> {
    /**
     * Process the argument, possibly throwing an exception.
     *
     * @param t argument
     * @throws E exception to throw
     * @throws InterruptedException when execution is interrupted
     */
    void accept(T t) throws E, InterruptedException;
}
