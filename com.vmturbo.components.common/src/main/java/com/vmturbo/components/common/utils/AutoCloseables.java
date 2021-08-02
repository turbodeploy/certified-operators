package com.vmturbo.components.common.utils;

import java.util.Collection;
import java.util.function.Function;

import javax.annotation.Nonnull;

/**
 * A small helper class that groups a collection of other {@link AutoCloseable}s for use
 * as a single collective resource in a try-with-resources block. When the top-level
 * {@link AutoCloseables} is closed, it will safely call close on all the internal resources.
 *
 * @param <T> The exception type to throw if one or more exception is thrown during close.
 */
public class AutoCloseables<T extends Exception> implements AutoCloseable {
    private final Collection<? extends AutoCloseable> autoCloseables;
    private final Function<Exception, T> exceptionSupplier;

    /**
     * Create a new {@link AutoCloseables} that will automatically close all the internal
     * {@link AutoCloseable}s in the inner collection. If one or more inner exceptions is thrown,
     * the exception supplier will be called with the top-level exception that was generated.
     * If more than one exception is thrown, the sub-exceptions will be added as suppressed
     * exceptions on the top-level exception.
     *
     * @param autoCloseables The collection of auto-closeables to automatically close when the
     *                       top-level AutoCloseable is closed.
     * @param exceptionSupplier Called if one or more exceptions are thrown during the close
     *                          of the internal auto-closeables.
     */
    public AutoCloseables(@Nonnull final Collection<? extends AutoCloseable> autoCloseables,
                          @Nonnull Function<Exception, T> exceptionSupplier) {
        this.autoCloseables = autoCloseables;
        this.exceptionSupplier = exceptionSupplier;
    }

    @Override
    public void close() throws T {
        Exception topLevelException = null;

        for (AutoCloseable autoCloseable : autoCloseables) {
            try {
                autoCloseable.close();
            } catch (Exception e) {
                if (topLevelException == null) {
                    topLevelException = e;
                } else {
                    topLevelException.addSuppressed(e);
                }
            }
        }

        if (topLevelException != null) {
            throw exceptionSupplier.apply(topLevelException);
        }
    }
}
