package com.vmturbo.components.common.diagnostics;

import java.io.InputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Diagnostics handler abstraction able to import diagnostics.
 *
 * @param <T> context object. This should not be null if {@code T} is not {@code Void}.
 */
public interface IDiagnosticsHandlerImportable<T> extends IDiagnosticsHandler {
    /**
     * Restore component's state from diagnostics.
     *
     * @param inputStream diagnostics streamed bytes
     * @param context the context object.
     * @return list of errors if faced
     * @throws DiagnosticsException if diagnostics failed to restore.
     */
    @Nonnull
    String restore(@Nonnull InputStream inputStream, @Nullable T context) throws DiagnosticsException;
}
