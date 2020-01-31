package com.vmturbo.components.common.diagnostics;

import java.io.InputStream;

import javax.annotation.Nonnull;

/**
 * Diagnostics handler abstraction able to import diagnostics.
 */
public interface IDiagnosticsHandlerImportable extends IDiagnosticsHandler {
    /**
     * Restore component's state from diagnostics.
     *
     * @param inputStream diagnostics streamed bytes
     * @return list of errors if faced
     * @throws DiagnosticsException if diagnostics failed to restore.
     */
    @Nonnull
    String restore(@Nonnull InputStream inputStream) throws DiagnosticsException;
}
