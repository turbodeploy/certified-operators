package com.vmturbo.components.common.diagnostics;

import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

/**
 * Diagnostics handler abstraction able to dump diagnostics.
 */
public interface IDiagnosticsHandler {
    /**
     * Dumps the group component state to a {@link ZipOutputStream}.
     *
     * @param diagnosticZip The destination.
     */
    void dump(@Nonnull ZipOutputStream diagnosticZip);

}
