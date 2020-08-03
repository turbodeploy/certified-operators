package com.vmturbo.components.common.diagnostics;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.Nonnull;

/**
 * Object able to dump diagnostics in binary format.
 */
public interface BinaryDiagnosable extends Diagnosable {
    /**
     * Save the diags as sequence of bytes sent to {@link OutputStream}.
     *
     * @param appender stream to put diagnostics bytes to.
     * @throws DiagnosticsException When an exception occurs during diagnostics collection.
     * @throws IOException when error occurred writing to the output stream
     */
    void collectDiags(@Nonnull OutputStream appender) throws DiagnosticsException, IOException;
}
