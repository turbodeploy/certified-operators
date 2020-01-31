package com.vmturbo.components.common.diagnostics;

import java.io.IOException;

import javax.annotation.Nonnull;

/**
 * An instance that is able to dump diagnostics in a binary format.
 */
public interface BinaryDiagsRestorable extends BinaryDiagnosable {

    /**
     * Restores internal state of an object from the previously collected diagnostics information.
     *
     * @param bytes previously collected diagnostics
     * @throws DiagnosticsException When an exception occurs during diagnostics collection.
     * @throws IOException when reading from diagnostics input stream failed
     */
    void restoreDiags(@Nonnull byte[] bytes) throws DiagnosticsException;
}
