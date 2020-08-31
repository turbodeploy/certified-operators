package com.vmturbo.components.common.diagnostics;

import java.io.IOException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An instance that is able to dump diagnostics in a binary format.
 *
 * @param <T> the type of context object.
 */
public interface BinaryDiagsRestorable<T> extends BinaryDiagnosable {

    /**
     * Restores internal state of an object from the previously collected diagnostics information.
     *
     * @param bytes previously collected diagnostics
     * @param context the context object for diagnostics restoration.
     * @throws DiagnosticsException When an exception occurs during diagnostics collection.
     * @throws IOException when reading from diagnostics input stream failed
     */
    void restoreDiags(@Nonnull byte[] bytes, @Nullable T context) throws DiagnosticsException;
}
