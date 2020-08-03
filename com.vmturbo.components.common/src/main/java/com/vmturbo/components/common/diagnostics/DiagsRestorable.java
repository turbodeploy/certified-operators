package com.vmturbo.components.common.diagnostics;

import java.util.List;

import javax.annotation.Nonnull;

/**
 * Interface of an object able to restore its state from the diagnostics, previously generated
 * by {@link StringDiagnosable}. Zip entries for implementations of this interface are
 * suffixed with {@link DiagsZipReader#TEXT_DIAGS_SUFFIX} in order to be distinguished
 * as string diagnostics files for restoring.
 */
public interface DiagsRestorable extends StringDiagnosable {
    /**
     * Restores internal state of an object from the previously collected diagnostics information.
     *
     * @param collectedDiags previously collected diagnostics
     * @throws DiagnosticsException When an exception occurs during diagnostics collection.
     */
    void restoreDiags(@Nonnull List<String> collectedDiags) throws DiagnosticsException;
}
