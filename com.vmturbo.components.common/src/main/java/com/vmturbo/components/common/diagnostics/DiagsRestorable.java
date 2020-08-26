package com.vmturbo.components.common.diagnostics;

import java.util.List;

import javax.annotation.Nonnull;

/**
 * This interface an object that its interface can be restored from diagnostics. Additionally,
 * the restore can have context object which is send to it and sent to all restoration processes.
 *
 * @param <T> the type of context object.
 */
public interface DiagsRestorable<T> extends StringDiagnosable {

    /**
     * Restores internal state of an object from the previously collected diagnostics information.
     *
     * @param collectedDiags previously collected diagnostics
     * @param context diagnostics load context.
     * @throws DiagnosticsException When an exception occurs during diagnostics collection.
     */
    void restoreDiags(@Nonnull List<String> collectedDiags, T context) throws DiagnosticsException;
}