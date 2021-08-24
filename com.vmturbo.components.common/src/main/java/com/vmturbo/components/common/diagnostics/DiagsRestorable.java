package com.vmturbo.components.common.diagnostics;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
     * Either this or {@link DiagsRestorable#restoreDiags(Stream, Object)} should be implemented.
     *
     * @param collectedDiags previously collected diagnostics
     * @param context diagnostics load context.
     * @throws DiagnosticsException When an exception occurs during diagnostics collection.
     */
    default void restoreDiags(@Nonnull List<String> collectedDiags, T context) throws DiagnosticsException {
        // do nothing by default
    }

    /**
     * Restores internal state of an object from the previously collected diagnostics information.
     * By default, the stream is collected into a list and routed to
     * {@link DiagsRestorable#restoreDiags(List, Object)}. But it can be overridden to process
     * line by line if the diags may contain huge data which can't fit into a byte array or list.
     *
     * @param diagsLineStream line string stream of the collected diagnostics
     * @param context diagnostics load context.
     * @throws DiagnosticsException When an exception occurs during diagnostics collection.
     */
    default void restoreDiags(@Nonnull Stream<String> diagsLineStream, T context) throws DiagnosticsException {
        restoreDiags(diagsLineStream.collect(Collectors.toList()), context);
    }
}