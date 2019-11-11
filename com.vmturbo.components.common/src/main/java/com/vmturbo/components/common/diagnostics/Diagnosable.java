package com.vmturbo.components.common.diagnostics;

import java.util.List;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

/**
 * The {@link Diagnosable} is an object that should be included into the diagnostics provided
 * by the component. Any object can implement this interface, and it's the responsibility of the
 * component author to actually inject all objects implementing this interface into the
 * object responsible for assembling the diags.
 * <p>
 * TODO (roman, Sept 7 2017): Right now most diagnostic collection happens outside of the objects
 * that own the data, so this interface isn't properly used. We should have the objects that
 * provide diags implement this interface, but this will require some refactoring.
 * <p>
 * TODO (roman, Sept 7 2017): The interface dealing with lists of strings is clumsy. It should
 * just be streams, and we should have a framework where a common diagnostics handler can
 * iterate through {@link Diagnosable}s and manage the streams given to each.
 */
public interface Diagnosable {
    /**
     * Save the diags as a Stream of strings {@link Diagnosable#restoreDiags}.
     *
     * @return The diags as a Stream of strings. The strings should NOT have newline characters.
     * @throws DiagnosticsException When an exception occurs during diagnostics collection.
     */
    @Nonnull
    Stream<String> collectDiagsStream() throws DiagnosticsException;

    /**
     * Restore the diags saved by {@link Diagnosable#collectDiagsStream()}.
     *
     * @param collectedDiags The diags collected from a previous call to
     *      {@link Diagnosable#collectDiagsStream()}. Must be in the same order.
     * @throws DiagnosticsException When an exception occurs during diagnostics restoration.
     */
    void restoreDiags(@Nonnull final List<String> collectedDiags) throws DiagnosticsException;

}
