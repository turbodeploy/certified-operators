package com.vmturbo.components.common.diagnostics;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

public interface StreamingDiagnosable {
    /**
     * Save the diags as a list of strings {@link Diagnosable#restoreDiags}.
     *
     * @throws DiagnosticsException When an exception occurs during diagnostics collection.
     *
     * @return The diags as a list of strings. The strings should NOT have newline characters.
     */
    @Nonnull
    Stream<String> collectDiags() throws DiagnosticsException;

    /**
     * Restore the diags saved by {@link Diagnosable#collectDiagsStream()}.
     *
     * @param collectedDiags The diags collected from a previous call to
     *      {@link Diagnosable#collectDiagsStream()}. Must be in the same order.
     * @throws DiagnosticsException When an exception occurs during diagnostics restoration.
     */
    void restoreDiags(@Nonnull final Stream<String> collectedDiags) throws DiagnosticsException;
}
