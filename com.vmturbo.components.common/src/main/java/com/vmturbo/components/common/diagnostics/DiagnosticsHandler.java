package com.vmturbo.components.common.diagnostics;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

/**
 * Diagnostics handler is used to dump diagnostics information from different providers.
 */
public class DiagnosticsHandler implements IDiagnosticsHandler {

    /**
     * ZIP entry name to write errors to.
     */
    public static final String ERRORS_FILE = "dump_errors";

    private final Collection<Consumer<DiagnosticsWriter>> providers;

    /**
     * Constructs diagnostics handler with the specified diagnostics provider.
     *
     * @param diagnosables providers of diagnostics information
     */
    public DiagnosticsHandler(@Nonnull Collection<? extends Diagnosable> diagnosables) {
        this.providers = Collections.unmodifiableList(diagnosables.stream()
                .map(DiagnosticsHandler::getProvider)
                .collect(Collectors.toList()));
    }

    @Nonnull
    private static Consumer<DiagnosticsWriter> getProvider(@Nonnull Diagnosable diagnosable) {
        if (diagnosable instanceof StringDiagnosable) {
            final StringDiagnosable stringDiagnosable = (StringDiagnosable)diagnosable;
            return writer -> writer.dumpDiagnosable(stringDiagnosable);
        } else if (diagnosable instanceof BinaryDiagsRestorable) {
            final BinaryDiagnosable binaryDiagnosable = (BinaryDiagnosable)diagnosable;
            return writer -> writer.dumpDiagnosable(binaryDiagnosable);
        } else {
            throw new IllegalArgumentException(
                    "Diagnosable could not be processed: " + diagnosable.getClass());
        }
    }

    /**
     * Dumps the group component state to a {@link ZipOutputStream}.
     *
     * @param diagnosticZip The destination.
     */
    public void dump(@Nonnull final ZipOutputStream diagnosticZip) {
        final DiagnosticsWriter diagsWriter = new DiagnosticsWriter(diagnosticZip);
        for (Consumer<DiagnosticsWriter> provider : providers) {
            provider.accept(diagsWriter);
        }

        if (!diagsWriter.getErrors().isEmpty()) {
            diagsWriter.writeZipEntry(ERRORS_FILE, diagsWriter.getErrors().iterator());
        }
    }
}
