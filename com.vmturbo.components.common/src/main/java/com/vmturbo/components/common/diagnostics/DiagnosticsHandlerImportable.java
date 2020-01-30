package com.vmturbo.components.common.diagnostics;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Diagnostics handler for importable diags. It extends {@link DiagnosticsHandler} with a
 * 'restore from diags' function.
 *
 * <p>If some of the restorable diagnosable failed to load its state from the associated file,
 * other diagnosable (and other files) will be tried nevertheless. As this is mostly a
 * debugging feature of XL, we'll try to restore as much as possible, regardless of consistency.
 */
public class DiagnosticsHandlerImportable extends DiagnosticsHandler implements
        IDiagnosticsHandlerImportable {

    private final Logger logger = LogManager.getLogger(getClass());

    private final Map<String, RestoreProcedure> consumers;
    private final DiagsZipReaderFactory zipReaderFactory;

    /**
     * Constructs diagnostics handler.
     *
     * @param zipReaderFactory zip reader factory to use when restoring.
     * @param diagnosables diagnostics providers. If any of them are also importable, they
     *         will be used for restoring diags
     */
    public DiagnosticsHandlerImportable(@Nonnull DiagsZipReaderFactory zipReaderFactory,
            @Nonnull Collection<? extends Diagnosable> diagnosables) {
        super(diagnosables);
        final Map<String, RestoreProcedure> consumers = new HashMap<>();
        for (Diagnosable diagnosable : diagnosables) {
            final String zipEntryName = DiagnosticsWriter.getZipEntryName(diagnosable);
            if (diagnosable instanceof DiagsRestorable) {
                final DiagsRestorable stringDiagnosable = (DiagsRestorable)diagnosable;
                consumers.put(zipEntryName,
                        diags -> restoreFromStringDiags(stringDiagnosable, diags));
            } else if (diagnosable instanceof BinaryDiagsRestorable) {
                final BinaryDiagsRestorable binaryDiagnosable = (BinaryDiagsRestorable)diagnosable;
                consumers.put(zipEntryName,
                        diags -> restoreFromBinaryDiags(binaryDiagnosable, diags));
            }
        }
        this.consumers = Collections.unmodifiableMap(consumers);
        this.zipReaderFactory = Objects.requireNonNull(zipReaderFactory);
    }

    /**
     * Restore component's state from diagnostics.
     *
     * @param inputStream diagnostics streamed bytes
     * @return list of errors if faced
     */
    @Override
    @Nonnull
    public String restore(@Nonnull final InputStream inputStream) throws DiagnosticsException {
        final List<String> errors = new ArrayList<>();
        for (final Diags diags : zipReaderFactory.createReader(inputStream)) {
            final RestoreProcedure diagnosable = consumers.get(diags.getName());
            if (diagnosable == null) {
                logger.info("Skipping file: {}", diags.getName());
            } else {
                try {
                    diagnosable.restoreDiags(diags);
                    logger.info("Import from file {} finished successfully", diags.getName());
                } catch (DiagnosticsException e) {
                    logger.error("Failed to import file " + diags.getName(), e);
                    errors.addAll(e.getErrors());
                }
            }
        }
        if (errors.isEmpty()) {
            return "Successfully restored component state";
        } else {
            throw new DiagnosticsException(errors);
        }
    }

    private void restoreFromStringDiags(@Nonnull DiagsRestorable stringDiags,
            @Nonnull Diags diagnostics) throws DiagnosticsException {
        logger.info("Importing string diagnostics from file {}", diagnostics.getName());
        if (diagnostics.getLines() == null) {
            throw new DiagnosticsException("File " + diagnostics.getName() +
                    " is expected to contain string data but does not");
        }
        stringDiags.restoreDiags(diagnostics.getLines());
    }

    private void restoreFromBinaryDiags(@Nonnull BinaryDiagsRestorable binaryDiags,
            @Nonnull Diags diagnostics) throws DiagnosticsException {
        logger.info("Importing binary diagnostics from file {}", diagnostics.getName());
        if (diagnostics.getBytes() == null) {
            throw new DiagnosticsException("File " + diagnostics.getName() +
                    " is expected to contain binary data but does not");
        }
        binaryDiags.restoreDiags(diagnostics.getBytes());
    }

    /**
     * Procedure to restore data from diagnostics.
     */
    private interface RestoreProcedure {
        /**
         * Restore data from diagnostics.
         *
         * @param diagnostics diagnostics data to import
         * @throws DiagnosticsException on some diagnostics import exceptions occurred
         */
        void restoreDiags(@Nonnull Diags diagnostics) throws DiagnosticsException;
    }
}