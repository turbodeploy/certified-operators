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
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Diagnostics handler for importable diags. It extends {@link DiagnosticsHandler} with a
 * 'restore from diags' function.
 *
 * <p>If some of the restorable diagnosable failed to load its state from the associated file,
 * other diagnosable (and other files) will be tried nevertheless. As this is mostly a
 * debugging feature of XL, we'll try to restore as much as possible, regardless of consistency.
 *
 * @param <T> the type of context object.
 */
public class DiagnosticsHandlerImportable<T> extends DiagnosticsHandler implements
        IDiagnosticsHandlerImportable<T> {

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
                    (diags, context) -> restoreFromStringDiags(stringDiagnosable, diags,
                        (T)context));
            } else if (diagnosable instanceof BinaryDiagsRestorable) {
                final BinaryDiagsRestorable binaryDiagnosable = (BinaryDiagsRestorable)diagnosable;
                consumers.put(zipEntryName,
                    (diags, context) -> restoreFromBinaryDiags(binaryDiagnosable, diags,
                        (T)context));
            }
        }
        this.consumers = Collections.unmodifiableMap(consumers);
        this.zipReaderFactory = Objects.requireNonNull(zipReaderFactory);
    }

    @Nonnull
    @Override
    public String restore(@Nonnull InputStream inputStream, @Nonnull T context) throws DiagnosticsException {
        final List<String> errors = new ArrayList<>();
        for (final Diags diags : zipReaderFactory.createReader(inputStream)) {
            final RestoreProcedure diagnosable = consumers.get(diags.getName());
            if (diagnosable == null) {
                logger.info("Skipping file: {}", diags.getName());
            } else {
                try {
                    diagnosable.restoreDiags(diags, context);
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
            @Nonnull Diags diagnostics, @Nullable T context) throws DiagnosticsException {
        logger.info("Importing string diagnostics from file {}", diagnostics.getName());
        if (diagnostics.getLines() == null) {
            throw new DiagnosticsException("File " + diagnostics.getName()
                    + " is expected to contain string data but does not");
        }
        stringDiags.restoreDiags(diagnostics.getLines(), context);
    }

    private void restoreFromBinaryDiags(@Nonnull BinaryDiagsRestorable binaryDiags,
            @Nonnull Diags diagnostics, @Nullable T context) throws DiagnosticsException {
        logger.info("Importing binary diagnostics from file {}", diagnostics.getName());
        if (diagnostics.getBytes() == null) {
            throw new DiagnosticsException("File " + diagnostics.getName()
                    + " is expected to contain binary data but does not");
        }
        binaryDiags.restoreDiags(diagnostics.getBytes(), context);
    }

    /**
     * Procedure to restore data from diagnostics.
     *
     * @param <T> type of context object
     */
    private interface RestoreProcedure<T> {
        /**
         * Restore data from diagnostics.
         *
         * @param diagnostics diagnostics data to import
         * @param context the context object
         * @throws DiagnosticsException on some diagnostics import exceptions occurred
         */
        void restoreDiags(@Nonnull Diags diagnostics, @Nullable T context) throws DiagnosticsException;
    }
}