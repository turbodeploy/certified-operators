package com.vmturbo.group.diagnostics;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.components.common.DiagnosticsWriter;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.Diagnosable.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.Diags;
import com.vmturbo.components.common.diagnostics.RecursiveZipReader;
import com.vmturbo.components.common.diagnostics.RecursiveZipReaderFactory;
import com.vmturbo.group.persistent.GroupStore;
import com.vmturbo.group.persistent.PolicyStore;
import com.vmturbo.group.persistent.SettingStore;

public class GroupDiagnosticsHandler {

    private final Logger logger = LogManager.getLogger();

    /**
     * The file name for the groups dump collected from the {@link GroupStore}.
     * It's a string file, so the "diags" extension is required for compatibility
     * with {@link RecursiveZipReader}.
     */
    @VisibleForTesting
    static final String GROUPS_DUMP_FILE = "groups_dump.diags";

    /**
     * The file name for the policies dump collected from the {@link PolicyStore}.
     * It's a string file, so the "diags" extension is required for compatibility
     * with {@link RecursiveZipReader}.
     */
    @VisibleForTesting
    static final String POLICIES_DUMP_FILE = "policies_dump.diags";

    /**
     * The file name for the settings dump collected from the {@link SettingStore}.
     * It's a string file, so the "diags" extension is required for compatibility
     * with {@link RecursiveZipReader}.
     */
    @VisibleForTesting
    static final String SETTINGS_DUMP_FILE = "settings_dump.diags";

    @VisibleForTesting
    static final String ERRORS_FILE = "dump_errors";

    private final GroupStore groupStore;

    private final PolicyStore policyStore;

    private final SettingStore settingStore;

    private final DiagnosticsWriter diagnosticsWriter;

    private final RecursiveZipReaderFactory zipReaderFactory;

    public GroupDiagnosticsHandler(@Nonnull final GroupStore groupStore,
                                   @Nonnull final PolicyStore policyStore,
                                   @Nonnull final SettingStore settingStore,
                                   @Nonnull final RecursiveZipReaderFactory zipReaderFactory,
                                   @Nonnull final DiagnosticsWriter diagnosticsWriter) {
        this.groupStore = Objects.requireNonNull(groupStore);
        this.policyStore = Objects.requireNonNull(policyStore);
        this.settingStore = Objects.requireNonNull(settingStore);
        this.zipReaderFactory = Objects.requireNonNull(zipReaderFactory);
        this.diagnosticsWriter = Objects.requireNonNull(diagnosticsWriter);
    }

    /**
     * Dumps the group component state to a {@link ZipOutputStream}.
     *
     * @param diagnosticZip The destination.
     * @return The list of errors encountered, or an empty list if successful.
     */
    public List<String> dump(@Nonnull final ZipOutputStream diagnosticZip) {
        final List<String> errors = new ArrayList<>();

        // Groups
        try {
            diagnosticsWriter.writeZipEntry(GROUPS_DUMP_FILE, groupStore.collectDiags(), diagnosticZip);
        } catch (DiagnosticsException e) {
            errors.addAll(e.getErrors());
        }

        // Policies
        try {
            diagnosticsWriter.writeZipEntry(POLICIES_DUMP_FILE, policyStore.collectDiags(), diagnosticZip);
        } catch (DiagnosticsException e) {
            errors.addAll(e.getErrors());
        }

        // Settings
        try {
            diagnosticsWriter.writeZipEntry(SETTINGS_DUMP_FILE, settingStore.collectDiags(), diagnosticZip);
        } catch (DiagnosticsException e) {
            errors.addAll(e.getErrors());
        }

        if (!errors.isEmpty()) {
            diagnosticsWriter.writeZipEntry(ERRORS_FILE, errors, diagnosticZip);
        }

        return errors;
    }

    public List<String> restore(@Nonnull final InputStream inputStream) {
        final List<String> errors = new ArrayList<>();

        for (Diags diags : zipReaderFactory.createReader(inputStream)) {
            switch (diags.getName()) {
                case GROUPS_DUMP_FILE:
                    errors.addAll(performRestore(diags, groupStore));
                    break;
                case POLICIES_DUMP_FILE:
                    errors.addAll(performRestore(diags, policyStore));
                    break;
                case SETTINGS_DUMP_FILE:
                    errors.addAll(performRestore(diags, settingStore));
                    break;
                default:
                    logger.warn("Skipping file: {}", diags.getName());
                    break;
            }
        }

        return errors;
    }

    @Nonnull
    public List<String> performRestore(@Nonnull final Diags diags,
                                       @Nonnull final Diagnosable diagnosable) {
        if (diags.getLines() == null) {
            return Collections.singletonList("The file " + diags.getName() + " was not saved as lines " +
                "of strings with the appropriate suffix!");
        } else {
            try {
                diagnosable.restoreDiags(diags.getLines());
                logger.info("Restored {} ", diags.getName());
            } catch (DiagnosticsException e) {
                return e.getErrors();
            }
        }

        return Collections.emptyList();
    }
}
