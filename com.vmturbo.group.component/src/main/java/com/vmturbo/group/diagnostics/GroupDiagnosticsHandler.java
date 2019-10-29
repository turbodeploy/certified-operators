package com.vmturbo.group.diagnostics;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import io.prometheus.client.CollectorRegistry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.common.DiagnosticsWriter;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.Diags;
import com.vmturbo.components.common.diagnostics.DiagsZipReader;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory;
import com.vmturbo.group.policy.PolicyStore;
import com.vmturbo.group.setting.SettingStore;

public class GroupDiagnosticsHandler {

    private final Logger logger = LogManager.getLogger();

    /**
     * The file name for the groups dump collected from the {@link com.vmturbo.group.group.IGroupStore}.
     * It's a string file, so the "diags" extension is required for compatibility
     * with {@link DiagsZipReader}.
     */
    @VisibleForTesting
    static final String GROUPS_DUMP_FILE = "groups_dump.diags";

    /**
     * The file name for the policies dump collected from the {@link PolicyStore}.
     * It's a string file, so the "diags" extension is required for compatibility
     * with {@link DiagsZipReader}.
     */
    @VisibleForTesting
    static final String POLICIES_DUMP_FILE = "policies_dump.diags";

    /**
     * The file name for the settings dump collected from the {@link SettingStore}.
     * It's a string file, so the "diags" extension is required for compatibility
     * with {@link DiagsZipReader}.
     */
    @VisibleForTesting
    static final String SETTINGS_DUMP_FILE = "settings_dump.diags";

    @VisibleForTesting
    static final String ERRORS_FILE = "dump_errors";

    private final Diagnosable groupStore;

    private final PolicyStore policyStore;

    private final SettingStore settingStore;

    private final DiagnosticsWriter diagnosticsWriter;

    private final DiagsZipReaderFactory zipReaderFactory;

    /**
     * Constructs diagnostics handler of group component from various stores.
     *
     * @param groupStore group store
     * @param policyStore policy store
     * @param settingStore setting store
     * @param zipReaderFactory zip reader factory
     * @param diagnosticsWriter writer
     */
    public GroupDiagnosticsHandler(@Nonnull final Diagnosable groupStore,
                                   @Nonnull final PolicyStore policyStore,
                                   @Nonnull final SettingStore settingStore,
                                   @Nonnull final DiagsZipReaderFactory zipReaderFactory,
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

        try {
            diagnosticsWriter.writePrometheusMetrics(CollectorRegistry.defaultRegistry, diagnosticZip);
        } catch (DiagnosticsException e) {
            errors.addAll(e.getErrors());
        }

        if (!errors.isEmpty()) {
            try {
                diagnosticsWriter.writeZipEntry(ERRORS_FILE, errors, diagnosticZip);
            } catch (DiagnosticsException e) {
                logger.error("Error writing {}: {}", ERRORS_FILE, errors);
            }
        }

        return errors;
    }

    public List<String> restore(@Nonnull final InputStream inputStream) {
        final List<String> errors = new ArrayList<>();

        final Map<String, RestoreFn> restoreFnsByName = new HashMap<>();

        for (final Diags diags : zipReaderFactory.createReader(inputStream)) {
            switch (diags.getName()) {
                case GROUPS_DUMP_FILE:
                    restoreFnsByName.put(GROUPS_DUMP_FILE, () -> performRestore(diags, groupStore));
                    break;
                case POLICIES_DUMP_FILE:
                    restoreFnsByName.put(POLICIES_DUMP_FILE, () -> performRestore(diags, policyStore));
                    break;
                case SETTINGS_DUMP_FILE:
                    restoreFnsByName.put(SETTINGS_DUMP_FILE, () -> performRestore(diags, settingStore));
                    break;
                default:
                    logger.warn("Skipping file: {}", diags.getName());
                    break;
            }
        }

        final RestoreFn groupRestoreFn = restoreFnsByName.get(GROUPS_DUMP_FILE);
        if (groupRestoreFn == null) {
            String errorMsgs = "Groups dump " + GROUPS_DUMP_FILE + " not found in diags." +
                " Not restoring anything, because setting policies and policies reference groups.";
            logger.error(errorMsgs);
            errors.add(errorMsgs);
        } else {
            errors.addAll(groupRestoreFn.restore());
            Optional.ofNullable(restoreFnsByName.get(POLICIES_DUMP_FILE))
                .ifPresent(policiesRestoreFn -> errors.addAll(policiesRestoreFn.restore()));
            Optional.ofNullable(restoreFnsByName.get(SETTINGS_DUMP_FILE))
                    .ifPresent(settingsRestoreFn -> errors.addAll(settingsRestoreFn.restore()));
        }

        return errors;
    }

    /**
     * A function to restore diagnostics to a class in the group component.
     * We use it to order the restoration of related classes.
     */
    @FunctionalInterface
    private interface RestoreFn {
        List<String> restore();
    }

    @Nonnull
    private List<String> performRestore(@Nonnull final Diags diags,
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
