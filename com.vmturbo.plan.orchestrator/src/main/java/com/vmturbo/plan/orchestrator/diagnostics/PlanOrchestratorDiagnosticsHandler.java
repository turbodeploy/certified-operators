package com.vmturbo.plan.orchestrator.diagnostics;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableBiMap;

import com.vmturbo.components.common.DiagnosticsWriter;
import com.vmturbo.components.common.diagnostics.Diagnosable;
import com.vmturbo.components.common.diagnostics.Diagnosable.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.Diags;
import com.vmturbo.components.common.diagnostics.RecursiveZipReaderFactory;
import com.vmturbo.plan.orchestrator.deployment.profile.DeploymentProfileDaoImpl;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.project.PlanProjectDao;
import com.vmturbo.plan.orchestrator.reservation.ReservationDao;
import com.vmturbo.plan.orchestrator.scenario.ScenarioDao;
import com.vmturbo.plan.orchestrator.templates.TemplateSpecParser;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;

public class PlanOrchestratorDiagnosticsHandler {

    private final DiagnosticsWriter diagnosticsWriter;

    private final RecursiveZipReaderFactory zipReaderFactory;

    private static final String DEPLOYMENT_PROFILES = "DeploymentProfiles.diags";
    private static final String PLAN_INSTANCES = "PlanInstances.diags";
    private static final String PLAN_PROJECTS = "PlanProjects.diags";
    private static final String RESERVATIONS = "Reservations.diags";
    private static final String SCENARIOS = "Scenarios.diags";
    private static final String TEMPLATES = "Templates.diags";
    private static final String TEMPLATE_SPECS = "TemplateSpecs.diags";

    @VisibleForTesting
    static final String ERRORS_FILE = "DiagnosticDumpErrors";

    @VisibleForTesting
    final ImmutableBiMap<String, Diagnosable> filenameToDiagnosableMap;

    public PlanOrchestratorDiagnosticsHandler(@Nonnull final PlanDao planInstanceDao,
                                              @Nonnull final PlanProjectDao planProjectDao,
                                              @Nonnull final ReservationDao reservationDao,
                                              @Nonnull final ScenarioDao scenarioDao,
                                              @Nonnull final TemplatesDao templateDao,
                                              @Nonnull final TemplateSpecParser templateSpecParser,
                                              @Nonnull final DeploymentProfileDaoImpl deploymentProfileDao,
                                              @Nonnull final RecursiveZipReaderFactory readerFactory,
                                              @Nonnull final DiagnosticsWriter diagnosticsWriter) {

        this.filenameToDiagnosableMap = ImmutableBiMap.<String, Diagnosable>builder()
            .put(DEPLOYMENT_PROFILES, Objects.requireNonNull(deploymentProfileDao))
            .put(PLAN_INSTANCES, Objects.requireNonNull(planInstanceDao))
            .put(PLAN_PROJECTS, Objects.requireNonNull(planProjectDao))
            .put(RESERVATIONS, Objects.requireNonNull(reservationDao))
            .put(SCENARIOS, Objects.requireNonNull(scenarioDao))
            .put(TEMPLATE_SPECS, Objects.requireNonNull(templateSpecParser))
            .put(TEMPLATES, Objects.requireNonNull(templateDao))
            .build();

        this.diagnosticsWriter = Objects.requireNonNull(diagnosticsWriter);
        this.zipReaderFactory = Objects.requireNonNull(readerFactory);
    }

    /**
     * Dumps the plan orchestrator component state to a {@link ZipOutputStream}.
     *
     * @param diagnosticZip the output stream to receive the dump
     * @return a list of strings representing any errors that may have occurred
     */
    public List<String> dump(@Nonnull final ZipOutputStream diagnosticZip) {
        final List<String> errors = new ArrayList<>();

        filenameToDiagnosableMap.forEach((filename, diagnosable) ->
            errors.addAll(writeDiags(filename, diagnosable, diagnosticZip))
        );

        if (!errors.isEmpty()) {
            diagnosticsWriter.writeZipEntry(ERRORS_FILE, errors, diagnosticZip);
        }

        return errors;
    }

    /**
     * Write diagnostics from one diagnosable to a certain file in an output stream
     * @param filename the file to write to
     * @param diagnosable diagnosable to collect diagnostics from
     * @param diagnosticZip the output stream to write to
     * @return a list of strings representing any errors that may have occurred
     */
    private List<String> writeDiags(@Nonnull final String filename,
                                    @Nonnull final Diagnosable diagnosable,
                                    @Nonnull final ZipOutputStream diagnosticZip) {
        try {
            diagnosticsWriter.writeZipEntry(filename, diagnosable.collectDiags(), diagnosticZip);
        } catch (DiagnosticsException e) {
            return e.getErrors();
        }
        return Collections.emptyList();
    }

    /**
     * Restores the plan orchestrator component state from an {@link InputStream}
     *
     * @param inputStream the input stream to restore diagnostics from
     * @return a list of strings representing any errors that may have occurred
     */
    public List<String> restore(@Nonnull final InputStream inputStream) {
        final List<String> errors = new ArrayList<>();
        for (Diags diags : zipReaderFactory.createReader(inputStream)) {
            errors.addAll(restoreFromDiags(diags));
        }
        return errors;
    }

    /**
     * Restore diagnostics to the diagnosable that matches the diagnostics file name
     *
     * @param diags the diagnostics to restore including file name
     * @return a list of strings representing any errors that may have occurred
     */
    private List<String> restoreFromDiags(@Nonnull final Diags diags) {
        final Diagnosable diagnosable = filenameToDiagnosableMap.get(diags.getName());
        if (diags.getLines() == null || diagnosable == null) {
            return Collections.singletonList("Unable to restore diags file " + diags.getName());
        }
        try {
            diagnosable.restoreDiags(diags.getLines());
        } catch (DiagnosticsException e) {
            return e.getErrors();
        }
        return Collections.emptyList();
    }

}
