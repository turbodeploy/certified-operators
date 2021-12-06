package com.vmturbo.plan.orchestrator.diagnostics;

import java.util.Arrays;

import com.vmturbo.plan.orchestrator.DbAccessConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.diagnostics.DiagnosticsControllerImportable;
import com.vmturbo.components.common.diagnostics.DiagnosticsHandlerImportable;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory.DefaultDiagsZipReader;
import com.vmturbo.plan.orchestrator.deployment.profile.DeploymentProfileConfig;
import com.vmturbo.plan.orchestrator.plan.PlanConfig;
import com.vmturbo.plan.orchestrator.plan.export.PlanExportConfig;
import com.vmturbo.plan.orchestrator.project.PlanProjectConfig;
import com.vmturbo.plan.orchestrator.reservation.ReservationConfig;
import com.vmturbo.plan.orchestrator.scenario.ScenarioConfig;
import com.vmturbo.plan.orchestrator.templates.TemplatesConfig;

/**
 * Configuration from plan orchestrator component diagnostics
 */
@Configuration
@Import({TemplatesConfig.class, PlanConfig.class, PlanExportConfig.class, PlanProjectConfig.class, DbAccessConfig.class,
    ScenarioConfig.class, DeploymentProfileConfig.class, ReservationConfig.class})
public class PlanOrchestratorDiagnosticsConfig {

    @Autowired
    private TemplatesConfig templatesConfig;

    @Autowired
    private PlanConfig planConfig;

    @Autowired
    private PlanExportConfig planExportConfig;

    @Autowired
    private PlanProjectConfig planProjectConfig;

    @Autowired
    private ScenarioConfig scenarioConfig;

    @Autowired
    private DeploymentProfileConfig deploymentProfileConfig;

    @Autowired
    private ReservationConfig reservationConfig;

    @Autowired
    private DbAccessConfig dbAccessConfig;

    @Bean
    public DiagsZipReaderFactory recursiveZipReaderFactory() {
        return new DefaultDiagsZipReader();
    }

    @Bean
    public DiagnosticsHandlerImportable diagnosticsHandler() {
        return new DiagnosticsHandlerImportable(recursiveZipReaderFactory(),
                Arrays.asList(planConfig.planDao(), planExportConfig.planDestinationDao(),
                        planProjectConfig.planProjectDao(),
                        reservationConfig.reservationDao(), scenarioConfig.scenarioDao(),
                        templatesConfig.templatesDao(), templatesConfig.templateSpecParser(),
                        deploymentProfileConfig.deploymentProfileDao()));

    }

    @Bean
    public DiagnosticsControllerImportable diagnosticsController() {
        return new DiagnosticsControllerImportable(diagnosticsHandler());
    }

}
