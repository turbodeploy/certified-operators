package com.vmturbo.plan.orchestrator.diagnostics;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.diagnostics.DiagnosticsControllerImportable;
import com.vmturbo.components.common.diagnostics.DiagnosticsHandlerImportable;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory.DefaultDiagsZipReader;
import com.vmturbo.plan.orchestrator.PlanOrchestratorDBConfig;
import com.vmturbo.plan.orchestrator.deployment.profile.DeploymentProfileConfig;
import com.vmturbo.plan.orchestrator.plan.PlanConfig;
import com.vmturbo.plan.orchestrator.project.PlanProjectConfig;
import com.vmturbo.plan.orchestrator.scenario.ScenarioConfig;
import com.vmturbo.plan.orchestrator.templates.TemplatesConfig;

/**
 * Configuration from plan orchestrator component diagnostics
 */
@Configuration
@Import({TemplatesConfig.class, PlanConfig.class, PlanProjectConfig.class, PlanOrchestratorDBConfig.class,
    ScenarioConfig.class, DeploymentProfileConfig.class})
public class PlanOrchestratorDiagnosticsConfig {

    @Autowired
    private TemplatesConfig templatesConfig;

    @Autowired
    private PlanConfig planConfig;

    @Autowired
    private PlanProjectConfig planProjectConfig;

    @Autowired
    private ScenarioConfig scenarioConfig;

    @Autowired
    private DeploymentProfileConfig deploymentProfileConfig;

    @Autowired
    private PlanOrchestratorDBConfig planOrchestratorDBConfig;

    @Bean
    public DiagsZipReaderFactory recursiveZipReaderFactory() {
        return new DefaultDiagsZipReader();
    }

    @Bean
    public DiagnosticsHandlerImportable diagnosticsHandler() {
        return new DiagnosticsHandlerImportable(recursiveZipReaderFactory(),
                Arrays.asList(planConfig.planDao(), planProjectConfig.planProjectDao(),
                        planOrchestratorDBConfig.reservationDao(), scenarioConfig.scenarioDao(),
                        templatesConfig.templatesDao(), templatesConfig.templateSpecParser(),
                        deploymentProfileConfig.deploymentProfileDao()));

    }

    @Bean
    public DiagnosticsControllerImportable diagnosticsController() {
        return new DiagnosticsControllerImportable(diagnosticsHandler());
    }

}
