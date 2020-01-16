package com.vmturbo.plan.orchestrator.diagnostics;

import com.vmturbo.plan.orchestrator.PlanOrchestratorDBConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.diagnostics.DiagnosticsWriter;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory.DefaultDiagsZipReader;
import com.vmturbo.plan.orchestrator.deployment.profile.DeploymentProfileConfig;
import com.vmturbo.plan.orchestrator.plan.PlanConfig;
import com.vmturbo.plan.orchestrator.project.PlanProjectConfig;
import com.vmturbo.plan.orchestrator.reservation.ReservationConfig;
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
    public DiagnosticsWriter diagnosticsWriter() {
        return new DiagnosticsWriter();
    }

    @Bean
    public DiagsZipReaderFactory recursiveZipReaderFactory() {
        return new DefaultDiagsZipReader();
    }

    @Bean
    public PlanOrchestratorDiagnosticsHandler diagnosticsHandler() {
        return new PlanOrchestratorDiagnosticsHandler(planConfig.planDao(),
            planProjectConfig.planProjectDao(), planOrchestratorDBConfig.reservationDao(),
            scenarioConfig.scenarioDao(), templatesConfig.templatesDao(),
            templatesConfig.templateSpecParser(), deploymentProfileConfig.deploymentProfileDao(),
            recursiveZipReaderFactory(), diagnosticsWriter());
    }

    @Bean
    public PlanOrchestratorDiagnosticsController diagnosticsController() {
        return new PlanOrchestratorDiagnosticsController(diagnosticsHandler());
    }

}
