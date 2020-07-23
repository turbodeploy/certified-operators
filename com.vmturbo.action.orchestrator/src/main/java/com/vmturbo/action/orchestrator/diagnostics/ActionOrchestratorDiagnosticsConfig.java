package com.vmturbo.action.orchestrator.diagnostics;

import java.util.Arrays;

import io.prometheus.client.CollectorRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.store.ActionStoreConfig;
import com.vmturbo.action.orchestrator.workflow.config.WorkflowConfig;
import com.vmturbo.components.common.diagnostics.DiagnosticsControllerImportable;
import com.vmturbo.components.common.diagnostics.DiagnosticsHandlerImportable;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory.DefaultDiagsZipReader;
import com.vmturbo.components.common.diagnostics.PrometheusDiagnosticsProvider;

/**
 * The Diagnostics package deals with dumping and restoring
 * the internal state (and auxiliary information) of the Action Orchestrator.
 * This state, in association with logs and other information,
 * can be used to debug deployments in local environments.
 */
@Configuration
@Import({ActionStoreConfig.class, WorkflowConfig.class})
public class ActionOrchestratorDiagnosticsConfig {

    @Autowired
    private ActionStoreConfig storeConfig;
    @Autowired
    private WorkflowConfig workflowConfig;

    @Bean
    public ActionOrchestratorDiagnostics diagnostics() {
        return new ActionOrchestratorDiagnostics(storeConfig.actionStorehouse(),
                storeConfig.actionModeCalculator());
    }

    /**
     * Zip reader factory.
     *
     * @return zip reader factory
     */
    @Bean
    public DiagsZipReaderFactory diagsZipReaderFactory() {
        return new DefaultDiagsZipReader();
    }

    @Bean
    public DiagnosticsHandlerImportable diagnosticsHandler() {
        return new DiagnosticsHandlerImportable(diagsZipReaderFactory(),
                Arrays.asList(
                        diagnostics(),
                        workflowConfig.workflowPersistentIdentityStore(),
                        workflowConfig.workflowDiagnostics(),
                        new PrometheusDiagnosticsProvider(CollectorRegistry.defaultRegistry)));
    }

    @Bean
    public DiagnosticsControllerImportable diagnosticsController() {
        return new DiagnosticsControllerImportable(diagnosticsHandler());
    }
}
