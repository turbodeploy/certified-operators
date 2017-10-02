package com.vmturbo.action.orchestrator.diagnostics;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.store.ActionStoreConfig;
import com.vmturbo.components.common.DiagnosticsWriter;

/**
 * The Diagnostics package deals with dumping and restoring
 * the internal state (and auxiliary information) of the Action Orchestrator.
 * This state, in association with logs and other information,
 * can be used to debug deployments in local environments.
 */
@Configuration
@Import({ActionStoreConfig.class})
public class ActionOrchestratorDiagnosticsConfig {

    @Autowired
    private ActionStoreConfig storeConfig;

    @Bean
    public ActionOrchestratorDiagnostics diagnostics() {
        return new ActionOrchestratorDiagnostics(storeConfig.actionStorehouse(),
                storeConfig.actionFactory(), diagnosticsWriter());
    }

    @Bean
    public DiagnosticsWriter diagnosticsWriter() {
        return new DiagnosticsWriter();
    }
}
