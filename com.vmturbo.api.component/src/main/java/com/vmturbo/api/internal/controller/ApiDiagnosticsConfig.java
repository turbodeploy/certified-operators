package com.vmturbo.api.internal.controller;

import com.google.common.collect.Lists;

import io.prometheus.client.CollectorRegistry;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.api.component.communication.CommunicationConfig;
import com.vmturbo.api.component.external.api.service.ServiceConfig;
import com.vmturbo.components.common.diagnostics.DiagnosticsController;
import com.vmturbo.components.common.diagnostics.DiagnosticsHandler;
import com.vmturbo.components.common.diagnostics.PrometheusDiagnosticsProvider;

/**
 * Configuration for group component diagnostics.
 */
@Configuration
@Import({CommunicationConfig.class, ServiceConfig.class})
public class ApiDiagnosticsConfig {

    @Autowired
    private CommunicationConfig communicationConfig;

    @Autowired
    private ServiceConfig serviceConfig;

    @Bean
    public PrometheusDiagnosticsProvider prometheusDiagnisticsProvider() {
        return new PrometheusDiagnosticsProvider(CollectorRegistry.defaultRegistry);
    }

    /**
     * Diagnostics handler.
     *
     * @return diagnostics handler
     */
    @Bean
    public DiagnosticsHandler diagsHandler() {
        return new DiagnosticsHandler(
                Lists.newArrayList(versionDiags(), prometheusDiagnisticsProvider()));
    }

    @Bean
    public VersionDiagnosable versionDiags() {
        return new VersionDiagnosable(communicationConfig.supplyChainFetcher(),
                serviceConfig.adminService(), communicationConfig.clusterMgr(),
                communicationConfig.getRealtimeTopologyContextId());
    }

    @Bean
    public DiagnosticsController diagnosticsController() {
        return new DiagnosticsController(diagsHandler());
    }
}
