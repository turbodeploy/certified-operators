package com.vmturbo.api.internal.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.api.component.communication.CommunicationConfig;
import com.vmturbo.api.component.external.api.service.ServiceConfig;
import com.vmturbo.components.common.DiagnosticsWriter;

/**
 * Configuration for group component diagnostics.
 */
@Configuration
@Import({
    CommunicationConfig.class,
    ServiceConfig.class
})
public class ApiDiagnosticsConfig {

    @Autowired
    private CommunicationConfig communicationConfig;

    @Autowired
    private ServiceConfig serviceConfig;

    @Bean
    public DiagnosticsWriter diagnosticsWriter() {
        return new DiagnosticsWriter();
    }

    @Bean
    public ApiDiagnosticsHandler diagsHandler() {
        return new ApiDiagnosticsHandler(communicationConfig.supplyChainFetcher(),
            serviceConfig.adminService(),
            communicationConfig.clusterMgr(),
            diagnosticsWriter(),
            communicationConfig.getRealtimeTopologyContextId());
    }

    @Bean
    public ApiDiagnosticsController diagnosticsController() {
        return new ApiDiagnosticsController(diagsHandler());
    }
}
