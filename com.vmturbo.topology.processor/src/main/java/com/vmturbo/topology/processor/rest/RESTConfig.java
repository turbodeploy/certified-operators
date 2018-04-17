package com.vmturbo.topology.processor.rest;

import java.util.List;

import com.google.gson.Gson;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.topology.processor.diagnostics.TopologyProcessorDiagnosticsConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.group.GroupConfig;
import com.vmturbo.topology.processor.operation.OperationConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.scheduling.SchedulerConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;
import com.vmturbo.topology.processor.topology.TopologyConfig;

/**
 * Configuration for the Controllers package in TopologyProcessor.
 */
@Configuration
@Import({
    EntityConfig.class,
    OperationConfig.class,
    TargetConfig.class,
    TopologyConfig.class,
    ProbeConfig.class,
    SchedulerConfig.class,
    GroupConfig.class,
    TopologyProcessorDiagnosticsConfig.class,
})
public class RESTConfig extends WebMvcConfigurerAdapter {
    @Autowired
    private EntityConfig entityConfig;

    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private TopologyConfig topologyConfig;

    @Autowired
    private OperationConfig operationConfig;

    @Autowired
    private SchedulerConfig schedulerConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private GroupConfig groupConfig;

    @Autowired
    private TopologyProcessorDiagnosticsConfig diagnosticsConfig;

    @Bean
    public TopologyController topologyController() {
        return new TopologyController(
            schedulerConfig.scheduler(),
            topologyConfig.topologyHandler(),
            entityConfig.entityStore(),
            groupConfig.policyManager());
    }

    @Bean
    public OperationController operationController() {
        return new OperationController(
            operationConfig.operationManager(),
            schedulerConfig.scheduler(),
            targetConfig.targetStore()
        );
    }

    @Bean
    public ProbeController probeController() {
        return new ProbeController(probeConfig.probeStore());
    }

    @Bean
    public TargetController targetController() {
        return new TargetController(
                schedulerConfig.scheduler(),
                targetConfig.targetStore(),
                probeConfig.probeStore(),
                operationConfig.operationManager(),
                topologyConfig.topologyHandler()
        );
    }

    @Bean
    public DiagnosticsController diagnosticsController() {
        return new DiagnosticsController(diagnosticsConfig.diagsHandler());
    }

    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        final Gson GSON = ComponentGsonFactory.createGson();
        GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
        msgConverter.setGson(GSON);
        converters.add(msgConverter);
    }
}
