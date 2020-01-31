package com.vmturbo.topology.processor.rest;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;

import com.google.gson.Gson;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.ResourceHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.context.request.async.CallableProcessingInterceptor;
import org.springframework.web.context.request.async.TimeoutCallableProcessingInterceptor;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.common.diagnostics.DiagnosticsControllerImportable;
import com.vmturbo.topology.processor.ClockConfig;
import com.vmturbo.topology.processor.diagnostics.TopologyProcessorDiagnosticsConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.group.GroupConfig;
import com.vmturbo.topology.processor.operation.OperationConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.scheduling.SchedulerConfig;
import com.vmturbo.topology.processor.stitching.journal.JournalFilterFactory;
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
    ClockConfig.class,
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
    private ClockConfig clockConfig;

    @Autowired
    private TopologyProcessorDiagnosticsConfig diagnosticsConfig;

    /**
     * Maximum amount of time to wait for async REST requests. This comes into use when requesting the stitching
     * journal for very large topologies. This can take a good deal of time for very large topologies.
     */
    @Value("${asyncRestRequestTimeoutSeconds:300}")
    private long asyncRestRequestTimeoutSeconds;

    private static final Logger logger = LogManager.getLogger();

    @Bean
    public TopologyController topologyController() {
        return new TopologyController(
            schedulerConfig.scheduler(),
            topologyConfig.topologyHandler(),
            entityConfig.entityStore(),
            clockConfig.clock());
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
    public DiagnosticsControllerImportable diagnosticsController() {
        return new DiagnosticsControllerImportable(diagnosticsConfig.diagsHandler());
    }

    @Bean
    public JournalFilterFactory journalFilterFactory() {
        return new JournalFilterFactory(probeConfig.probeStore(), targetConfig.targetStore());
    }

    @Bean
    public StitchingJournalController stitchingController() {
        return new StitchingJournalController(
            topologyConfig.topologyHandler(),
            schedulerConfig.scheduler(),
            journalFilterFactory(),
            clockConfig.clock());
    }

    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        final Gson GSON = ComponentGsonFactory.createGson();
        GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
        msgConverter.setGson(GSON);

        // Note that adding converters to the list, turns off default converter registration.
        // When using extendMessageConverters instead, we get default converters that we don't want.
        // So instead re-add many of the defaults manually.
        converters.add(msgConverter);
        converters.add(new ByteArrayHttpMessageConverter());
        converters.add(new StringHttpMessageConverter());
        converters.add(new ResourceHttpMessageConverter());
    }

    @Override
    public void configureAsyncSupport (AsyncSupportConfigurer configurer) {
        configurer.setDefaultTimeout(Duration.ofSeconds(asyncRestRequestTimeoutSeconds).toMillis());
        super.configureAsyncSupport(configurer);
    }

    @Bean
    public CallableProcessingInterceptor callableProcessingInterceptor() {
        return new TimeoutCallableProcessingInterceptor() {
            @Override
            public <T> Object handleTimeout(NativeWebRequest request, Callable<T> task) throws Exception {
                logger.error("Async request to timed out.");
                return super.handleTimeout(request, task);
            }
        };
    }
}
