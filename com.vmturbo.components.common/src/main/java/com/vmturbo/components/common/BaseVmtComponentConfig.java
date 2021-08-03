package com.vmturbo.components.common;

import static com.vmturbo.components.common.ConsulRegistrationConfig.ENABLE_CONSUL_MIGRATION;

import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.vmturbo.common.api.crypto.CryptoFacility;
import com.vmturbo.common.protobuf.logging.LoggingREST.LogConfigurationServiceController;
import com.vmturbo.common.protobuf.logging.LoggingREST.TracingConfigurationServiceController;
import com.vmturbo.common.protobuf.logging.MemoryMetricsREST.HeapDumpServiceController;
import com.vmturbo.common.protobuf.logging.MemoryMetricsREST.MemoryMetricsServiceController;
import com.vmturbo.common.protobuf.memory.HeapDumper;
import com.vmturbo.components.common.config.SpringConfigSource;
import com.vmturbo.components.common.diagnostics.DiagnosticService;
import com.vmturbo.components.common.diagnostics.FileFolderZipper;
import com.vmturbo.components.common.health.DeadlockHealthMonitor;
import com.vmturbo.components.common.health.MemoryMonitor;
import com.vmturbo.components.common.logging.HeapDumpRpcService;
import com.vmturbo.components.common.logging.LogConfigurationService;
import com.vmturbo.components.common.logging.MemoryMetricsRpcService;
import com.vmturbo.components.common.logging.TracingConfigurationRpcService;
import com.vmturbo.components.common.metrics.ComponentLifespanMetrics;
import com.vmturbo.components.common.migration.MigrationController;
import com.vmturbo.components.common.migration.MigrationFramework;
import com.vmturbo.components.common.tracing.TracingManager;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.KeyValueStoreConfig;

/**
 * Create Spring Beans provided by com.vmturbo.components.common.
 **/
@Configuration
@Import({BaseVmtComponentConfig.DebugSwaggerConfig.class, KeyValueStoreConfig.class,
        ConsulRegistrationConfig.class})
public class BaseVmtComponentConfig {

    private static final String REMOVE_HEAP_DUMP_SERVICE_PLACEHOLDER = "${removeHeapDumpService:false}";

    @Autowired
    private Environment environment;

    @Value("${deadlockCheckIntervalSecs:900}")
    private int deadlockCheckIntervalSecs;

    @Value("${enableMemoryMonitor:true}")
    private boolean enableMemoryMonitor;

    @Value(REMOVE_HEAP_DUMP_SERVICE_PLACEHOLDER)
    private boolean removeHeapDumpService;

    @Value("${enableHeapDumping:false}")
    private boolean enableHeapDumping;

    private static final Logger logger = LogManager.getLogger();

    /**
     * This property is used to disable consul migration. This is necessary for tests and
     * for components running outside the primary Turbonomic K8s cluster.
     */
    @Value("${" + ENABLE_CONSUL_MIGRATION + ":true}")
    private Boolean enableConsulMigration;

    /**
     * The name of the feature flag controlling whether externally-supplied secrets are used.
     */
    public static final String ENABLE_EXTERNAL_SECRETS_FLAG = "enableExternalSecrets";

    /**
     * If true, use Kubernetes secrets to read in the sensitive Auth data (like encryption keys and
     * private/public key pairs). If false, this data will be read from (legacy) persistent volumes.
     *
     * <p>Note: This feature flag is exposed in a static way to avoid having to refactor the
     * many static methods that already exist in {@link CryptoFacility}. This is expected to be a
     * short-lived situation, until enabling external secrets becomes the default.</p>
     */
    @Value("${" + ENABLE_EXTERNAL_SECRETS_FLAG + ":false}")
    public void setEnableExternalSecretsStatic(boolean enableExternalSecrets){
        CryptoFacility.enableExternalSecrets = enableExternalSecrets;
    }

    /**
     * Required to fill @{...} @Value annotations referencing
     * properties from the diagnostic.properties.
     *
     * <p>See:
     * https://docs.spring.io/spring/docs/4.2.4.RELEASE/javadoc-api/org/springframework/context/annotation/PropertySource.html
     *
     * @return The configurer.
     */
    @Bean
    public static PropertySourcesPlaceholderConfigurer configurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    @Bean
    public SpringConfigSource configSource() {
        return new SpringConfigSource(environment);
    }

    @Bean
    public DiagnosticService diagnosticService() {
        return new DiagnosticService();
    }

    @Bean
    public FileFolderZipper fileFolderZipper() {
        return new FileFolderZipper();
    }

    @Bean
    public OsCommandProcessRunner osCommandProcessRunner() {
        return new OsCommandProcessRunner();
    }

    @Bean
    public OsProcessFactory scriptProcessFactory() {
        return new OsProcessFactory();
    }

    @Bean
    public ComponentController componentController() {
        return new ComponentController();
    }

    @Bean
    public ComponentLifespanMetrics componentLifespanMetrics() {
        return ComponentLifespanMetrics.getInstance();
    }

    @Bean
    public DeadlockHealthMonitor deadlockHealthMonitor() {
        return new DeadlockHealthMonitor(deadlockCheckIntervalSecs);
    }

    @Bean
    public MemoryMonitor memoryMonitor() {
        // creates a memory monitor that reports unhealthy when old gen seems to be full
        return enableMemoryMonitor ? new MemoryMonitor(configSource()) : null;
    }

    @Bean
    public KeyValueStoreConfig keyValueStoreConfig() {
        return new KeyValueStoreConfig();
    }

    @Bean
    public KeyValueStore keyValueStore() {
        return keyValueStoreConfig().keyValueStore();
    }

    @Bean
    public Optional<MigrationFramework> migrationFramework() {
        return Optional.ofNullable(enableConsulMigration ?
                new MigrationFramework(keyValueStore()) : null);
    }

    @Bean
    public MigrationController migrationController() {
        return new MigrationController(migrationFramework().orElse(null));
    }

    @Bean
    public LogConfigurationService logConfigurationService() {
        return new LogConfigurationService();
    }

    @Bean
    public TracingConfigurationRpcService tracingConfigurationRpcService() {
        return new TracingConfigurationRpcService(TracingManager.get());
    }

    @Bean
    public HeapDumper heapDumper() {
        return new HeapDumper();
    }

    @Bean
    public MemoryMetricsRpcService memoryMetricsRpcService() {
        return new MemoryMetricsRpcService();
    }

    @Bean
    public Optional<HeapDumpRpcService> heapDumpRpcService() {
        if (removeHeapDumpService) {
            return Optional.empty();
        } else {
            return Optional.of(new HeapDumpRpcService(heapDumper()));
        }
    }

    @Bean
    public boolean heapDumpEnabled() {
        return enableHeapDumping;
    }

    @Bean
    public TracingConfigurationServiceController tracingConfigurationServiceController() {
        return new TracingConfigurationServiceController(tracingConfigurationRpcService());
    }

    @Bean
    public LogConfigurationServiceController logConfigurationServiceController() {
        return new LogConfigurationServiceController(logConfigurationService());
    }

    @Bean
    public MemoryMetricsServiceController memoryMetricsServiceController() {
        return new MemoryMetricsServiceController(memoryMetricsRpcService());
    }

    @Bean
    @Conditional(ShouldRetainHeapDumpService.class)
    public HeapDumpServiceController heapDumpServiceController() {
        return heapDumpRpcService()
            .map(HeapDumpServiceController::new)
            .orElse(null);
    }

    /**
     * A logging filter to log HTTP requests.
     * This makes it easier to debug failing calls.
     *
     * @return The filter.
     */
    @Bean
    public LoggingFilter loggingFilter() {
        return new LoggingFilter();
    }

    @Configuration
    public static class DebugSwaggerConfig extends WebMvcConfigurerAdapter {

        /**
         * Add a Resource entry for the Swagger-UI to be served from the "/swagger" folder in the
         * component container.
         *
         * @param registry - the Spring {@linkplain ResourceHandlerRegistry} for this child
         *                 Spring Context for the Turbonomic XL component
         */
        @Override
        public void addResourceHandlers(ResourceHandlerRegistry registry) {
            // resources for the internal debug swagger UI for a component
            ResourceHandlerRegistration reg = registry.addResourceHandler("/swagger/**")
                    .addResourceLocations("file:/swagger/");
        }
    }

    /**
     * Helper condition to decide whether or not to add the heap dump service.
     * An easier way to resolve this is using @ConditionalOnProperty but that would require including
     * spring boot as a dependency which would increase component and binary size.
     */
    public static class ShouldRetainHeapDumpService implements Condition {
        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            final boolean removeHeapDumpService = Boolean.parseBoolean(
                context.getEnvironment().resolvePlaceholders(REMOVE_HEAP_DUMP_SERVICE_PLACEHOLDER));
            return !removeHeapDumpService;
        }
    }
}
