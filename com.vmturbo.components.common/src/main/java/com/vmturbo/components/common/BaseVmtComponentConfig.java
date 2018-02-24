package com.vmturbo.components.common;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.vmturbo.components.common.health.DeadlockHealthMonitor;
import com.vmturbo.components.common.health.MemoryMonitor;
import com.vmturbo.components.common.metrics.ComponentLifespanMetrics;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.KeyValueStoreConfig;

/**
 * Create Spring Beans provided by com.vmturbo.components.common.
 **/
@Configuration
@Import({BaseVmtComponentConfig.DebugSwaggerConfig.class, KeyValueStoreConfig.class})
public class BaseVmtComponentConfig {

    @Value("${deadlockCheckIntervalSecs:900}")
    private int deadlockCheckIntervalSecs;


    @Value("${maxHealthyUsedMemoryRatio:0.95}")
    private double maxHealthyUsedMemoryRatio;


    /**
     * Required to fill @{...} @Value annotations referencing
     * properties from the diagnostic.properties.
     *
     * See:
     * https://docs.spring.io/spring/docs/4.2.4.RELEASE/javadoc-api/org/springframework/context/annotation/PropertySource.html
     *
     * @return The configurer.
     */
    @Bean
    public static PropertySourcesPlaceholderConfigurer configurer() {
        return new PropertySourcesPlaceholderConfigurer();
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
    public EnvironmentChangeListener environmentChangeListener() {
        return new EnvironmentChangeListener();
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
        return new MemoryMonitor(maxHealthyUsedMemoryRatio);
    }

    @Bean
    public KeyValueStoreConfig keyValueStoreConfig() {
        return new KeyValueStoreConfig();
    }

    @Bean
    public KeyValueStore keyValueStore() {
        return keyValueStoreConfig().keyValueStore();
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

}
