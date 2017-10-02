package com.vmturbo.components.common;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

/**
 * Create Spring Beans provided by com.vmturbo.components.common.
 **/
@Configuration
public class BaseVmtComponentConfig {

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
}
