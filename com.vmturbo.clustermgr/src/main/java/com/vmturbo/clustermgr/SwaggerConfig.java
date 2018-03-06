package com.vmturbo.clustermgr;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

/**
 * Web configuration to enable Swagger access
 **/
@Configuration
public class SwaggerConfig extends WebMvcConfigurerAdapter {

    /*
     * Add a Resource entry for the Swagger-UI to be served from the "/swagger" folder in the
     * component container.
     *
     * @param registry - the Spring {@linkplain ResourceHandlerRegistry} for this child
     *                 Spring Context for the Turbonomic XL component
     */
    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        // resources for the internal debug swagger UI for a component
        registry.addResourceHandler("/swagger/**").addResourceLocations("file:/swagger/");
    }

    /**
     * Add a redirection for /swagger to /swagger/index.html
     *
     * @param registry is the injected registry of all View Controllers, to which the new direct is added.
     */
    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addRedirectViewController("/swagger", "/swagger/index.html");
    }
}