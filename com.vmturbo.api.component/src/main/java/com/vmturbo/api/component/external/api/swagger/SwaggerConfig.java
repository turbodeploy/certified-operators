package com.vmturbo.api.component.external.api.swagger;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

/**
 * Configuration for the Swagger Documentation subsystem for the API component.
 *
 * Note that there are two separate Swagger configurations - one for the base API Component itself,
 * and the shared REST API common to XL and Legacy.
 *
 * The Swagger document for the shared REST API is declared here. The base address is
 * http://{host}/documentation/swagger-ui.html to avoid conflict with the REST API for the Component.
 *
 * The API definition for the Shared REST API is created within the Spring child context defined in
 * ExternalApiConfig. This context has two URL roots:  /vmturbo/rest (old) and /vmturbo/api/v2 (preferred).
 *
 * Note that Swagger-ui requires several resources be located in a fixed address, e.g. "/configuration/ui".
 **/
@Configuration
public class SwaggerConfig extends WebMvcConfigurerAdapter {
    /**
     * Map the Resources for the REST API Swagger Interface to the /documentation URL root.
     *
     * Add the necessary redirectionss to the {@linkplain ViewControllerRegistry} for the child Spring
     * Context in which the Turbonomic REST API @Controllers are instantiated.
     *
     * This includes mapping the root itself, /documentation, to the top-level html file: swagger-ui.html
     *
     * In other words if the user goes to "http://{host}/documentation they will be redirected to the
     * proper URL for the Swagger UI.
     *
     * @param registry - the Spring {@linkplain ViewControllerRegistry} for the child context containing
     *                 the main REST API.
     */
    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addRedirectViewController("/documentation/v2/api-docs", "/vmturbo/api/v2/v2/api-docs");
        registry.addRedirectViewController("/documentation/configuration/ui", "/configuration/ui");
        registry.addRedirectViewController("/documentation/configuration/security", "/configuration/security");
        registry.addRedirectViewController("/documentation/swagger-resources", "/swagger-resources");
        registry.addRedirectViewController("/documentation", "/documentation/swagger-ui.html");
        registry.addRedirectViewController("/documentation/", "/documentation/swagger-ui.html");
    }

    /**
     * Add a Resource entry for the Swagger-UI to be served from the "/resources" folder in the swagger-ui.jar.
     *
     * @param registry - the Spring {@linkplain ResourceHandlerRegistry} for this child Spring Context for the
     *                 Turbonomic REST API.
     */
    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/documentation/**").addResourceLocations("classpath:/META-INF/resources/");
    }
}
