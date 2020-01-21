package com.vmturbo.api.component.external.api.swagger;

import org.springframework.beans.factory.annotation.Value;
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
 * http://{host}/vmturbo/apidoc/swagger-ui.html to avoid conflict with the REST API for the Component.
 *
 * The API definition for the Shared REST API is created within the Spring child context defined in
 * ExternalApiConfig. This context has two URL roots:  /vmturbo/rest (old) and /api/v3 (preferred).
 *
 * Note that Swagger-ui requires several resources be located in a fixed address, e.g. "/configuration/ui".
 **/
@Configuration
public class SwaggerConfig extends WebMvcConfigurerAdapter {

    @Value("${externalApiSwaggerUri}")
    private String externalApiSwaggerUri;

    @Value("${externalApiFileSystemBase}")
    private String externalApiFileSystemBase;

    /**
     * Add a Resource entry for the Swagger-UI to be served from the "/resources" folder in the swagger-ui.jar.
     *
     * @param registry - the Spring {@linkplain ResourceHandlerRegistry} for this child Spring Context for the
     *                 Turbonomic REST API.
     */
    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        // resources for the Turbonomic UI
        registry.addResourceHandler(externalApiSwaggerUri)
                .addResourceLocations(externalApiFileSystemBase);
    }
}
