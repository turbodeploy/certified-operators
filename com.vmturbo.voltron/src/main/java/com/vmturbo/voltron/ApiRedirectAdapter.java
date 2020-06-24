package com.vmturbo.voltron;

import java.nio.file.Paths;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * Serves the UI from the root URL, instead of from the /api_component/ subpath.
 *
 * <p/>The UI will also be accessible via the /api_component/app/index.html subpath, but the
 * API itself is only served from the root URL, so the UI served from the subpath may not work.
 */
@Configuration
@Import({ApiSecurityConfig.class})
public class ApiRedirectAdapter implements WebMvcConfigurer {

    /**
     * The path to the UX folder.
     */
    @Value("${ux-path}")
    private String uxPath;

    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addRedirectViewController("/", "/app/index.html");
        // Redirect to a locally running Grafana.
        // Note - we don't do the "user" authentication, since this is only for development.
        registry.addRedirectViewController("/reports", "http://localhost:3000");
        registry.addRedirectViewController("/vmturbo/apidoc", "/vmturbo/apidoc/index.html");
        registry.addRedirectViewController("/swagger", "/swagger/index.html");
    }

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        // This is copied from ExternalApiConfig.

        // We use "Paths" to normalize the paths to the relevant directories, and then we need to
        // add a trailing "/" for the resource handler logic.
        final String appLocation = Paths.get(uxPath, "app").normalize().toString() + "/";
        final String assetsLocation = Paths.get(uxPath, "assets").normalize().toString() + "/";
        final String docLocation = Paths.get(uxPath, "doc").normalize().toString() + "/";

        // resources for the Turbonomic UI
        registry.addResourceHandler("/app/**")
            .addResourceLocations("file:" + appLocation);
        // In a production environment the assets and docs are in a resource bundle, and there
        // are no requests to those paths. But in a local development environment with an
        // un-compressed UI we need to support these routes or else the UI won't work.
        registry.addResourceHandler("/assets/**")
            .addResourceLocations("file:" + assetsLocation);
        registry.addResourceHandler("/doc/**")
            .addResourceLocations("file:" + docLocation);
        registry.addResourceHandler("/vmturbo/apidoc/**")
            .addResourceLocations("file:/swagger/");
        registry.addResourceHandler("/swagger/**")
            .addResourceLocations("file:/swagger/");
    }
}
