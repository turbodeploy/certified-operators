package com.vmturbo.api.component.external.api;

import java.util.List;

import javax.servlet.ServletContext;

import com.google.common.collect.ImmutableList;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.vmturbo.api.component.external.api.service.MarketsService;
import com.vmturbo.components.common.LoggingFilter;

/**
 * Configuration for the external Turbonomic REST API.
 * <p>
 * The external REST API is defined by controllers such as
 * {@link com.vmturbo.api.controller.MarketsController}, which live in a package shared
 * between legacy and XL. The REST controllers forward calls to Java interfaces such as
 * {@link com.vmturbo.api.serviceinterfaces.IMarketsService}.
 * <p>
 * We create a {@link DispatcherServlet} with it's own application context containing all the
 * controllers. We also create implementations of the Java interfaces - for example,
 * {@link MarketsService}.
 * <p>
 * Note:  There is no current commitment to backwards compatibility for the V1 API in XL. If the decision is made
 * to support the V1 REST API, then we would simply add an additional DispatcherServlet.
 */
@Configuration
@Import({ApiSecurityConfig.class})
public class ExternalApiConfig extends WebMvcConfigurerAdapter {

    @Autowired
    private ServletContext servletContext;
    @Autowired
    private WebApplicationContext applicationContext;

    /**
     * The base URLs for the external REST API. All external REST API URL's should
     * start with one of these.
     */
    public static final List<String> BASE_URL_MAPPINGS = ImmutableList.of(
            // This is the base currently used in the new UX.
            "/vmturbo/rest/*",
            // This should be the future, to align with the V1 API which is at /api/
            "/vmturbo/api/v2/*",
            // for SAML filters, see ApiSecurityConfig#samlFilter
            "/vmturbo/saml/*",
            // We are also supporting /api as of OM-32218
            "/api/*"
            );

    /**
     * A logging filter to log requests coming in to the API component.
     * This makes it easier to debug failing calls.
     *
     * @return The filter.
     */
    @Bean
    public LoggingFilter loggingFilter() {
        return new LoggingFilter();
    }

    /**
     * Add a link to the file system containing the static resources for the web application.
     * Any resource reference is redirected to the file system directory "/www". The application itself
     * is located at "/www/app".
     *
     * Also handle mapping for "swagger-ui.html" and "/webjars/**" to enable the Swagger UI for the
     * API Component itself (not the Turbonomic REST API - for that see
     *        {@link com.vmturbo.api.component.external.api.swagger.SwaggerConfig}.
     * The resources for the Swagger-UI are contained in the springfox-swagger-ui jar.
     *
     * @param registry is the {@link ResourceHandlerRegistry} to which the new ResourceHandler will be added.
     */
    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {

        // resources for the Turbonomic UI
        registry.addResourceHandler("/app/**")
                .addResourceLocations("file:/www/app/");
        registry.addResourceHandler("/assets/**")
                .addResourceLocations("file:/www/assets/");
        registry.addResourceHandler("/vmturbo/apidoc/**")
                .addResourceLocations("file:/swagger/");
        registry.addResourceHandler("/swagger/**")
                .addResourceLocations("file:/swagger/");
    }

    /**
     * Add a redirection from "/" to "/app/index.html" to tee up the main UX url.
     *
     * @param registry is the injected registry of all View Controllers, to which the new direct is added.
     */
    // see https://stackoverflow.com/questions/27381781/java-spring-boot-how-to-map-my-my-app-root-to-index-html
    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
        registry.addRedirectViewController("/", "/app/index.html");
        registry.addRedirectViewController("/vmturbo/apidoc", "/vmturbo/apidoc/index.html");
        registry.addRedirectViewController("/swagger", "/swagger/index.html");
    }

}
