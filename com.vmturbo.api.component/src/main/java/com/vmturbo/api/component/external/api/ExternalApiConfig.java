package com.vmturbo.api.component.external.api;

import static com.vmturbo.api.component.external.api.HeaderApiSecurityConfig.VENDOR_URL_CONTEXT_TAG;

import java.nio.file.Paths;
import java.util.List;

import javax.servlet.ServletContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import com.vmturbo.api.component.external.api.service.MarketsService;
import com.vmturbo.api.component.security.HeaderAuthenticationCondition;
import com.vmturbo.components.common.LoggingFilter;
import com.vmturbo.components.common.utils.EnvironmentUtils;

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
@Import({ApiSecurityConfig.class, HeaderApiSecurityConfig.class, SamlApiSecurityConfig.class, OpenIdApiSecurityConfig.class})
public class ExternalApiConfig extends WebMvcConfigurerAdapter {

    /**
     * The path to the UX folder in the container. Must be absolute (start from root ('/')).
     */
    @Value("${ux-path:/www}")
    private String uxPath;

    @Autowired
    private ServletContext servletContext;
    @Autowired
    private WebApplicationContext applicationContext;

    private static final Logger logger = LogManager.getLogger();

    /**
     * The base URLs for the external REST API. All external REST API URL's should
     * start with one of these.
     */
    public static final List<String> BASE_URL_MAPPINGS = getBaseURLMappings();

    /**
     * Build base URLs for the external REST API. All external REST API URL's should
     * start with one of these.
     *
     * @return base URLs for the external REST API.
     */
    @VisibleForTesting
    static ImmutableList<String> getBaseURLMappings() {
        final Builder<String> urlListBuilder = ImmutableList.builder();
        final ImmutableList<String> urlList = ImmutableList.of(
                // This is the base currently used in the new UX.
                "/vmturbo/rest/*",
                // This is the versioned name of the REST API, and should be preferred going forward
                // It is aligned with the V2 API in OpsManager, which has a base URL of /api/v2
                "/api/v3/*",
                // vmturbo is needed, so proxy server (e.g. nginx) knowS where to route our requests
                // in integration environment.
                "/vmturbo/saml2/*",
                "/vmturbo/oauth2/*",
                // We are also supporting /api as of OM-32218
                "/api/*");
        urlListBuilder.addAll(urlList);
        // if in integration mode, e.g. Cisco Intersight, adding vendor specific URL context too.
        if (EnvironmentUtils.parseBooleanFromEnv(HeaderAuthenticationCondition.ENABLED)) {
            EnvironmentUtils.getOptionalEnvProperty(VENDOR_URL_CONTEXT_TAG)
                    .ifPresent(vendorUrlContext -> {
                        urlListBuilder.add(String.format("/api/%s/v3/*", vendorUrlContext));
                        logger.info("Adding vendor URl context: {}", vendorUrlContext);
                    });
        }
        return urlListBuilder.build();
    }

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
