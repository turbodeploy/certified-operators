package com.vmturbo.api.component;

import java.util.EnumSet;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.servlet.DispatcherType;
import javax.servlet.MultipartConfigElement;
import javax.servlet.Servlet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.security.web.session.HttpSessionEventPublisher;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.filter.DelegatingFilterProxy;
import org.springframework.web.servlet.DispatcherServlet;

import com.vmturbo.api.component.external.api.ExternalApiConfig;
import com.vmturbo.api.component.external.api.dispatcher.DispatcherControllerConfig;
import com.vmturbo.api.component.external.api.dispatcher.DispatcherValidatorConfig;
import com.vmturbo.api.component.external.api.service.ServiceConfig;
import com.vmturbo.api.component.external.api.swagger.SwaggerConfig;
import com.vmturbo.api.component.external.api.websocket.ApiWebsocketConfig;
import com.vmturbo.api.external.controller.ProbesController;
import com.vmturbo.api.internal.controller.ApiDiagnosticsConfig;
import com.vmturbo.api.internal.controller.DBAdminController;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.config.PropertiesLoader;

/**
 * This is the "main()" for the API Component. The API component implements
 * all external REST API calls. Some calls are simply forwarded to the correct component. Other
 * calls make one or more calls to other components
 * and then assemble the response - e.g. filtering, joining related information, etc.
 */
@Configuration("theComponent")
@Import({
    ApiComponentGlobalConfig.class,
    ApiWebsocketConfig.class,
    ExternalApiConfig.class,
    DBAdminController.class,
    ProbesController.class,
    SwaggerConfig.class,
    ServiceConfig.class,
    ApiDiagnosticsConfig.class
})
public class ApiComponent extends BaseVmtComponent {

    private static final Logger logger = LogManager.getLogger();

    @Autowired
    private ApiDiagnosticsConfig diagnosticsConfig;

    // env vars
    private static final String ENV_UPLOAD_MAX_FILE_SIZE_KB = "multipartConfigMaxFileSizeKb";
    private static final int DEFAULT_UPLOAD_MAX_FILE_SIZE_KB = 50;

    private static final String ENV_UPLOAD_MAX_REQUEST_SIZE_KB = "multipartConfigMaxRequestSizeKb";
    private static final int DEFAULT_UPLOAD_MAX_REQUEST_SIZE_KB = 200;

    /**
     * Starts the component.
     *
     * @param args The mandatory arguments.
     */
    public static void main(String[] args) {
        startContext(ApiComponent::createContext);
    }

    /**
     * Add the dispatcher servlet that serves up the external API to the spring context.
     *
     * @param rootContext The context to add the servlet to. This will be the parent context of the
     *                    dispatcher servlet.
     * @param contextServer The server that the context is attached to - this is the "root" servlet.
     *                      The dispatcher servlet and all its route mappings will be added to it.
     * @return The {@link ServletHolder} containing the dispatcher servlet.
     */
    public static ServletHolder addDispatcherToContext(
            @Nonnull final AnnotationConfigWebApplicationContext rootContext,
            @Nonnull ServletContextHandler contextServer) {
        final AnnotationConfigWebApplicationContext restContext =
            new AnnotationConfigWebApplicationContext();
        restContext.register(DispatcherControllerConfig.class);
        restContext.register(DispatcherValidatorConfig.class);
        restContext.setParent(rootContext);
        final Servlet restDispatcherServlet = new DispatcherServlet(restContext);
        ((DispatcherServlet)restDispatcherServlet).setNamespace("api dispatcher");
        final ServletHolder restServletHolder = new ServletHolder(restDispatcherServlet);

        // add a multipart config for handling license file uploads
        // Since these are going on a memory-backed file location, we will set size
        // restrictions to prevent using up too much memory.
        int multipartConfigMaxFileSizeKb = 1024 * getOptionalIntEnvProperty(ENV_UPLOAD_MAX_FILE_SIZE_KB,
            DEFAULT_UPLOAD_MAX_FILE_SIZE_KB);
        int multipartConfigMaxRequestSizeKb = 1024 * getOptionalIntEnvProperty(ENV_UPLOAD_MAX_REQUEST_SIZE_KB,
            DEFAULT_UPLOAD_MAX_REQUEST_SIZE_KB);
        // file size threshold (the size at which incoming files are spooled to disk) is set to the
        // max request size to avoid the spooling behavior. Our /tmp folder is backed by RAM anyways
        // so it won't make a diff.
        logger.info("Creating multipart config w/max file size {}kb and max request size {}kb",
            multipartConfigMaxFileSizeKb, multipartConfigMaxRequestSizeKb);
        restServletHolder.getRegistration()
            .setMultipartConfig(new MultipartConfigElement("/tmp/uploads",
                multipartConfigMaxFileSizeKb,
                multipartConfigMaxRequestSizeKb,
                multipartConfigMaxRequestSizeKb));

        // Explicitly add Spring security to the following servlets: report, REST API, WebSocket messages
        final FilterHolder filterHolder = new FilterHolder();
        filterHolder.setFilter(new DelegatingFilterProxy());
        filterHolder.setName("springSecurityFilterChain");
        contextServer.addFilter(filterHolder, ServiceConfig.REPORT_CGI_PATH,
            EnumSet.of(DispatcherType.REQUEST));
        contextServer.addFilter(filterHolder, ApiWebsocketConfig.WEBSOCKET_URL,
            EnumSet.of(DispatcherType.REQUEST));
        for (String pathSpec : ExternalApiConfig.BASE_URL_MAPPINGS) {
            contextServer.addServlet(restServletHolder, pathSpec);
            contextServer.addFilter(filterHolder, pathSpec, EnumSet.of(DispatcherType.REQUEST));
        }
        return restServletHolder;
    }

    /**
     * Registers contexts to use with Spring and Web server. Internally, there is a root context,
     * which provide all the routines common to XL components. Additional child Spring context
     * is created for REST API in order to enforce authentication and authorization
     * (springSecurityFilterChain). Special DispacheerServlet instance is created upon REST Spring
     * context.
     *
     * <p>Spring security filters are added to REST DispatcherServlet, websockets API connection and
     * reporting CGI-BIN directory.
     *
     * @param contextServer Jetty context handler to register with
     * @return rest application context
     * @throws ContextConfigurationException if there is an error loading the external configuration
     * properties
     */
    private static ConfigurableWebApplicationContext createContext(
            @Nonnull ServletContextHandler contextServer) throws ContextConfigurationException {
        final AnnotationConfigWebApplicationContext rootContext =
                new AnnotationConfigWebApplicationContext();
        rootContext.register(ApiComponent.class);
        PropertiesLoader.addConfigurationPropertySources(rootContext);
        final Servlet dispatcherServlet = new DispatcherServlet(rootContext);
        final ServletHolder servletHolder = new ServletHolder(dispatcherServlet);
        contextServer.addServlet(servletHolder, "/*");

        addDispatcherToContext(rootContext, contextServer);

        // Setup Spring context
        final ContextLoaderListener springListener = new ContextLoaderListener(rootContext);
        contextServer.addEventListener(springListener);
        // Publish HTTP session lifecycle events to remove destroyed sessions from SessionRgistry
        contextServer.addEventListener(new HttpSessionEventPublisher());
        return rootContext;
    }

    @Override
    protected void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        try {
            diagnosticsConfig.diagsHandler().dump(diagnosticZip);
        } catch (Exception e) {
            logger.error("Unable to capture diagnostics due to error: ", e);
        }
    }
}
