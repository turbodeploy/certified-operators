package com.vmturbo.api.component;

import java.util.EnumSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.servlet.DispatcherType;
import javax.servlet.Servlet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.filter.DelegatingFilterProxy;
import org.springframework.web.servlet.DispatcherServlet;

import com.vmturbo.api.component.controller.DBAdminController;
import com.vmturbo.api.component.external.api.ExternalApiConfig;
import com.vmturbo.api.component.external.api.dispatcher.DispatcherControllerConfig;
import com.vmturbo.api.component.external.api.dispatcher.DispatcherValidatorConfig;
import com.vmturbo.api.component.external.api.service.AdminService;
import com.vmturbo.api.component.external.api.service.ServiceConfig;
import com.vmturbo.api.component.external.api.swagger.SwaggerConfig;
import com.vmturbo.api.component.external.api.websocket.ApiWebsocketConfig;
import com.vmturbo.api.dto.admin.ProductVersionDTO;
import com.vmturbo.components.common.BaseVmtComponent;

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
    SwaggerConfig.class,
    DBAdminController.class,
    ServiceConfig.class
})
public class ApiComponent extends BaseVmtComponent {

    private static final Logger logger = LogManager.getLogger();

    public static final String VERSION_FILE_NAME = "turbonomic_cluster_version.txt";

    @Autowired
    private ServiceConfig serviceConfig;

    public static void main(String[] args) {
        startContext(ApiComponent::createContext);
    }

    /**
     * Registers contexts to use with Spring and Web server. Internally, there is a root context,
     * which provide all the routines common to XL components. Additional child Spring context
     * is created for REST API in order to enforce authentication and authorization
     * (springSecurityFilterChain). Special DispacheerServlet instance is created upon REST Spring
     * context.
     *
     * Spring security filters are added to REST DispatcherServlet, websockets API connection and
     * reporting CGI-BIN directory
     *
     * @param contextServer Jetty context handler to register with
     * @return rest application context
     */
    private static ConfigurableApplicationContext createContext(
            @Nonnull ServletContextHandler contextServer) {
        final AnnotationConfigWebApplicationContext rootContext =
                new AnnotationConfigWebApplicationContext();
        rootContext.register(ApiComponent.class);
        final Servlet dispatcherServlet = new DispatcherServlet(rootContext);
        final ServletHolder servletHolder = new ServletHolder(dispatcherServlet);
        contextServer.addServlet(servletHolder, "/*");

        final AnnotationConfigWebApplicationContext restContext =
                new AnnotationConfigWebApplicationContext();
        restContext.register(DispatcherControllerConfig.class);
        restContext.register(DispatcherValidatorConfig.class);
        restContext.setParent(rootContext);
        final Servlet restDispatcherServlet = new DispatcherServlet(restContext);
        final ServletHolder restServletHolder = new ServletHolder(restDispatcherServlet);

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

        // Setup Spring context
        final ContextLoaderListener springListener = new ContextLoaderListener(rootContext);
        contextServer.addEventListener(springListener);
        return restContext;
    }

    @Override
    protected void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        try {
            diagnosticZip.putNextEntry(new ZipEntry(VERSION_FILE_NAME));
            final ProductVersionDTO versionDTO = serviceConfig.adminService().getVersionInfo(true);

            diagnosticZip.write((versionDTO.getVersionInfo() + "\n\n").getBytes());
            diagnosticZip.write(("Updates: " + versionDTO.getUpdates() + "\n").getBytes());
            diagnosticZip.write(("Market version: " + versionDTO.getMarketVersion() + "\n").getBytes());
            diagnosticZip.closeEntry();
        } catch (Exception e) {
            logger.error("Unable to capture diagnostics due to error: ", e);
        }
    }
}
