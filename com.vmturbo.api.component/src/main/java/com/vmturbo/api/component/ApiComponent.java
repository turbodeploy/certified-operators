package com.vmturbo.api.component;

import java.util.EnumSet;

import javax.annotation.Nonnull;
import javax.servlet.DispatcherType;
import javax.servlet.Servlet;

import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
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
import com.vmturbo.api.component.external.api.service.ServiceConfig;
import com.vmturbo.api.component.external.api.swagger.SwaggerConfig;
import com.vmturbo.api.component.external.api.websocket.ApiWebsocketConfig;
import com.vmturbo.components.common.BaseVmtComponent;

/**
 * This is the "main()" for the API Component. The API component implements
 * all external REST API calls. Some calls are simply forwarded to the correct component. Other
 * calls make one or more calls to other components
 * and then assemble the response - e.g. filtering, joining related information, etc.
 */
@Configuration("theComponent")
@EnableDiscoveryClient
@Import({ApiComponentGlobalConfig.class, ApiWebsocketConfig.class, ExternalApiConfig.class,
        SwaggerConfig.class, DBAdminController.class})
public class ApiComponent extends BaseVmtComponent {

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
}
