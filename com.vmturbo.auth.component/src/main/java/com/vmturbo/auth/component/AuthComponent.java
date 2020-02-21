package com.vmturbo.auth.component;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import javax.servlet.DispatcherType;
import javax.servlet.Servlet;

import io.grpc.BindableService;
import io.grpc.ServerInterceptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.filter.DelegatingFilterProxy;
import org.springframework.web.servlet.DispatcherServlet;

import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtServerInterceptor;
import com.vmturbo.auth.component.licensing.LicensingConfig;
import com.vmturbo.auth.component.spring.SpringAuthFilter;
import com.vmturbo.auth.component.userscope.UserScopeServiceConfig;
import com.vmturbo.auth.component.widgetset.WidgetsetConfig;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.config.PropertiesLoader;
import com.vmturbo.components.common.health.sql.MariaDBHealthMonitor;

/**
 * The main auth component.
 */
@Configuration("theComponent")
@Import({AuthRESTSecurityConfig.class, AuthDBConfig.class, SpringSecurityConfig.class,
        WidgetsetConfig.class, LicensingConfig.class, UserScopeServiceConfig.class})
public class AuthComponent extends BaseVmtComponent {
    private static final String PATH_SPEC = "/*";
    /**
     * The logger.
     */
    private final Logger logger = LogManager.getLogger(AuthComponent.class);

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

    @Autowired
    private AuthDBConfig authDBConfig;

    @Autowired
    private AuthRESTSecurityConfig authRESTSecurityConfig;

    @Autowired
    private WidgetsetConfig widgetsetConfig;

    @Autowired
    private LicensingConfig licensingConfig;

    @Autowired
    private UserScopeServiceConfig userScopeServiceConfig;

    /**
     * JWT token verification and decoding.
     */
    @Autowired
    private SpringSecurityConfig securityConfig;

    @PostConstruct
    private void setup() {
        logger.info("Adding MariaDB health check to the component health monitor.");
        getHealthMonitor()
            .addHealthCheck(new MariaDBHealthMonitor(mariaHealthCheckIntervalSeconds,
                                    authDBConfig.dataSource()::getConnection));
                    // (May 20, 2019, Gary Zeng) Current Kafka monitor only updates status when
                    // "SendMessageCallbackHandler" is triggered. We do trigger "handler" when
                    // auth start (to send out license related to notification ), so if Kafka is down,
                    // the status will be updated as "not healthy" to K8s. But auth will NOT trigger
                    // the "handler", until the "updateLicenseSummaryPeriodically" logic kicked in,
                    // which is everyday at midnight. OM-46416 is opened to fix the monitor.
                    // .addHealthCheck(licensingConfig.kafkaProducerHealthMonitor());
    }

    /**
     * Starts the component.
     *
     * @param args The mandatory arguments.
     */
    public static void main(String[] args) {
        startContext(AuthComponent::createContext);
    }

    @Nonnull
    @Override
    public List<ServerInterceptor> getServerInterceptors() {
        final JwtServerInterceptor jwtInterceptor = new JwtServerInterceptor(securityConfig.apiAuthKVStore());
        return Collections.singletonList(jwtInterceptor);
    }

    @Nonnull
    @Override
    public List<BindableService> getGrpcServices() {
        return Arrays.asList(widgetsetConfig.widgetsetRpcService(authRESTSecurityConfig.targetStore()),
            licensingConfig.licenseManager(),
            licensingConfig.licenseCheckService(),
            userScopeServiceConfig.userScopeService());
    }

    /**
     * Register child Spring context for REST API in order to enforce authentication and authorization
     * (springSecurityFilterChain). Special DispatcherServlet instance is created upon REST Spring
     * context.
     *
     * <p>Spring security filters, which includes custom {@link SpringAuthFilter}, are added to
     * REST DispatcherServlet.
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
        PropertiesLoader.addConfigurationPropertySources(rootContext);
        rootContext.register(AuthComponent.class);

        final AnnotationConfigWebApplicationContext restContext =
                new AnnotationConfigWebApplicationContext();
        restContext.setParent(rootContext);
        final Servlet restDispatcherServlet = new DispatcherServlet(restContext);
        final ServletHolder restServletHolder = new ServletHolder(restDispatcherServlet);

        // Explicitly add Spring security to the following servlets: REST API
        final FilterHolder filterHolder = new FilterHolder();
        filterHolder.setFilter(new DelegatingFilterProxy());
        filterHolder.setName("springSecurityFilterChain");
        contextServer.addServlet(restServletHolder, PATH_SPEC);
        contextServer.addFilter(filterHolder, PATH_SPEC, EnumSet.of(DispatcherType.REQUEST));

        // Setup Spring context
        final ContextLoaderListener springListener = new ContextLoaderListener(rootContext);
        contextServer.addEventListener(springListener);
        return restContext;
    }

}
