package com.vmturbo.reports.component;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.embedded.EmbeddedServletContainerFactory;
import org.springframework.boot.context.embedded.tomcat.TomcatEmbeddedServletContainerFactory;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.commons.util.InetUtils;
import org.springframework.cloud.commons.util.InetUtilsProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.health.sql.MariaDBHealthMonitor;

/**
 * Component for running reports.
 */
@Component("theComponent")
@Import({ReportingConfig.class, ReportingConsulConfig.class})
@Configuration
@EnableWebMvc
@EnableDiscoveryClient
public class ReportsComponent extends BaseVmtComponent {

    @Autowired
    private ReportingConfig reportingConfig;

    @Value("${spring.application.name}")
    private String componentName;
    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

    @Override
    public String getComponentName() {
        return componentName;
    }

    @PostConstruct
    private void setup() {
        // We call localFlyway to promote reporting db migration directly
        reportingConfig.dbConfig().localFlyway();
        reportingConfig.scheduler().scheduleAllReportsGeneration();

        // add kafka producer health check
        getHealthMonitor().addHealthCheck(reportingConfig.kafkaHealthMonitor());
        getHealthMonitor().addHealthCheck(
                new MariaDBHealthMonitor(mariaHealthCheckIntervalSeconds,
                        reportingConfig.dbConfig().dataSource()::getConnection));
    }

    public static void main(String[] args) {
        // apply the configuration properties for this component prior to Spring instantiation
        fetchConfigurationProperties();
        // instantiate and run this component
        new SpringApplicationBuilder()
                .sources(ReportsComponent.class)
                .run(args);
    }

    @Override
    @Nonnull
    protected Optional<Server> buildGrpcServer(@Nonnull final ServerBuilder builder) {
        // Monitor for server metrics with prometheus.
        final MonitoringServerInterceptor monitoringInterceptor =
            MonitoringServerInterceptor.create(me.dinowernli.grpc.prometheus.Configuration.allMetrics());

        return Optional.of(builder
            .addService(ServerInterceptors.intercept(reportingConfig.reportingService(), monitoringInterceptor))
            .build());
    }

    @Bean
    protected EmbeddedServletContainerFactory containerFactory() {
        return new TomcatEmbeddedServletContainerFactory();
    }

    @Bean
    protected InetUtils inetUtils() {
        return new InetUtils(new InetUtilsProperties());
    }
}
