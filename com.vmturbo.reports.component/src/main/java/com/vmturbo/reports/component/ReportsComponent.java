package com.vmturbo.reports.component;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.embedded.EmbeddedServletContainerFactory;
import org.springframework.boot.context.embedded.tomcat.TomcatEmbeddedServletContainerFactory;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.commons.util.InetUtils;
import org.springframework.cloud.commons.util.InetUtilsProperties;
import org.springframework.cloud.consul.config.ConsulConfigBootstrapConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import com.vmturbo.components.common.BaseVmtComponent;

/**
 * Component for running reports.
 */
@Component("theComponent")
@Import({ReportingConfig.class, ConsulConfigBootstrapConfiguration.class})
@Configuration
@EnableWebMvc
@EnableDiscoveryClient
public class ReportsComponent extends BaseVmtComponent {

    @Autowired
    private ReportingConfig reportingConfig;

    @Override
    public String getComponentName() {
        return "reports-component";
    }

    @PostConstruct
    private void setup() {
        // add kafka producer health check
        getHealthMonitor().addHealthCheck(reportingConfig.kafkaHealthMonitor());
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
        return Optional.of(builder.addService(reportingConfig.reportingService()).build());
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
