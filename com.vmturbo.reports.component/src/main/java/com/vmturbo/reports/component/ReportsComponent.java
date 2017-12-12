package com.vmturbo.reports.component;

import java.util.Optional;

import javax.annotation.Nonnull;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import org.springframework.beans.factory.annotation.Autowired;
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

/**
 * Component for running reports.
 */
@Component("theComponent")
@Import({ReportingConfig.class})
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

    public static void main(String[] args) {
        new SpringApplicationBuilder().sources(ReportsComponent.class).run(args);
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
