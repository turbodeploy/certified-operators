package com.vmturbo.reports.component;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;

import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.health.sql.MariaDBHealthMonitor;

/**
 * Component for running reports.
 */
@Component("theComponent")
@Import({ReportingConfig.class})
@Configuration
public class ReportsComponent extends BaseVmtComponent {

    @Autowired
    private ReportingConfig reportingConfig;

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

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
        startContext(ReportsComponent.class);
    }

    @Override
    @Nonnull
    protected Optional<io.grpc.Server> buildGrpcServer(@Nonnull final ServerBuilder builder) {
        // Monitor for server metrics with prometheus.
        final MonitoringServerInterceptor monitoringInterceptor =
            MonitoringServerInterceptor.create(me.dinowernli.grpc.prometheus.Configuration.allMetrics());

        return Optional.of(builder
            .addService(ServerInterceptors.intercept(reportingConfig.reportingService(), monitoringInterceptor))
            .build());
    }
}
