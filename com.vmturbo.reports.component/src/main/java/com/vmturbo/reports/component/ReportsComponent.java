package com.vmturbo.reports.component;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import io.grpc.BindableService;

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

    /**
     * Starts the component.
     *
     * @param args The mandatory arguments.
     */
    public static void main(String[] args) {
        startContext(ReportsComponent.class);
    }

    @Nonnull
    @Override
    public List<BindableService> getGrpcServices() {
        return Collections.singletonList(reportingConfig.reportingService());
    }
}
