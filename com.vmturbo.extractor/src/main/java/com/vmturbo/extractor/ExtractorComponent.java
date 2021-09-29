package com.vmturbo.extractor;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import io.grpc.BindableService;
import io.grpc.ServerInterceptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtServerInterceptor;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.health.sql.PostgreSQLHealthMonitor;
import com.vmturbo.extractor.action.ActionConfig;
import com.vmturbo.extractor.diags.ExtractorDiagnosticsConfig;
import com.vmturbo.extractor.grafana.GrafanaConfig;
import com.vmturbo.extractor.service.ExtractorRpcConfig;
import com.vmturbo.extractor.topology.TopologyListenerConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * The Extractor component receiving information broadcast from various components in the system. It
 * writes data to timescaledb database for query, export, and search/sort/filter.
 */
@Configuration("theComponent")
@Import({TopologyListenerConfig.class,
        ActionConfig.class,
        ExtractorRpcConfig.class,
        ExtractorDbConfig.class,
        GrafanaConfig.class,
        ExtractorDiagnosticsConfig.class,
        SpringSecurityConfig.class,
        ExtractorGlobalConfig.class})
public class ExtractorComponent extends BaseVmtComponent {
    private static final Logger logger = LogManager.getLogger();

    @Autowired
    private ExtractorDiagnosticsConfig diagnosticsConfig;

    @Autowired
    private TopologyListenerConfig listenerConfig;

    @Autowired
    private ExtractorRpcConfig rpcConfig;

    @Autowired
    private ExtractorDbConfig extractorDbConfig;

    @Autowired
    private ExtractorGlobalConfig extractorGlobalConfig;

    @Autowired
    private SpringSecurityConfig securityConfig;

    @Value("${timescaledbHealthCheckIntervalSeconds:60}")
    private int timescaledbHealthCheckIntervalSeconds;

    private void setupHealthMonitor() throws InterruptedException {
        logger.info("Adding PostgreSQL health checks to the component health monitor.");
        try {
            getHealthMonitor().addHealthCheck(new PostgreSQLHealthMonitor(
                    timescaledbHealthCheckIntervalSeconds,
                    extractorDbConfig.ingesterEndpoint().datasource()::getConnection));
        } catch (UnsupportedDialectException | SQLException e) {
            throw new IllegalStateException("DbEndpoint not available, could not start health monitor", e);
        }
    }

    @Override
    public void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        diagnosticsConfig.diagnosticsHandler().dump(diagnosticZip);
    }

    @Nonnull
    @Override
    public List<BindableService> getGrpcServices() {
        return Collections.singletonList(rpcConfig.extractorSettingService());
    }

    @Nonnull
    @Override
    public List<ServerInterceptor> getServerInterceptors() {
        return Collections.singletonList(new JwtServerInterceptor(securityConfig.apiAuthKVStore()));
    }

    /**
     * Starts the component.
     *
     * @param args none expected
     */
    public static void main(String[] args) {
        startComponent(ExtractorComponent.class);
    }

    @Override
    protected void onStartComponent() {
        logger.debug("Writer config: {}", listenerConfig.writerConfig());
        // only set up postgres health monitor if reporting or searchApi is enabled
        if (extractorGlobalConfig.requireDatabase()) {
            try {
                setupHealthMonitor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Failed to set up health monitor -"
                        + "interrupted while waiting for endpoint initialization", e);
            }
            if (extractorDbConfig.dbSizeMonitorEnabled) {
                try {
                    extractorDbConfig.dbSizeMonitor().activate();
                } catch (Exception e) {
                    logger.error("Failed to establish DbSizeMonitor: {}", e.toString());
                }
            }
        }
    }
}
