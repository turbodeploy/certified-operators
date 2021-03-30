package com.vmturbo.extractor.grafana;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.ExtractorGlobalConfig;
import com.vmturbo.extractor.grafana.Grafanon.GrafanonConfig;
import com.vmturbo.extractor.grafana.client.GrafanaClient;
import com.vmturbo.extractor.grafana.client.GrafanaClientConfig;
import com.vmturbo.extractor.schema.ExtractorDbBaseConfig;

/**
 * Configures the Grafana initialization logic.
 */
@Configuration
@Import({ExtractorDbBaseConfig.class, ExtractorGlobalConfig.class})
public class GrafanaConfig {

    @Value("${grafanaHost:grafana}")
    private String grafanaHost;

    @Value("${grafanaPort:3000}")
    private int grafanaPort;

    @Value("${grafanaAdminUser:admin}")
    private String adminUser;

    @Value("${grafanaAdminPassword:admin}")
    private String adminPassword;

    @Value("${grafanaErrorSleepIntervalSeconds:30}")
    private int grafanaErrorSleepIntervalSec;

    @Value("${grafanaTimescaleDatasourceName:Turbo Timescale}")
    private String datasourceName;

    @Value("${grafanaEditorUsername:turbo-report-editor}")
    private String grafanaEditorUsername;

    @Value("${grafanaEditorDisplayName:Report Editor}")
    private String grafanaEditorDisplayName;

    @Value("${grafanaEditorPassword:}")
    private String grafanaEditorPassword;

    /**
     * This is the path to the dashboard folders.
     * TODO (roman, Jun 10 2019): Allow injecting additional dashboard paths.
     */
    @Value("${grafanaBuiltinDashboardPath:/dashboards}")
    private String builtinDashboardPath;

    /**
     * Configuration used to enable/disable reporting data ingestion.
     */
    @Value("${enableReporting:false}")
    private boolean enableReporting;

    @Autowired
    private ExtractorDbConfig extractorDbConfig;

    @Autowired
    private ExtractorGlobalConfig extractorGlobalConfig;

    /**
     * The one that drives the grafana refresh.
     *
     * @return The {@link Grafanon}.
     */
    @Bean
    public Grafanon grafanon() {
        GrafanonConfig config = new GrafanonConfig(() -> extractorDbConfig.grafanaQueryEndpoint().getConfig())
                .setTimescaleDisplayName(datasourceName)
                .setErrorSleepInterval(grafanaErrorSleepIntervalSec, TimeUnit.SECONDS)
                .setEditorDisplayName(grafanaEditorDisplayName)
                .setEditorUsername(grafanaEditorUsername)
                .setEditorPassword(grafanaEditorPassword);
        return new Grafanon(config, dashboardsOnDisk(), grafanaClient(), extractorGlobalConfig.featureFlags(), extractorDbConfig.ingesterEndpoint());
    }

    /**
     * REST API client for Grafana.
     *
     * @return The {@link GrafanaClient}.
     */
    @Bean
    public GrafanaClient grafanaClient() {
        final GrafanaClientConfig clientConfig = new GrafanaClientConfig()
                .setGrafanaHost(grafanaHost)
                .setGrafanaPort(grafanaPort)
                .setAdminUser(adminUser)
                .setAdminPassword(adminPassword);
        return new GrafanaClient(clientConfig);
    }

    /**
     * Accessor for the dashboards stored on disk.
     *
     * @return The {@link DashboardsOnDisk}.
     */
    @Bean
    public DashboardsOnDisk dashboardsOnDisk() {
        return new DashboardsOnDisk(builtinDashboardPath);
    }

    /**
     * A controller for debugging purposes.
     *
     * @return The {@link GrafanonController}.
     */
    @Bean
    public GrafanonController grafanonController() {
        return new GrafanonController(grafanon());
    }
}
