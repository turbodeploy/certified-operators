package com.vmturbo.ml.datastore.influx;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.util.UriComponentsBuilder;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.MetricTypeWhitelist.MetricType;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.ActionStateWhitelist.ActionState;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.ActionTypeWhitelist.ActionType;
import com.vmturbo.components.common.BaseVmtComponentConfig;
import com.vmturbo.ml.datastore.influx.Obfuscator.HashingObfuscator;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Configuration for interacting with Influx DB in the ML datastore.
 */
@Configuration
@Import({
    BaseVmtComponentConfig.class
})
public class InfluxConfig {

    @Value("${influxHost}")
    private String influxHost;

    @Value("${influxPort}")
    private int influxPort;

    @Value("${influxUsername}")
    private String influxUsername;

    @Value("${influxDatabaseName}")
    private String influxDatabaseName;

    @Value("${influxShardDuration}")
    private String influxShardDuration;

    @Value("${influxRetentionPeriod}")
    private String influxRetentionPeriod;

    @Value("${influxRetentionPolicyName}")
    private String influxRetentionPolicyName;

    @Value("${authHost}")
    private String authHost;

    @Value("${authRoute:}")
    private String authRoute;

    @Value("${serverHttpPort}")
    private int authPort;

    @Value("${authRetryDelaySecs}")
    private int authRetryDelaySecs;

    @Value("${influxBatchSize}")
    private int influxBatchSize;

    @Value("${gzipToInflux}")
    private boolean gzipToInflux;

    @Value("${defaultCommodityWhitelist}")
    private String[] defaultCommodityWhitelist;

    @Value("${defaultMetricTypeWhitelist}")
    private String[] defaultMetricTypeWhitelist;

    @Value("${defaultActionTypeWhitelist}")
    private String[] defaultActionTypeWhitelist;

    @Value("${defaultActionStateWhitelist}")
    private String[] defaultActionStateWhitelist;

    @Value("${clustersSupported}")
    private boolean clustersSupported;

    @Value("${jitterEnabled}")
    private boolean jitterEnabled;

    @Value("${metricJitter:0}")
    private double metricJitter;

    @Autowired
    private BaseVmtComponentConfig baseVmtComponentConfig;

    @Bean
    public InfluxMetricsWriterFactory influxDBConnectionFactory() {
        DBPasswordUtil dbPasswordUtil = new DBPasswordUtil(authHost, authPort, authRoute,
            authRetryDelaySecs);

        return new InfluxMetricsWriterFactory.Builder()
            .setInfluxConnectionUser(influxUsername)
            .setInfluxConnectionPassword(dbPasswordUtil.getInfluxDbRootPassword())
            .setInfluxConnectionUrl(getInfluxUrl())
            .setInfluxDbDatabase(influxDatabaseName)
            .setShardDuration(influxShardDuration)
            .setRetentionPeriod(influxRetentionPeriod)
            .setRetentionPolicyName(influxRetentionPolicyName)
            .setInfluxBatchSize(influxBatchSize)
            .setGzipToInflux(gzipToInflux)
            .build();
    }

    @Bean
    public MetricsStoreWhitelist metricsStoreWhitelist() {
        Map<MetricsStoreWhitelist.WhitelistType<?>, Set<? extends Enum<?>>> defaultWhitelists = new HashMap<>();
        defaultWhitelists.put(MetricsStoreWhitelist.COMMODITY_TYPE, defaultCommodityTypeWhitelist());
        defaultWhitelists.put(MetricsStoreWhitelist.METRIC_TYPE, defaultMetricTypeWhitelist());
        defaultWhitelists.put(MetricsStoreWhitelist.ACTION_STATE, defaultActionStateWhitelist());
        defaultWhitelists.put(MetricsStoreWhitelist.ACTION_TYPE, defaultActionTypeWhitelist());
        return new MetricsStoreWhitelist(defaultWhitelists,
                clustersSupported,
                baseVmtComponentConfig.keyValueStore());
    }

    @Bean
    public MetricJitter metricJitter() {
        return new MetricJitter(jitterEnabled, metricJitter);
    }

    @Bean
    public Set<CommodityType> defaultCommodityTypeWhitelist() {
        return Arrays.stream(defaultCommodityWhitelist)
            .map(this::stripArrayCharacters)
            .map(CommodityType::valueOf)
            .collect(Collectors.toSet());
    }

    @Bean
    public Set<MetricType> defaultMetricTypeWhitelist() {
        return Arrays.stream(defaultMetricTypeWhitelist)
            .map(this::stripArrayCharacters)
            .map(MetricType::valueOf)
            .collect(Collectors.toSet());
    }

    @Bean
    public Set<ActionType> defaultActionTypeWhitelist() {
        return Arrays.stream(defaultActionTypeWhitelist)
                .map(this::stripArrayCharacters)
                .map(ActionType::valueOf)
                .collect(Collectors.toSet());
    }

    @Bean
    public Set<MLDatastore.ActionStateWhitelist.ActionState> defaultActionStateWhitelist() {
        return Arrays.stream(defaultActionStateWhitelist)
                .map(this::stripArrayCharacters)
                .map(ActionState::valueOf)
                .collect(Collectors.toSet());
    }

    @Nonnull
    private String stripArrayCharacters(@Nonnull final String str) {
        // Strip off '[' or ']' characters from a string
        return str.replaceAll("[\\[\\]]", "");
    }

    /**
     * Returns database connection URL.
     *
     * @return DB connection URL
     */
    @Nonnull
    private String getInfluxUrl() {
        return UriComponentsBuilder.newInstance()
            .scheme("http")
            .host(influxHost)
            .port(influxPort)
            .build()
            .toUriString();
    }
}
