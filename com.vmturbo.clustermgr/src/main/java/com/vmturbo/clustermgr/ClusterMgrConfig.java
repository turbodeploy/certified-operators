package com.vmturbo.clustermgr;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.FormHttpMessageConverter;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.GsonHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.http.converter.xml.SourceHttpMessageConverter;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;

import com.vmturbo.clustermgr.aggregator.DataAggregator;
import com.vmturbo.clustermgr.collectors.DataMetricLogs;
import com.vmturbo.clustermgr.management.ComponentRegistrationConfig;
import com.vmturbo.clustermgr.transfer.DataTransfer;
import com.vmturbo.common.protobuf.logging.LoggingREST.LogConfigurationServiceController;
import com.vmturbo.common.protobuf.logging.LoggingREST.TracingConfigurationServiceController;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.GrpcChannelFactory;
import com.vmturbo.components.common.OsCommandProcessRunner;
import com.vmturbo.components.common.OsProcessFactory;
import com.vmturbo.components.common.logging.LogConfigurationService;
import com.vmturbo.components.common.logging.TracingConfigurationRpcService;
import com.vmturbo.components.common.tracing.TracingManager;
import com.vmturbo.components.common.utils.BuildProperties;
import com.vmturbo.kvstore.ConsulKeyValueStore;
import com.vmturbo.proactivesupport.DataCollectorFramework;

/**
 * Spring configuration for cluster manager component.
 */
@Configuration
@EnableWebMvc
@Import({ComponentRegistrationConfig.class})
public class ClusterMgrConfig extends WebMvcConfigurerAdapter {

    @Value("${consul_host}")
    private String consulHost;
    @Value("${consul_port:8500}")
    private int consulPort;

    /**
     * The urgent collection interval setting.
     * The default is 10 minutes.
     */
    @Value("${collectionIntervalUrgentSec:600}")
    private long collectionIntervalUrgent;

    /**
     * The offline collection interval setting.
     * The default is 3 days.
     */
    @Value("${collectionIntervalOfflineSec:259200}")
    private long collectionIntervalOffline;

    /**
     * The TCP/IP bridge receiver port.
     */
    @Value("${bridgePort:8120}")
    private int bridgePort;

    /**
     * The data forwarding interval.
     * The default is 7 days.
     */
    @Value("${dataForwardIntervalSec:604800}")
    private long dataForwardInterval;

    /**
     * The flag, if set to {@code true}, will lock out the telemetry hard.
     * The default is {@code false}.
     */
    @Value("${telemetryLockedOut:false}")
    private boolean telemetryLockedOut;

    /**
     * The anonymized flag.
     */
    @Value("${anonymized:false}")
    private boolean anonymized;

    @Value("${authHost:auth}")
    private String authHost;

    @Value("${serverGrpcPort:9001}")
    private int serverGrpcPort;

    @Value("${consulNamespace:}")
    private String consulNamespace;

    @Value("${enableConsulNamespace:false}")
    private boolean enableConsulNamespace;

    /**
     * The hardLock key.
     */
    public static final String TELEMETRY_LOCKED = "telemetry/hardlock";

    /**
     * The enabled key.
     */
    public static final String TELEMETRY_ENABLED = "telemetry/enabled";

    /**
     * The public key.
     */
    private static final String PUBLIC_KEY =
            "MIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKCAYEAwsnhpxQCco0aBwa9z1Xc7vpGa7Bgl5PMkD4jTD9a" +
            "VJBM4Y8VTRXHi2yt3qB7JRWFdlw0LwL2Pmk5pdQUMSDphITlio5nkY7NbAXMk8176nI8T9Y" +
            "/h9z7Ad6XZxV5DbgEDuCPmwUPep8Hi/JAwCaWnHFbf+Ql1xehUBHvlUq3hK2E9L/IBSqAWehNBYMfs" +
            "+V9AZweKCofDYDMAcshj/mFTM9ZNmvJnXqle+easaH25k9myLsNRPlij2P1X2HvFrqGlPpZi9WbrK3AP" +
            "/qtqzmreV3scKvlrzQRRD8z4ep4M1CyCOzVY6guGKPHzzCHkOV7w" +
            "/4URWVuZHPTPpicQbyY70mGtR8GTVn1UcyaIWCc27I2yYePRQ" +
            "//UmRakTbyByhkwwB3NbRUsxFWgeHAc8YxI9msdhliBa2R3b0rh4+fqrFI9DJc48u05L2bdD22mvr1StAl" +
            "+5l6GDQUrX09s3rU8JgZnOTY0ruj+GABnXfW7GT4L64llX64xbylJDGSjH1pAgMBAAE=";

    @Autowired
    private ComponentRegistrationConfig componentRegistrationConfig;

    /**
     * The {@link RequestMappingHandlerAdapter} bean.
     *
     * @return The object.
     */
    @Bean
    public RequestMappingHandlerAdapter requestMappingHandlerAdapter() {
        final RequestMappingHandlerAdapter adapter = new RequestMappingHandlerAdapter();
        adapter.setMessageConverters(ImmutableList.of(new MappingJackson2HttpMessageConverter(),
                                                      new ByteArrayHttpMessageConverter(),
                                                      new SourceHttpMessageConverter<>(),
                                                      new FormHttpMessageConverter(),
                                                      new StringHttpMessageConverter()));
        return adapter;
    }

    /**
     * The {@link ClusterMgrController} bean.
     *
     * @return The object.
     */
    @Bean
    public ClusterMgrController clusterMgrController() {
        return new ClusterMgrController(clusterMgrService());
    }

    /**
     * Declare the Spring GlobalExceptionHandler class to define special-case handling
     * for how different Java exceptions within a Controller execution are mapped to HTTP
     * responses.
     *
     * @return the GlobalExceptionHandler instance to configure controller error mapping
     */
    @Bean
    public ClusterMgrRestExceptionHandler globalExceptionHandler() {
        return new ClusterMgrRestExceptionHandler();
    }

    /**
     * The {@link DiagEnvironmentSummary} bean.
     *
     * @return The object.
     */
    @Bean
    public DiagEnvironmentSummary diagFileNameFormatter() {
        return new DiagEnvironmentSummary(BuildProperties.get(),
            Clock.systemUTC(), (host, port) -> GrpcChannelFactory.newChannelBuilder(host, port).build(), authHost, serverGrpcPort);
    }

    /**
     * The {@link ClusterMgrService} bean.
     *
     * @return The object.
     */
    @Bean
    public ClusterMgrService clusterMgrService() {
        return new ClusterMgrService(consulService(), osCommandProcessRunner(),
            diagFileNameFormatter(), componentRegistrationConfig.componentRegistry());
    }

    /**
     * The {@link LogConfigurationService} bean.
     *
     * @return The object.
     */
    @Bean
    public LogConfigurationService logConfigurationService() {
        return new LogConfigurationService();
    }

    /**
     * The {@link LogConfigurationServiceController} bean.
     *
     * @return The object.
     */
    @Bean
    public LogConfigurationServiceController logConfigurationServiceController() {
        return new LogConfigurationServiceController(logConfigurationService());
    }

    /**
     * The {@link TracingConfigurationRpcService} in the cluster manager.
     *
     * @return The service.
     */
    @Bean
    public TracingConfigurationRpcService tracingConfigurationRpcService() {
        return new TracingConfigurationRpcService(TracingManager.get());
    }

    /**
     * A {@link TracingConfigurationServiceController}.
     *
     * @return Controller for the tracing RPC service.
     */
    @Bean
    public TracingConfigurationServiceController tracingConfigurationServiceController() {
        return new TracingConfigurationServiceController(tracingConfigurationRpcService());
    }

    /**
     * The {@link ConsulService} bean.
     *
     * @return The object.
     */
    @Bean
    public ConsulService consulService() {
        return new ConsulService(consulHost, consulPort,
            ConsulKeyValueStore.constructNamespacePrefix(consulNamespace, enableConsulNamespace));
    }

    /**
     * The {@link DataAggregator} bean.
     *
     * @return The object.
     */
    @Bean
    public DataAggregator dataAggregator() {
        return new DataAggregator();
    }

    /**
     * The {@link TcpIpAggegatorReceiverBridge} bean.
     *
     * @return The object.
     * @throws IOException If there is an initialization error.
     */
    @Bean
    public TcpIpAggegatorReceiverBridge tcpIpAggegatorReceiverBridge() throws IOException {
        return new TcpIpAggegatorReceiverBridge(bridgePort, dataAggregator());
    }

    /**
     * The {@link OsCommandProcessRunner} bean.
     *
     * @return The object.
     */
    @Bean
    public OsCommandProcessRunner osCommandProcessRunner() {
        return new OsCommandProcessRunner();
    }

    /**
     * The {@link OsProcessFactory} bean.
     *
     * @return The object.
     */
    @Bean
    public OsProcessFactory scriptProcessFactory() {
        return new OsProcessFactory();
    }

    /**
     * Returns the value from consul or the OS environment, in case consul doesn't contain the
     * value.
     *
     * @param consulKey The key for consul key/value store.
     * @param envKey    The environment variable name.
     * @return The value.
     */
    private @Nonnull String getValue(final @Nonnull String consulKey,
                                     final @Nonnull String envKey) {
        String defaultValue = System.getenv(envKey);
        defaultValue = defaultValue == null ? "" : defaultValue;
        String value = consulService().getValueAsString(consulKey, defaultValue);
        return value == null ? "" : value;
    }

    /**
     * Generate the Base64-encoded SHA-1 digest of the str.
     * The result will be massaged to allow it to be a part of the file name.
     *
     * @param str The string to be digested.
     * @return The massaged Base64-encoded SHA-1 digest of the str.
     * @throws NoSuchAlgorithmException     In case SHA-1 is unsupported.
     */
    private @Nonnull String digest(final @Nonnull String str)
            throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] digest = md.digest(str.getBytes(StandardCharsets.UTF_8));
        return new String(Base64.getEncoder().encode(digest), StandardCharsets.UTF_8)
                .replace('/', '.')
                .replace('+', '-')
                .replace('=', '_');
    }

    @Bean
    DataCollectorFramework dataCollectorFramework() {
        // Lock out the telemetry if needed.
        // In case that happens, the flag in the docker-compose.yml has to be cleared,
        // and Opt-In flag has to be re-checked again to force the telemetry being re-enabled.
        // In addition to that, the consul value of telemetry/hardlock has to be removed.
        if (telemetryLockedOut) {
            consulService().putValue(TELEMETRY_LOCKED, "true");
        }

        LocalAggregatorBridge bridge = new LocalAggregatorBridge(dataAggregator());
        final DataCollectorFramework instance = DataCollectorFramework.instance();
        instance.setKeyValueCollector(
                () -> {
                    // In case we are hard locked, disallow collection.
                    String locked = consulService().getValueAsString(TELEMETRY_LOCKED, "false");
                    if (Boolean.parseBoolean(locked)) {
                        return false;
                    }
                    // Check the regular "enabled" flag.
                    String enabled = consulService().getValueAsString(TELEMETRY_ENABLED, "true");
                    return Boolean.parseBoolean(enabled);
                });
        instance.setAggregatorBridge(bridge);
        // Register metric only if we don't require an anonymized setup.
        if (!anonymized) {
            instance.registerMetric(new DataMetricLogs(clusterMgrService()));
        }
        instance.start(TimeUnit.SECONDS.toMillis(collectionIntervalUrgent),
                       TimeUnit.SECONDS.toMillis(collectionIntervalOffline));
        String instanceID = consulService().getValueAsString("instanceID", "-");
        if ("-".equals(instanceID)) {
            instanceID = UUID.randomUUID().toString();
            consulService().putValue("instanceID", instanceID);
        }
        String customerID = getValue("customer_id", "CUSTOMER_ID");
        String accessKey = getValue("access_key", "AWS_ACCESS_KEY_ID");
        String secretAccessKey = getValue("secret_access_key", "AWS_SECRET_ACCESS_KEY");
        String s3FilePrefix = null;
        try {
            if (!Strings.isNullOrEmpty(customerID) && !Strings.isNullOrEmpty(instanceID)) {
                s3FilePrefix = digest(customerID) + "_" + digest(instanceID);
            }
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 is unsupported", e);
        }
        DataTransfer transfer = new DataTransfer(TimeUnit.SECONDS.toMillis(dataForwardInterval),
                                                 dataAggregator(),
                                                 PUBLIC_KEY, s3FilePrefix, accessKey,
                                                 secretAccessKey, () -> {
            String enabled = consulService().getValueAsString(TELEMETRY_ENABLED, "true");
            return Boolean.parseBoolean(enabled);
        });
        transfer.start();
        return instance;
    }

    /**
     * We override the default path variable matcher in order to correctly interpret properties
     * including dots. Without this setting, property "prop.name" is treated as "prop" in
     * Spring MVC REST controllers
     *
     * @param matcher patch matcher configurer
     */
    @Override
    public void configurePathMatch(PathMatchConfigurer matcher) {
        matcher.setUseRegisteredSuffixPatternMatch(true);
    }

    /**
     * Add a new instance of the {@link GsonHttpMessageConverter} to the list of available {@link HttpMessageConverter}s in use.
     *
     * @param converters is the list of {@link HttpMessageConverter}s to which the new converter instance is added.
     */
    @Override
    public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {
        // Handle text-plain.
        final StringHttpMessageConverter stringMessageConverter =
            new StringHttpMessageConverter(Charset.forName("UTF-8"));
        converters.add(stringMessageConverter);

        // GSON for application-json serialization.
        final GsonHttpMessageConverter msgConverter = new GsonHttpMessageConverter();
        msgConverter.setGson(ComponentGsonFactory.createGson());

        converters.add(msgConverter);
    }
}
