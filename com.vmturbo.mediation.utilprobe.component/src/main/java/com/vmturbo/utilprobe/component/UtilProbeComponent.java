package com.vmturbo.utilprobe.component;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.kvstore.ConsulKeyValueStore;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.mediation.client.MediationComponentMain;
import com.vmturbo.mediation.common.ProbeConfigurationLoadException;
import com.vmturbo.mediation.common.ProbeProperties;
import com.vmturbo.mediation.common.accounts.AccountValuesConverter;
import com.vmturbo.mediation.common.accounts.FieldsBasedAccountValuesConverter;
import com.vmturbo.mediation.common.features.ProbeClassContext;
import com.vmturbo.mediation.utilprobe.common.UtilProbe;
import com.vmturbo.mediation.utilprobe.common.UtilProbeAccount;
import com.vmturbo.platform.sdk.common.MediationMessage;
import com.vmturbo.utilprobe.component.service.ConsulManagementService;

/**
 * Component for UtilProbe.
 */
@Configuration
@Import({MediationComponentMain.class})
class UtilProbeComponent extends MediationComponentMain {

    private static final Logger logger = LogManager.getLogger(UtilProbeComponent.class);

    private static final String PROBE_TYPE = "UTILPROBE_TYPE";
    private static final String PROBE_CATEGORY = "UTILPROBE_CATEGORY";

    @Value("${consul_host:}")
    private String consulHost;
    @Value("${consul_port:}")
    private String consulPort;
    @Value("${topologyProcessorInstance:}")
    private String topologyProcessorInstance;
    @Value("${kvStoreRetryIntervalMillis:1000}")
    private String kvStoreRetryIntervalMillis;
    @Value("${probe-directory:probe-jars}")
    private File probeDirectory;

    @Autowired
    private ConsulManagementService consulManagementService;

    static void start() {
        startContext(UtilProbeComponent.class);
    }

    /**
     * Creating Probe Properties Collection.
     *
     * @return ProbeProperties collection
     * @throws ProbeConfigurationLoadException
     *      Exception while loading probe configuration
     */
    @Override
    @Bean
    public Collection<ProbeProperties<?>> probePropertiesCollection() throws ProbeConfigurationLoadException {
        final String probeType = System.getenv(PROBE_TYPE);
        final String probeCategory = System.getenv(PROBE_CATEGORY);

        logger.info("Probe type: {}\nProbe category: {}", probeType, probeCategory);

        Collection<ProbeProperties<?>> newProps = new ArrayList<>();
        ProbeClassContext<UtilProbeAccount> probeClassContext =
                new ProbeClassContext(Thread.currentThread().getContextClassLoader(), UtilProbe.class);
        AccountValuesConverter<UtilProbeAccount> accountValuesConverter =
                new FieldsBasedAccountValuesConverter<>(UtilProbeAccount.class);
        logger.info("Account definitions: {}\nTargetId fields: {}",
                accountValuesConverter.getAccountDefinitions().toString(),
                accountValuesConverter.getTargetIdFields().toString());

        // Pass probe category and probe type from the parameters
        newProps.add(new ProbeProperties(
                probeCategory,
                probeType,
                probeClassContext,
                accountValuesConverter,
                probeDirectory,
                ProbeProperties.DEFAULT_REDISCOVERY_INTERVAL_IN_SEC,
                ProbeProperties.DEFAULT_INCREMENTAL_REDISCOVERY_INTERVAL_IN_SEC,
                ProbeProperties.MINIMUM_PERFORMANCE_REDISCOVERY_INTERVAL_IN_SEC,
                ProbeProperties.DEFAULT_PROBE_TIMEOUT_SEC,
                MediationMessage.ProbeInfo.CreationMode.STAND_ALONE));
        return newProps;
    }

    /**
     * Initializing key/value store.
     *
     * @return key/value store for topology processor
     */
    @Bean
    protected KeyValueStore getKeyValueStore() {
        String logMessage = new StringBuilder()
                .append("Initializing KV store:\n")
                .append("Namespace: {}\n")
                .append("Consul host: {}\n")
                .append("Consul port: {}\n")
                .append("Retry interval: {} ms\n")
                .toString();
        logger.info(logMessage, topologyProcessorInstance, consulHost,
                consulPort, kvStoreRetryIntervalMillis);
        return new ConsulKeyValueStore(topologyProcessorInstance,
                this.consulHost,
                this.consulPort,
                Long.parseLong(kvStoreRetryIntervalMillis),
                TimeUnit.MILLISECONDS);
    }

    /**
     * Initializing key/value manager service.
     *
     * @return key value manager service
     */
    @Bean
    public ConsulManagementService getKeyValueManagerService() {
        return new ConsulManagementService(getKeyValueStore());
    }

    /**
     * Operations after context constructing.
     */
    @PostConstruct
    @Override
    public void componentContextConstructed() {
        super.componentContextConstructed();
        consulManagementService.deleteConfig(System.getenv(PROBE_CATEGORY),
                System.getenv(PROBE_TYPE));
    }

    /**
     * Operations before context closing.
     */
    @PreDestroy
    public void componentContextClosing() {
        consulManagementService.deleteConfig(System.getenv(PROBE_CATEGORY),
                System.getenv(PROBE_TYPE));
        super.componentContextClosing();
    }

}