package com.vmturbo.utilprobe.component;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.mediation.client.MediationComponentMain;
import com.vmturbo.mediation.common.ProbeConfigurationLoadException;
import com.vmturbo.mediation.common.ProbeProperties;
import com.vmturbo.mediation.common.accounts.AccountValuesConverter;
import com.vmturbo.mediation.common.accounts.FieldsBasedAccountValuesConverter;
import com.vmturbo.mediation.common.features.ProbeClassContext;
import com.vmturbo.mediation.utilprobe.common.UtilProbe;
import com.vmturbo.mediation.utilprobe.common.UtilProbeAccount;
import com.vmturbo.platform.sdk.common.MediationMessage;

/**
 * Component for UtilProbe.
 */
@Configuration
@Import({MediationComponentMain.class})
class UtilProbeComponent extends MediationComponentMain {

    private static final Logger logger = LogManager.getLogger(UtilProbeComponent.class);

    private static final String PROBE_TYPE = "UTILPROBE_TYPE";
    private static final String PROBE_CATEGORY = "UTILPROBE_CATEGORY";

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

        logger.info("Probe type: " + probeType);
        logger.info("Probe category: " + probeCategory);

        Collection<ProbeProperties<?>> newProps = new ArrayList<>();
        ProbeClassContext<UtilProbeAccount> probeClassContext =
                new ProbeClassContext<>(Thread.currentThread().getContextClassLoader(), UtilProbe.class);
        AccountValuesConverter<UtilProbeAccount> accountValuesConverter =
                new FieldsBasedAccountValuesConverter<>(UtilProbeAccount.class);
        logger.info("Account definitions: " + accountValuesConverter.getAccountDefinitions().toString());
        logger.info("TargetId fields: " + accountValuesConverter.getTargetIdFields().toString());

        // Pass probe category and probe type from the parameters
        newProps.add(new ProbeProperties(
                probeCategory,
                probeType,
                probeClassContext,
                accountValuesConverter,
                ProbeProperties.DEFAULT_REDISCOVERY_INTERVAL_IN_SEC,
                ProbeProperties.DEFAULT_INCREMENTAL_REDISCOVERY_INTERVAL_IN_SEC,
                ProbeProperties.MINIMUM_PERFORMANCE_REDISCOVERY_INTERVAL_IN_SEC,
                ProbeProperties.DEFAULT_PROBE_TIMEOUT_SEC,
                MediationMessage.ProbeInfo.CreationMode.STAND_ALONE));
        return newProps;
    }

}