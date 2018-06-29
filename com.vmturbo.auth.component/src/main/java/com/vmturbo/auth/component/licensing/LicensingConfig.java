package com.vmturbo.auth.component.licensing;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.auth.component.AuthKVConfig;
import com.vmturbo.auth.component.RepositoryClientConfig;
import com.vmturbo.auth.component.licensing.LicenseCheckService.LicenseSummaryPublisher;
import com.vmturbo.auth.component.licensing.store.ILicenseStore;
import com.vmturbo.auth.component.licensing.store.LicenseKVStore;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.common.health.KafkaProducerHealthMonitor;

/**
 * Spring configuration for Auth Licensing-related services.
 */
@Configuration
@Import({AuthKVConfig.class, RepositoryClientConfig.class, BaseKafkaProducerConfig.class})
public class LicensingConfig {

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private AuthKVConfig authKVConfig;

    @Autowired
    private BaseKafkaProducerConfig kafkaProducerConfig;

    @Bean
    public ILicenseStore licenseStore() {
        return new LicenseKVStore(authKVConfig.authKeyValueStore());
    }

    @Bean
    public KafkaProducerHealthMonitor kafkaProducerHealthMonitor() {
        return new KafkaProducerHealthMonitor(kafkaProducerConfig.kafkaMessageSender());
    }

    @Bean
    public IMessageSender<LicenseSummary> licenseSummaryPublisher() {
        return kafkaProducerConfig.kafkaMessageSender()
                .messageSender(LicenseCheckClient.LICENSE_SUMMARY_TOPIC,
                        LicenseSummaryPublisher::generateMessageKey);
    }

/*
    @Bean
    public LicenseController licenseController() {
        return new LicenseController(licenseStore());
    }
    */

    @Bean
    public LicenseManagerService licenseManager() {
        return new LicenseManagerService(licenseStore());
    }

    @Bean
    public LicenseCheckService licenseCheckService() {
        return new LicenseCheckService(licenseManager(),
                repositoryClientConfig.searchServiceClient(),
                repositoryClientConfig.repositoryListener(),
                licenseSummaryPublisher());
    }

}
