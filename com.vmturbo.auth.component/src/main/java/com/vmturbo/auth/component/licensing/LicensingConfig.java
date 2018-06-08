package com.vmturbo.auth.component.licensing;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.auth.component.AuthKVConfig;
import com.vmturbo.auth.component.RepositoryClientConfig;
import com.vmturbo.auth.component.licensing.store.ILicenseStore;
import com.vmturbo.auth.component.licensing.store.LicenseKVStore;
import com.vmturbo.auth.component.services.LicenseController;
import com.vmturbo.common.protobuf.repository.RepositoryNotificationDTO.RepositoryNotification;
import com.vmturbo.components.api.client.BaseKafkaConsumerConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.repository.api.Repository;
import com.vmturbo.repository.api.impl.RepositoryNotificationReceiver;

/**
 * Spring configuration for Auth Licensing-related services.
 */
@Configuration
@Import({AuthKVConfig.class, RepositoryClientConfig.class})
public class LicensingConfig {

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private AuthKVConfig authKVConfig;

    @Bean
    public ILicenseStore licenseStore() {
        return new LicenseKVStore(authKVConfig.authKeyValueStore());
    }

    @Bean
    public LicenseController licenseController() {
        return new LicenseController(licenseStore());
    }

    @Bean
    public LicenseManagerService licenseManager() {
        return new LicenseManagerService(licenseStore());
    }

    @Bean
    public LicenseCheckService licenseCheckService() {
        return new LicenseCheckService(licenseManager(),
                repositoryClientConfig.searchServiceClient(),
                repositoryClientConfig.repositoryListener());
    }

}
