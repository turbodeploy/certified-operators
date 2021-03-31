package com.vmturbo.auth.component.licensing;

import java.time.Clock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
import com.vmturbo.common.protobuf.licensing.LicensingREST.LicenseManagerServiceController;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.common.health.MessageProducerHealthMonitor;
import com.vmturbo.components.common.mail.MailManager;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.notification.api.NotificationApiConfig;

/**
 * Spring configuration for Auth Licensing-related services.
 */
@Configuration
@Import({AuthKVConfig.class, RepositoryClientConfig.class, BaseKafkaProducerConfig.class,
        GroupClientConfig.class, NotificationApiConfig.class})
public class LicensingConfig {

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private AuthKVConfig authKVConfig;

    @Autowired
    private BaseKafkaProducerConfig kafkaProducerConfig;

    @Autowired
    private NotificationApiConfig notificationApiConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    /**
     * The number of days before sending license expiration warning.
     */
    @Value("${numBeforeLicenseExpirationDays:2}")
    private int numBeforeLicenseExpirationDays;

    @Value("${allowedMaximumEditor:1}")
    private int allowedMaximumEditor;

    @Bean
    public ILicenseStore licenseStore() {
        return new LicenseKVStore(authKVConfig.authKeyValueStore());
    }

    @Bean
    public MessageProducerHealthMonitor kafkaProducerHealthMonitor() {
        return new MessageProducerHealthMonitor(kafkaProducerConfig.kafkaMessageSender());
    }

    @Bean
    public IMessageSender<LicenseSummary> licenseSummaryPublisher() {
        return kafkaProducerConfig.kafkaMessageSender()
                .messageSender(LicenseCheckClient.LICENSE_SUMMARY_TOPIC,
                        LicenseSummaryPublisher::generateMessageKey);
    }


    /**
     * Bean to enable licenseManagerService
     * @return
     */
    @Bean
    public LicenseManagerServiceController licenseManagerServiceController() {
        return new LicenseManagerServiceController(licenseManager());
    }

    /**
     * License manager.
     * @return License manager bean.
     */
    @Bean
    public LicenseManagerService licenseManager() {
        return new LicenseManagerService(licenseStore(), notificationApiConfig.notificationMessageSender());
    }

    @Bean
    public LicensedEntitiesCountCalculator licensedEntitiesCountCalculator() {
        return new LicensedEntitiesCountCalculator(repositoryClientConfig.searchServiceClient());
    }

    /**
     * The {@link LicenseCheckService}.
     *
     * @return The {@link LicenseCheckService}.
     */
    @Bean
    public LicenseCheckService licenseCheckService() {
        return new LicenseCheckService(licenseManager(),
                SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel()),
                licensedEntitiesCountCalculator(),
                repositoryClientConfig.repositoryListener(),
                licenseSummaryPublisher(),
                notificationApiConfig.notificationMessageSender(),
                mailManager(),
                Clock.systemUTC(),
                numBeforeLicenseExpirationDays,
                true,
                allowedMaximumEditor);
    }

    @Bean
    public MailManager mailManager() {
        return new MailManager(SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel()));
    }

}
