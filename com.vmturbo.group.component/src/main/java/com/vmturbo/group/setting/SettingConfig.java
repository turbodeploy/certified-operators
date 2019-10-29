package com.vmturbo.group.setting;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.group.IdentityProviderConfig;
import com.vmturbo.group.api.SettingMessages.SettingNotification;
import com.vmturbo.group.api.SettingsUpdatesReciever;
import com.vmturbo.group.group.GroupConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({SQLDatabaseConfig.class, IdentityProviderConfig.class, GroupConfig.class, BaseKafkaProducerConfig.class})
public class SettingConfig {

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private GroupConfig groupConfig;

    @Value("${createDefaultSettingPolicyRetryIntervalSec}")
    private long createDefaultSettingPolicyRetryIntervalSec;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Bean
    public SettingSpecStore settingSpecsStore() {
        return new EnumBasedSettingSpecStore();
    }

    @Bean
    public SettingPolicyValidator settingPolicyValidator() {
        return new DefaultSettingPolicyValidator(settingSpecsStore(), groupConfig.groupStore());
    }

    @Bean
    public SettingStore settingStore() {
        return new SettingStore(settingSpecsStore(),
                databaseConfig.dsl(),
                identityProviderConfig.identityProvider(),
                settingPolicyValidator(),
                groupConfig.groupStore(),
                new SettingsUpdatesSender(settingMessageSender()));
    }

    /**
     * BaseKafkaProducerConfig autowired reference.
     */
    @Autowired
    private BaseKafkaProducerConfig baseKafkaProducerConfig;

    @Bean
    public IMessageSender<SettingNotification> settingMessageSender() {
        return baseKafkaProducerConfig.kafkaMessageSender()
                .messageSender(SettingsUpdatesReciever.SETTINGS_UPDATES_TOPIC);
    }

    @Bean
    public DefaultSettingPolicyCreator defaultSettingPoliciesCreator() {
        final DefaultSettingPolicyCreator creator =
                new DefaultSettingPolicyCreator(settingSpecsStore(), settingStore(),
                        TimeUnit.SECONDS.toMillis(createDefaultSettingPolicyRetryIntervalSec));
        /*
         * Asynchronously create the default setting policies.
         * This is asynchronous so that DB availability doesn't prevent the group component from
         * starting up.
         */
        settingsCreatorThreadPool().execute(creator);
        return creator;
    }

    @Bean(destroyMethod = "shutdownNow")
    protected ExecutorService settingsCreatorThreadPool() {
        final ThreadFactory tf =
                new ThreadFactoryBuilder().setNameFormat("default-settings-creator-%d").build();
        return Executors.newCachedThreadPool(tf);
    }

    @Bean
    public DefaultGlobalSettingsCreator defaultGlobalSettingsCreator() {
        // Use the same retry interval as the one used for entity settings.
        final DefaultGlobalSettingsCreator creator =
                new DefaultGlobalSettingsCreator(settingSpecsStore(), settingStore(),
                        TimeUnit.SECONDS.toMillis(createDefaultSettingPolicyRetryIntervalSec));

        Thread t = new Thread(creator);
        t.setName("default-global-settings-creator");
        t.start();
        return creator;
    }

    @Bean
    public EntitySettingStore entitySettingStore() {
        return new EntitySettingStore(realtimeTopologyContextId, settingStore());
    }
}
