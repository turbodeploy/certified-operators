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
import com.vmturbo.group.GroupComponentDBConfig;
import com.vmturbo.group.IdentityProviderConfig;
import com.vmturbo.group.api.SettingMessages.SettingNotification;
import com.vmturbo.group.api.SettingsUpdatesReciever;
import com.vmturbo.group.group.GroupConfig;
import com.vmturbo.plan.orchestrator.api.impl.PlanGarbageDetector;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;

@Configuration
@Import({GroupComponentDBConfig.class, IdentityProviderConfig.class, GroupConfig.class,
        BaseKafkaProducerConfig.class, PlanOrchestratorClientConfig.class})
public class SettingConfig {

    @Autowired
    private GroupComponentDBConfig databaseConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private GroupConfig groupConfig;

    @Autowired
    private PlanOrchestratorClientConfig planOrchestratorClientConfig;

    @Value("${createDefaultSettingPolicyRetryIntervalSec}")
    public long createDefaultSettingPolicyRetryIntervalSec;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    /**
     * Feature flag. If it is true then ExecutionSchedule settings are not displayed in UI.
     */
    @Value("${hideExecutionScheduleSetting:true}")
    private boolean hideExecutionScheduleSetting;

    /**
     * Feature flag. If it is true then ExternalApproval settings are not displayed in UI.
     */
    @Value("${hideExternalApprovalOrAuditSettings:true}")
    private boolean hideExternalApprovalOrAuditSettings;

    @Bean
    public SettingSpecStore settingSpecsStore() {
        return new EnumBasedSettingSpecStore(hideExecutionScheduleSetting,
            hideExternalApprovalOrAuditSettings);
    }

    @Bean
    public SettingPolicyValidator settingPolicyValidator() {
        return new DefaultSettingPolicyValidator(settingSpecsStore(), groupConfig.groupStore());
    }

    @Bean
    public SettingStore settingStore() {
        return new SettingStore(settingSpecsStore(),
                databaseConfig.dsl(),
                settingPolicyValidator(),
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


    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService settingsCreatorThreadPool() {
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

    /**
     * Listener for plan deletion.
     *
     * @return The listener.
     */
    @Bean
    public PlanGarbageDetector settingPlanGarbageDetector() {
        final SettingPlanGarbageCollector collector = new SettingPlanGarbageCollector(settingStore());
        return planOrchestratorClientConfig.newPlanGarbageDetector(collector);
    }
}
