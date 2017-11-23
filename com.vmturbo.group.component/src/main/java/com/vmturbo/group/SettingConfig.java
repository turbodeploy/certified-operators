package com.vmturbo.group;

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

import com.vmturbo.common.protobuf.setting.SettingProtoREST.SettingPolicyServiceController;
import com.vmturbo.common.protobuf.setting.SettingProtoREST.SettingServiceController;
import com.vmturbo.group.persistent.DefaultSettingPolicyCreator;
import com.vmturbo.group.persistent.DefaultSettingPolicyValidator;
import com.vmturbo.group.persistent.EntitySettingStore;
import com.vmturbo.group.persistent.EnumBasedSettingSpecStore;
import com.vmturbo.group.persistent.SettingPolicyValidator;
import com.vmturbo.group.persistent.SettingSpecStore;
import com.vmturbo.group.persistent.SettingStore;
import com.vmturbo.group.service.SettingPolicyRpcService;
import com.vmturbo.group.service.SettingRpcService;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({SQLDatabaseConfig.class, IdentityProviderConfig.class, ArangoDBConfig.class})
public class SettingConfig {

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private ArangoDBConfig arangoDBConfig;

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
        return new DefaultSettingPolicyValidator(settingSpecsStore(), arangoDBConfig.groupStore());
    }

    @Bean
    public SettingStore settingStore() {
        return new SettingStore(settingSpecsStore(), databaseConfig.dsl(),
                identityProviderConfig.identityProvider(), settingPolicyValidator());
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
    public EntitySettingStore entitySettingStore() {
        return new EntitySettingStore(realtimeTopologyContextId, settingStore());
    }

    @Bean
    public SettingRpcService settingService() {
        return new SettingRpcService(settingSpecsStore());
    }

    @Bean
    public SettingServiceController settingServiceController() {
        return new SettingServiceController(settingService());
    }

    @Bean
    public SettingPolicyRpcService settingPolicyService() {
        return new SettingPolicyRpcService(settingStore(), settingSpecsStore(),
                entitySettingStore());
    }

    @Bean
    public SettingPolicyServiceController settingPolicyServiceController() {
        return new SettingPolicyServiceController(settingPolicyService());
    }
}
