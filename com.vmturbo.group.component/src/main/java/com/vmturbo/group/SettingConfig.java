package com.vmturbo.group;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.setting.SettingProtoREST.SettingPolicyServiceController;
import com.vmturbo.common.protobuf.setting.SettingProtoREST.SettingServiceController;
import com.vmturbo.group.persistent.DefaultSettingPolicyValidator;
import com.vmturbo.group.persistent.EntitySettingStore;
import com.vmturbo.group.persistent.FileBasedSettingsSpecStore;
import com.vmturbo.group.persistent.SettingPolicyValidator;
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

    @Value("${settingSpecJsonFile:setting/setting-spec.json}")
    private String settingSpecJsonFile;

    @Value("${createDefaultSettingPolicyRetryIntervalSec}")
    private long createDefaultSettingPolicyRetryIntervalSec;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Bean
    public FileBasedSettingsSpecStore settingSpecsSource() {
        return new FileBasedSettingsSpecStore(settingSpecJsonFile);
    }

    @Bean
    public SettingPolicyValidator settingPolicyValidator() {
        return new DefaultSettingPolicyValidator(settingSpecsSource(), arangoDBConfig.groupStore());
    }

    @Bean
    public SettingStore settingStore() {
        return new SettingStore(settingSpecsSource(), databaseConfig.dsl(),
                identityProviderConfig.identityProvider(), settingPolicyValidator(),
                createDefaultSettingPolicyRetryIntervalSec, TimeUnit.SECONDS);
    }

    @Bean
    public EntitySettingStore entitySettingStore() {
        return new EntitySettingStore(realtimeTopologyContextId, settingStore());
    }

    @Bean
    public SettingRpcService settingService() {
        return new SettingRpcService(settingSpecsSource());
    }

    @Bean
    public SettingServiceController settingServiceController() {
        return new SettingServiceController(settingService());
    }

    @Bean
    public SettingPolicyRpcService settingPolicyService() {
        return new SettingPolicyRpcService(settingStore(), settingSpecsSource(),
                entitySettingStore());
    }

    @Bean
    public SettingPolicyServiceController settingPolicyServiceController() {
        return new SettingPolicyServiceController(settingPolicyService());
    }
}
