package com.vmturbo.cost.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.group.api.GroupClientConfig;

/**
 * Cost component config for external component service communication.
 */
@Configuration
@Import({GroupClientConfig.class})
public class CommunicationConfig {

    @Autowired
    private GroupClientConfig groupClientConfig;

    /**
     * Creates a new {@link SettingServiceBlockingStub} instance.
     * @return The new {@link SettingServiceBlockingStub} instance.
     */
    @Bean
    public SettingServiceBlockingStub settingRpcService() {
        return SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }
}
