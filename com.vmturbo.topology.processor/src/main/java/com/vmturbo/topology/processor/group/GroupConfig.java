package com.vmturbo.topology.processor.group;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.group.filter.TopologyFilterFactory;
import com.vmturbo.topology.processor.group.policy.ReservationPolicyFactory;
import com.vmturbo.topology.processor.group.policy.PolicyFactory;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.group.settings.EntitySettingsApplicator;
import com.vmturbo.topology.processor.group.settings.EntitySettingsResolver;
import com.vmturbo.topology.processor.plan.PlanConfig;

/**
 * The configuration for dealing with groups.
 */
@Configuration
@Import({EntityConfig.class, GroupClientConfig.class, PlanConfig.class})
public class GroupConfig {

    @Autowired
    private EntityConfig entityConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private PlanConfig planConfig;

    @Value("${discoveredGroupUploadIntervalSeconds}")
    private long discoveredGroupUploadIntervalSeconds;

    @Bean
    public PolicyServiceBlockingStub policyRpcService() {
        return PolicyServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    @Bean
    public SettingPolicyServiceBlockingStub settingPolicyServiceClient() {
        return SettingPolicyServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    @Bean
    public GroupServiceBlockingStub groupServiceClient() {
        return GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    @Bean
    public SettingServiceBlockingStub settingServiceClient() {
        return SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    @Bean
    public TopologyFilterFactory topologyFilterFactory() {
        return new TopologyFilterFactory();
    }

    @Bean
    public PolicyManager policyManager() {
        return new PolicyManager(policyRpcService(), groupServiceClient(), new PolicyFactory(),
                initialPlacementPolicyFactory(), planConfig.reservationServiceBlockingStub());
    }

    @Bean
    public EntitySettingsApplicator entitySettingsApplicator() {
        return new EntitySettingsApplicator();
    }

    @Bean
    public EntitySettingsResolver settingsManager() {
        return new EntitySettingsResolver(settingPolicyServiceClient(),
                    groupServiceClient(),
                    settingServiceClient());
    }

    @Bean
    public DiscoveredGroupUploader discoveredGroupUploader() {
        return new DiscoveredGroupUploader(groupClientConfig.groupChannel(),
                entityConfig.entityStore());
    }

    @Bean
    public ReservationPolicyFactory initialPlacementPolicyFactory() {
        return new ReservationPolicyFactory(groupServiceClient());
    }
}
