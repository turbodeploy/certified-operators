package com.vmturbo.topology.processor.group;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceStub;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.group.discovery.DiscoveredClusterConstraintCache;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.group.filter.TopologyFilterFactory;
import com.vmturbo.topology.processor.group.policy.application.PolicyApplicator;
import com.vmturbo.topology.processor.group.policy.application.PolicyFactory;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.group.policy.ReservationPolicyFactory;
import com.vmturbo.topology.processor.group.settings.EntitySettingsApplicator;
import com.vmturbo.topology.processor.group.settings.EntitySettingsResolver;
import com.vmturbo.topology.processor.plan.PlanConfig;

/**
 * The configuration for dealing with groups.
 */
@Configuration
@Import({EntityConfig.class, GroupClientConfig.class, PlanConfig.class,
        ActionOrchestratorClientConfig.class})
public class GroupConfig {

    @Autowired
    private EntityConfig entityConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private PlanConfig planConfig;

    @Autowired
    private ActionOrchestratorClientConfig actionOrchestratorClientConfig;

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

    /**
     *
     * @return Blocking Group Service client.
     */
    @Bean
    public GroupServiceBlockingStub groupServiceBlockingStub() {
        return GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    /**
     *
     * @return Async Group Service client.
     */
    @Bean
    public GroupServiceStub groupServiceStub() {
        return GroupServiceGrpc.newStub(groupClientConfig.groupChannel());
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
        return new PolicyManager(policyRpcService(), groupServiceBlockingStub(), policyFactory(),
            initialPlacementPolicyFactory(), planConfig.reservationServiceBlockingStub(),
            policyApplicator());
    }

    @Bean
    public PolicyFactory policyFactory() {
        return new PolicyFactory();
    }

    @Bean
    public PolicyApplicator policyApplicator() {
        return new PolicyApplicator(policyFactory());
    }

    @Bean
    public EntitySettingsApplicator entitySettingsApplicator() {
        return new EntitySettingsApplicator();
    }

    @Bean
    public EntitySettingsResolver settingsManager() {
        return new EntitySettingsResolver(settingPolicyServiceClient(),
                    groupServiceBlockingStub(),
                    settingServiceClient());
    }

    @Bean
    public DiscoveredGroupUploader discoveredGroupUploader() {
        return new DiscoveredGroupUploader(groupServiceStub(),
                entityConfig.entityStore(), discoveredClusterConstraintCache());
    }

    @Bean
    public ReservationPolicyFactory initialPlacementPolicyFactory() {
        return new ReservationPolicyFactory(groupServiceBlockingStub());
    }

    @Bean
    public DiscoveredClusterConstraintCache discoveredClusterConstraintCache() {
        return new DiscoveredClusterConstraintCache(entityConfig.entityStore());
    }
}
