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
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.common.protobuf.schedule.ScheduleServiceGrpc;
import com.vmturbo.common.protobuf.schedule.ScheduleServiceGrpc.ScheduleServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceStub;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.graph.search.filter.TopologyFilterFactory;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.group.discovery.DiscoveredClusterConstraintCache;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;
import com.vmturbo.topology.processor.group.policy.PolicyManager;
import com.vmturbo.topology.processor.group.policy.application.PolicyApplicator;
import com.vmturbo.topology.processor.group.policy.application.PolicyFactory;
import com.vmturbo.topology.processor.group.settings.EntitySettingsApplicator;
import com.vmturbo.topology.processor.group.settings.EntitySettingsResolver;
import com.vmturbo.topology.processor.targets.TargetConfig;
import com.vmturbo.topology.processor.topology.TopologyInvertedIndexFactory;

/**
 * The configuration for dealing with groups.
 */
@Configuration
@Import({EntityConfig.class,
    GroupClientConfig.class,
    PlanOrchestratorClientConfig.class,
    ActionOrchestratorClientConfig.class,
    TargetConfig.class})
public class GroupConfig {

    @Autowired
    private EntityConfig entityConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private PlanOrchestratorClientConfig planClientConfig;

    @Autowired
    private ActionOrchestratorClientConfig actionOrchestratorClientConfig;

    @Autowired
    private TargetConfig targetConfig;

    @Value("${discoveredGroupUploadIntervalSeconds}")
    private long discoveredGroupUploadIntervalSeconds;

    /**
     * Size of chunks for uploading entity settings to the group component.
     */
    @Value("${entitySettingsChunksSize}")
    private int entitySettingsChunksSize;

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
    public ScheduleServiceBlockingStub scheduleServiceClient() {
        return ScheduleServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    /**
     * Async stub for the setting policy.
     * @return Async Setting Policy Service client.
     */
    @Bean
    public SettingPolicyServiceStub settingServiceClientAsync() {
        return SettingPolicyServiceGrpc.newStub(groupClientConfig.groupChannel());
    }

    @Bean
    public TopologyFilterFactory<TopologyEntity> topologyFilterFactory() {
        return new TopologyFilterFactory<>();
    }

    @Bean
    public SearchResolver<TopologyEntity> searchResolver() {
        return new SearchResolver<>(topologyFilterFactory());
    }

    @Bean
    public PolicyManager policyManager() {
        return new PolicyManager(policyRpcService(), groupServiceBlockingStub(), policyFactory(),
            policyApplicator());
    }

    /**
     * Factory class for policies.
     *
     * @return The {@link PolicyFactory}.
     */
    @Bean
    public PolicyFactory policyFactory() {
        return new PolicyFactory(new TopologyInvertedIndexFactory());
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
                    settingServiceClient(),
                    settingServiceClientAsync(),
                    scheduleServiceClient(),
                    entitySettingsChunksSize);
    }

    @Bean
    public DiscoveredGroupUploader discoveredGroupUploader() {
        return new DiscoveredGroupUploader(groupServiceStub(), entityConfig.entityStore(),
                discoveredClusterConstraintCache(), targetConfig.targetStore());
    }


    @Bean
    public DiscoveredClusterConstraintCache discoveredClusterConstraintCache() {
        return new DiscoveredClusterConstraintCache(entityConfig.entityStore());
    }
}
