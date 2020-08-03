package com.vmturbo.group.service;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.auth.api.authorization.UserSessionConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupDTOREST.GroupServiceController;
import com.vmturbo.common.protobuf.group.PolicyDTOREST.PolicyServiceController;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionREST.TopologyDataDefinitionServiceController;
import com.vmturbo.common.protobuf.schedule.ScheduleProtoREST.ScheduleServiceController;
import com.vmturbo.common.protobuf.search.TargetSearchServiceGrpc;
import com.vmturbo.common.protobuf.search.TargetSearchServiceGrpc.TargetSearchServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProtoREST.SettingPolicyServiceController;
import com.vmturbo.common.protobuf.setting.SettingProtoREST.SettingServiceController;
import com.vmturbo.group.GroupComponentDBConfig;
import com.vmturbo.group.IdentityProviderConfig;
import com.vmturbo.group.group.GroupConfig;
import com.vmturbo.group.policy.DiscoveredPlacementPolicyUpdater;
import com.vmturbo.group.policy.PolicyConfig;
import com.vmturbo.group.schedule.ScheduleConfig;
import com.vmturbo.group.setting.DefaultSettingPolicyCreator;
import com.vmturbo.group.setting.DiscoveredSettingPoliciesUpdater;
import com.vmturbo.group.setting.SettingConfig;
import com.vmturbo.group.stitching.GroupStitchingManager;
import com.vmturbo.group.topologydatadefinition.TopologyDataDefinitionConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;

@Configuration
@Import({ActionOrchestratorClientConfig.class,
        GroupComponentDBConfig.class,
        GroupConfig.class,
        IdentityProviderConfig.class,
        PolicyConfig.class,
        RepositoryClientConfig.class,
        SettingConfig.class,
        ScheduleConfig.class,
        UserSessionConfig.class,
        TopologyProcessorClientConfig.class,
        TopologyDataDefinitionConfig.class})
public class RpcConfig {

    @Value("${groupRetrievePermitsSize:300000}")
    private int groupRetrievePermitsSize;

    @Value("${groupLoadTimoutSec:30}")
    private long groupLoadTimeoutSec;

    @Autowired
    private GroupComponentDBConfig databaseConfig;

    @Autowired
    private GroupConfig groupConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private PolicyConfig policyConfig;

    @Autowired
    private SettingConfig settingConfig;

    @Autowired
    private ScheduleConfig scheduleConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private ActionOrchestratorClientConfig aoClientConfig;

    @Autowired
    private UserSessionConfig userSessionConfig;

    @Autowired
    private TopologyProcessorClientConfig topologyProcessorClientConfig;

    @Autowired
    private TopologyDataDefinitionConfig topologyDataDefinitionConfig;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${entitySettingsResponseChunkSize:20}")
    private int entitySettingsResponseChunkSize;

    @Bean
    public PolicyRpcService policyService() {
        return new PolicyRpcService(policyConfig.policyStore(), groupService(),
                groupConfig.groupStore(), userSessionConfig.userSessionContext());
    }

    @Bean
    public PolicyServiceController policyServiceController(final PolicyRpcService policyRpcService) {
        return new PolicyServiceController(policyRpcService);
    }

    @Bean
    public TransactionProvider transactionProvider() {
        return new TransactionProviderImpl(settingConfig.settingStore(), databaseConfig.dsl(),
                identityProviderConfig.identityProvider());
    }

    /**
     * Group gRPC service bean.
     *
     * @return group service bean
     */
    @Bean
    public GroupRpcService groupService() {
        return new GroupRpcService(groupConfig.temporaryGroupCache(),
                repositoryClientConfig.searchServiceClient(),
                userSessionConfig.userSessionContext(), groupStitchingManager(),
                transactionProvider(), identityProviderConfig.identityProvider(),
                targetSearchService(), settingsPoliciesUpdater(), placementPolicyUpdater(),
                groupRetrievePermitsSize, groupLoadTimeoutSec);
    }

    /**
     * Setting policies updater to process incoming discovered setting policies.
     *
     * @return setting policies updater
     */
    @Bean
    public DiscoveredSettingPoliciesUpdater settingsPoliciesUpdater() {
        return new DiscoveredSettingPoliciesUpdater(identityProviderConfig.identityProvider());
    }

    /**
     * Placement policies updater to process incoming discovered placement policies.
     *
     * @return placement policies updater
     */
    @Bean
    public DiscoveredPlacementPolicyUpdater placementPolicyUpdater() {
        return new DiscoveredPlacementPolicyUpdater(identityProviderConfig.identityProvider());
    }

    /**
     * Target search service.
     *
     * @return target search service
     */
    @Bean
    public TargetSearchServiceBlockingStub targetSearchService() {
        return TargetSearchServiceGrpc.newBlockingStub(
                topologyProcessorClientConfig.topologyProcessorChannel());
    }
    /**
     * An instance of group stitching manager.
     *
     * @return {@link GroupStitchingManager} bean
     */
    @Bean
    public GroupStitchingManager groupStitchingManager() {
        return new GroupStitchingManager(identityProviderConfig.identityProvider());
    }

    @Bean
    public GroupServiceController groupServiceController(final GroupRpcService groupRpcService) {
        return new GroupServiceController(groupRpcService);
    }

    @Bean
    public SettingRpcService settingService() {
        return new SettingRpcService(settingConfig.settingSpecsStore(),
                settingConfig.settingStore());
    }

    @Bean
    public SettingServiceController settingServiceController() {
        return new SettingServiceController(settingService());
    }

    @Bean
    public ActionsServiceBlockingStub actionsRpcService() {
        return ActionsServiceGrpc.newBlockingStub(
                aoClientConfig.actionOrchestratorChannel())
                // Intercept client call and add JWT token to the metadata
                .withInterceptors(new JwtClientInterceptor());
    }

    @Bean
    public SettingPolicyRpcService settingPolicyService() {
        return new SettingPolicyRpcService(settingConfig.settingStore(),
                settingConfig.settingSpecsStore(),
                settingConfig.entitySettingStore(),
                actionsRpcService(),
                identityProviderConfig.identityProvider(),
                transactionProvider(),
                realtimeTopologyContextId, entitySettingsResponseChunkSize);
    }

    /**
     * Service for CRUD operations on
     * {@link com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition}.
     *
     * @return an instance of the service.
     */
    @Bean
    public TopologyDataDefinitionRpcService topologyDataDefinitionRpcService() {
        return new TopologyDataDefinitionRpcService(
            topologyDataDefinitionConfig.topologyDataDefinitionStore());
    }

    /**
     * Controller for TopologyDataDefinitionRpcService.
     *
     * @param topologyDataDefinitionRpcService service needed by controller constructor.
     * @return TopologyDataDefinitionServiceController needed to service grpc calls.
     */
    @Bean
    public TopologyDataDefinitionServiceController topologyDataDefinitionServiceController(
        TopologyDataDefinitionRpcService topologyDataDefinitionRpcService) {
        return new TopologyDataDefinitionServiceController(topologyDataDefinitionRpcService);
    }

    @Bean
    public SettingPolicyServiceController settingPolicyServiceController() {
        return new SettingPolicyServiceController(settingPolicyService());
    }

    @Bean
    public ScheduleRpcService scheduleService() {
        return new ScheduleRpcService(scheduleConfig.scheduleStore());
    }

    @Bean
    public ScheduleServiceController scheduleServiceController() {
        return new ScheduleServiceController(scheduleService());
    }

    /**
     * A bean to construct default setting policies.
     *
     * @return default setting policies creator
     */
    @Bean
    public DefaultSettingPolicyCreator defaultSettingPoliciesCreator() {
        // TODO move this bean definition info SettingsConfig as soon as we get rid of SettingStore
        // and PolicyStore singletons in TransactionProvider
        final DefaultSettingPolicyCreator creator =
                new DefaultSettingPolicyCreator(settingConfig.settingSpecsStore(),
                        transactionProvider(), TimeUnit.SECONDS.toMillis(
                        settingConfig.createDefaultSettingPolicyRetryIntervalSec),
                        identityProviderConfig.identityProvider());
        /*
         * Asynchronously create the default setting policies.
         * This is asynchronous so that DB availability doesn't prevent the group component from
         * starting up.
         */
        settingConfig.settingsCreatorThreadPool().execute(creator);
        return creator;
    }

}
