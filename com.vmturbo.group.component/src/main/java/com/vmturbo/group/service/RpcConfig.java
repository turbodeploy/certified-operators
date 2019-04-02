package com.vmturbo.group.service;

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
import com.vmturbo.common.protobuf.setting.SettingProtoREST.SettingPolicyServiceController;
import com.vmturbo.common.protobuf.setting.SettingProtoREST.SettingServiceController;
import com.vmturbo.group.IdentityProviderConfig;
import com.vmturbo.group.group.GroupConfig;
import com.vmturbo.group.policy.PolicyConfig;
import com.vmturbo.group.setting.SettingConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({ActionOrchestratorClientConfig.class,
        GroupConfig.class,
        IdentityProviderConfig.class,
        PolicyConfig.class,
        RepositoryClientConfig.class,
        SettingConfig.class,
        SQLDatabaseConfig.class,
        UserSessionConfig.class})
public class RpcConfig {

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Autowired
    private GroupConfig groupConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private PolicyConfig policyConfig;

    @Autowired
    private SettingConfig settingConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private ActionOrchestratorClientConfig aoClientConfig;

    @Autowired
    private UserSessionConfig userSessionConfig;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Bean
    public PolicyRpcService policyService() {
        return new PolicyRpcService(policyConfig.policyStore(), groupService(),
                userSessionConfig.userSessionContext());
    }

    @Bean
    public PolicyServiceController policyServiceController(final PolicyRpcService policyRpcService) {
        return new PolicyServiceController(policyRpcService);
    }

    @Bean
    public GroupRpcService groupService() {
        return new GroupRpcService(groupConfig.groupStore(),
                groupConfig.temporaryGroupCache(),
                repositoryClientConfig.searchServiceClient(),
                groupConfig.entityToClusterMapping(),
                databaseConfig.dsl(),
                policyConfig.policyStore(),
                settingConfig.settingStore(),
                userSessionConfig.userSessionContext());
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
                actionsRpcService(), realtimeTopologyContextId);
    }

    @Bean
    public SettingPolicyServiceController settingPolicyServiceController() {
        return new SettingPolicyServiceController(settingPolicyService());
    }

}
