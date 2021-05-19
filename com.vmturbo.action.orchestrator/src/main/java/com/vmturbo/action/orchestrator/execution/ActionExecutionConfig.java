package com.vmturbo.action.orchestrator.execution;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.ActionOrchestratorGlobalConfig;
import com.vmturbo.action.orchestrator.action.constraint.ActionConstraintStoreFactory;
import com.vmturbo.action.orchestrator.topology.TopologyProcessorConfig;
import com.vmturbo.auth.api.licensing.LicenseCheckClientConfig;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.topology.ProbeActionCapabilitiesServiceGrpc;
import com.vmturbo.common.protobuf.topology.ProbeActionCapabilitiesServiceGrpc.ProbeActionCapabilitiesServiceBlockingStub;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * Configuration for classes required to execute actions via
 * the {@link TopologyProcessor}.
 */
@Configuration
@Import({ActionOrchestratorGlobalConfig.class,
        TopologyProcessorConfig.class,
        GroupClientConfig.class,
        LicenseCheckClientConfig.class,
        RepositoryClientConfig.class})
public class ActionExecutionConfig {

    @Autowired
    private ActionOrchestratorGlobalConfig globalConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private TopologyProcessorConfig tpConfig;

    @Autowired
    private LicenseCheckClientConfig licenseCheckClientConfig;

    @Value("${failedGroupUpdateDelaySeconds:10}")
    private int groupUpdateDelaySeconds;

    @Value("${actionExecution.timeoutMins:360}")
    private int actionExecutionTimeoutMins;

    @Value("${failedActionPatterns:The cluster where the VM/Availability Set is allocated is currently out of capacity\ncom\\.microsoft\\.azure\\.CloudException: Async operation failed with provisioning state: Failed: Allocation failed}")
    private String failedActionPatterns;

    @Bean
    public ProbeCapabilityCache targetCapabilityCache() {
        return new ProbeCapabilityCache(tpConfig.topologyProcessor(), actionCapabilitiesService());
    }

    @Bean
    public FailedCloudVMGroupProcessor failedCloudVMGroupProcessor() {
        return new FailedCloudVMGroupProcessor(GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel()),
                RepositoryServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel()),
                SettingPolicyServiceGrpc.newBlockingStub(groupClientConfig.groupChannel()),
                Executors.newSingleThreadScheduledExecutor(),
                groupUpdateDelaySeconds, failedActionPatterns);
    }

    /**
     * Returns grpc service to get action capabilities.
     *
     * @return action capabilities grpc service
     */
    @Bean
    public ProbeActionCapabilitiesServiceBlockingStub actionCapabilitiesService() {
        return ProbeActionCapabilitiesServiceGrpc.newBlockingStub(tpConfig
                .topologyProcessorChannel());
    }

    @Bean
    public ActionConstraintStoreFactory actionConstraintStoreFactory() {
        return new ActionConstraintStoreFactory();
    }

    /**
     * Bean for {@link ActionExecutor}.
     * @return The {@link ActionExecutor}.
     */
    @Bean
    public ActionExecutor actionExecutor() {
        final ActionExecutor executor =
                new ActionExecutor(tpConfig.topologyProcessorChannel(),
                    globalConfig.actionOrchestratorClock(),
                    actionExecutionTimeoutMins,
                    TimeUnit.MINUTES, licenseCheckClientConfig.licenseCheckClient());

        tpConfig.topologyProcessor().addActionListener(executor);

        return executor;
    }

    @Bean
    public ActionTargetSelector actionTargetSelector() {
        return new ActionTargetSelector(targetCapabilityCache(),
                actionConstraintStoreFactory(),
                tpConfig.actionTopologyStore());
    }
}
