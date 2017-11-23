package com.vmturbo.api.component.communication;

import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.converter.xml.Jaxb2RootElementHttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import io.grpc.Channel;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.api.component.external.api.websocket.ApiWebsocketConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.clustermgr.api.impl.ClusterMgrClient;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.TemplateSpecServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateSpecServiceGrpc.TemplateSpecServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.grpc.extensions.PingingChannelBuilder;
import com.vmturbo.plan.orchestrator.api.PlanOrchestrator;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;

/**
 * Configuration for the communication between the API component
 * and the rest of the components in the system.
 */
@Configuration
@Import({ApiWebsocketConfig.class, TopologyProcessorClientConfig.class,
        ActionOrchestratorClientConfig.class, PlanOrchestratorClientConfig.class})
public class CommunicationConfig {

    @Autowired
    private TopologyProcessorClientConfig tpClientConfig;
    @Autowired
    private ActionOrchestratorClientConfig aoClientConfig;
    @Autowired
    private PlanOrchestratorClientConfig planClientConfig;
    @Value("${clusterMgrHost}")
    private String clusterMgrHost;

    @Value("${server.port}")
    private int httpPort;

    @Value("${repositoryHost}")
    private String repositoryHost;

    @Value("${authHost}")
    public String authHost;

    @Value("${groupHost}")
    private String groupHost;

    @Value("${historyHost}")
    private String historyHost;

    @Value("${realtimeTopologyContextId}")
    private Long realtimeTopologyContextId;

    @Value("${grpcPingIntervalSeconds}")
    private long grpcPingIntervalSeconds;

    @Value("${websocket.pong.timeout}")
    private long websocketPongTimeout;

    @Value("${server.grpcPort}")
    private int grpcPort;

    @Autowired
    private ApiWebsocketConfig websocketConfig;

    public long getRealtimeTopologyContextId() {
        return realtimeTopologyContextId;
    }

    public String getAuthHost() {
        return authHost;
    }

    public int getHttpPort() {
        return httpPort;
    }

    @Bean
    public RestTemplate serviceRestTemplate() {
        RestTemplate restTemplate;
        // set up the RestTemplate to re-use in forwarding requests to RepositoryComponent
        restTemplate = new RestTemplate();
        final Jaxb2RootElementHttpMessageConverter msgConverter = new Jaxb2RootElementHttpMessageConverter();
        restTemplate.getMessageConverters().add(msgConverter);
        return restTemplate;
    }

    @Bean
    public ApiComponentActionListener apiComponentActionListener()
        throws CommunicationException, InterruptedException, URISyntaxException {
        final ApiComponentActionListener actionsListener =
            new ApiComponentActionListener(websocketConfig.websocketHandler());
        aoClientConfig.actionOrchestratorClient().addActionsListener(actionsListener);
        return actionsListener;
    }


    @Bean
    public ActionsServiceBlockingStub actionsRpcService() {
        return ActionsServiceGrpc.newBlockingStub(aoClientConfig.actionOrchestratorChannel())
                // Intercept client call and add JWT token to the metadata
                .withInterceptors(new JwtClientInterceptor());
    }

    @Bean
    public PolicyServiceBlockingStub policyRpcService() {
        return PolicyServiceGrpc.newBlockingStub(groupChannel());
    }

    @Bean
    public PlanServiceBlockingStub planRpcService() {
        return PlanServiceGrpc.newBlockingStub(planClientConfig.planOrchestratorChannel());
    }

    @Bean
    public EntitySeverityServiceBlockingStub entitySeverityService() {
        return EntitySeverityServiceGrpc.newBlockingStub(
                aoClientConfig.actionOrchestratorChannel());
    }

    @Bean
    public RepositoryApi repositoryApi() {
        return new RepositoryApi(repositoryHost, httpPort, serviceRestTemplate(),
                entitySeverityService(), getRealtimeTopologyContextId());
    }


    @Bean
    public ClusterMgrClient clusterMgr() {
        return ClusterMgrClient.rpcOnly(ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort(clusterMgrHost, httpPort)
                .setPongMessageTimeout(websocketPongTimeout)
                .build());
    }

    @Bean
    public TopologyProcessor topologyProcessor() {
        return tpClientConfig.topologyProcessorRpcOnly();
    }

    @Bean
    public ApiComponentPlanListener apiComponentPlanListener() {
        // TODO (roman, Dec 20 2016): It's kind of ugly to have the communication config
        // depend on the external API package. It may be worth it to have the
        // ApiWebsocketHandler/ApiWebsocketConfig register listeners instead.
        return new ApiComponentPlanListener(websocketConfig.websocketHandler());
    }

    @Bean
    public PlanOrchestrator planOrchestrator() {
        final PlanOrchestrator planOrchestrator = planClientConfig.planOrchestrator();
        planOrchestrator.addPlanListener(apiComponentPlanListener());
        return planOrchestrator;
    }

    @Bean
    public Channel groupChannel() {
        return PingingChannelBuilder.forAddress(groupHost, grpcPort)
            .setPingInterval(grpcPingIntervalSeconds, TimeUnit.SECONDS)
            .usePlaintext(true)
            .build();
    }

    @Bean
    public GroupServiceBlockingStub groupRpcService() {
        return GroupServiceGrpc.newBlockingStub(groupChannel());
    }

    @Bean
    public SettingServiceBlockingStub settingRpcService() {
        return SettingServiceGrpc.newBlockingStub(groupChannel());
    }

    @Bean
    public Channel historyChannel() {
        return PingingChannelBuilder.forAddress(historyHost, grpcPort)
                .setPingInterval(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .usePlaintext(true)
                .build();
    }

    @Bean
    public StatsHistoryServiceBlockingStub historyRpcService() {
        return StatsHistoryServiceGrpc.newBlockingStub(historyChannel());
    }

    @Bean
    public Channel repositoryChannel() {
        return PingingChannelBuilder.forAddress(repositoryHost, grpcPort)
                .setPingInterval(grpcPingIntervalSeconds, TimeUnit.SECONDS)
                .usePlaintext(true)
                .build();
    }

    @Bean
    public SearchServiceGrpc.SearchServiceBlockingStub searchServiceBlockingStub() {
        return SearchServiceGrpc.newBlockingStub(repositoryChannel());
    }

    @Bean
    public SupplyChainServiceBlockingStub supplyChainRpcService() {
        return SupplyChainServiceGrpc.newBlockingStub(repositoryChannel());
    }

    @Bean
    public TemplateServiceBlockingStub templateServiceBlockingStub() {
        return TemplateServiceGrpc.newBlockingStub(planClientConfig.planOrchestratorChannel());
    }

    @Bean
    public TemplateSpecServiceBlockingStub templateSpecServiceBlockingStub() {
        return TemplateSpecServiceGrpc.newBlockingStub(planClientConfig.planOrchestratorChannel());
    }

    @Bean
    public Channel planOrchestratorChannel() {
        return planClientConfig.planOrchestratorChannel();
    }

    @Bean
    public Channel actionOrchestratorChannel() {
        return aoClientConfig.actionOrchestratorChannel();
    }
}
