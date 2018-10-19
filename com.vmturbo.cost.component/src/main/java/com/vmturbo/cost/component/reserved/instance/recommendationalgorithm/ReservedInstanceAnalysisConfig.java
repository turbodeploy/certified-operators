package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.cost.component.CostServiceConfig;
import com.vmturbo.cost.component.discount.CostConfig;
import com.vmturbo.cost.component.pricing.PricingConfig;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.reserved.instance.action.ReservedInstanceActionsSender;
import com.vmturbo.cost.component.reserved.instance.action.ReservedInstanceActionsSenderConfig;
import com.vmturbo.cost.component.topology.TopologyListenerConfig;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;

@Import({
        ComputeTierDemandStatsConfig.class,
        CostConfig.class,
        CostServiceConfig.class,
        GroupClientConfig.class,
        PricingConfig.class,
        RepositoryClientConfig.class,
        ReservedInstanceConfig.class,
        ReservedInstanceActionsSenderConfig.class,
        TopologyListenerConfig.class})
public class ReservedInstanceAnalysisConfig {

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Autowired
    private ComputeTierDemandStatsConfig computeTierDemandStatsConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private PricingConfig pricingConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private ReservedInstanceConfig reservedInstanceConfig;

    @Autowired
    private ReservedInstanceActionsSenderConfig reservedInstanceActionsSenderConfig;

    @Autowired
    private TopologyListenerConfig topologyListenerConfig;

    @Bean
    public SettingServiceBlockingStub settingServiceClient() {
        return SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    @Bean
    public RepositoryServiceBlockingStub repositoryServiceClient() {
        return RepositoryServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel());
    }

    @Bean
    public ReservedInstanceAnalyzer reservedInstanceAnalyzer() {
        return new ReservedInstanceAnalyzer(
                settingServiceClient(),
                repositoryServiceClient(),
                reservedInstanceConfig.reservedInstanceBoughtStore(),
                reservedInstanceConfig.reservedInstanceSpecStore(),
                pricingConfig.priceTableStore(),
                computeTierDemandStatsConfig.riDemandStatsStore(),
                topologyListenerConfig.cloudTopologyFactory(),
                reservedInstanceActionsSenderConfig.actionSender(),
                realtimeTopologyContextId);
    }

}
