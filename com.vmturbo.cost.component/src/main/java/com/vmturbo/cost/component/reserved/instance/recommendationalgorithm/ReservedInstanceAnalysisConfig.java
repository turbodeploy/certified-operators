package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.CostServiceConfig;
import com.vmturbo.cost.component.discount.CostConfig;
import com.vmturbo.cost.component.pricing.PricingConfig;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecConfig;
import com.vmturbo.cost.component.reserved.instance.action.ReservedInstanceActionsSenderConfig;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.group.api.GroupMemberRetriever;
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
        ReservedInstanceSpecConfig.class})
public class ReservedInstanceAnalysisConfig {

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${preferredCurrentWeight}")
    private float preferredCurrentWeight;

    @Value("${riMinimumDataPoints}")
    private int riMinimumDataPoints;

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
    private ReservedInstanceSpecConfig reservedInstanceSpecConfig;

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
                reservedInstanceSpecConfig.reservedInstanceSpecStore(),
                pricingConfig.priceTableStore(),
                computeTierDemandStatsConfig.riDemandStatsStore(),
                cloudTopologyFactory(),
                reservedInstanceActionsSenderConfig.actionSender(),
                reservedInstanceConfig.buyReservedInstanceStore(),
                reservedInstanceConfig.actionContextRIBuyStore(),
                realtimeTopologyContextId, preferredCurrentWeight,
                riMinimumDataPoints);
    }

    public ReservedInstanceBoughtStore reservedInstanceBoughtStore() {
        return reservedInstanceConfig.reservedInstanceBoughtStore();
    }

    @Bean
    public TopologyEntityCloudTopologyFactory cloudTopologyFactory() {
        return new DefaultTopologyEntityCloudTopologyFactory(
                new GroupMemberRetriever(
                        GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel())));
    }
}
