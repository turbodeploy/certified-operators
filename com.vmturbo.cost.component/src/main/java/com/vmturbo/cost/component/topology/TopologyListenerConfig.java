package com.vmturbo.cost.component.topology;


import java.util.EnumSet;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.CloudCostCalculator;
import com.vmturbo.cost.calculation.CloudCostCalculator.CloudCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.cost.component.entity.cost.EntityCostConfig;
import com.vmturbo.cost.component.pricing.PricingConfig;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsConfig;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig.Subscription;

/**
 * Setup listener for topologies from Topology Processor.
 *
 */

@Configuration
@Import({ComputeTierDemandStatsConfig.class,
        TopologyProcessorClientConfig.class,
        PricingConfig.class,
        EntityCostConfig.class})
public class TopologyListenerConfig {

    @Autowired
    private TopologyProcessorClientConfig topologyClientConfig;

    @Autowired
    private ComputeTierDemandStatsConfig computeTierDemandStatsConfig;

    @Autowired
    private PricingConfig pricingConfig;

    @Autowired
    private EntityCostConfig entityCostConfig;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Bean
    public LiveTopologyEntitiesListener liveTopologyEntitiesListener() {
        return new LiveTopologyEntitiesListener(realtimeTopologyContextId,
                computeTierDemandStatsConfig.riDemandStatsWriter(),
                topologyClientConfig.topologyProcessorRpcOnly(),
                topologyCostCalculator(), entityCostConfig.entityCostStore());
    }

    @Bean
    public TopologyProcessor topologyProcessor() {
        final TopologyProcessor topologyProcessor =
                topologyClientConfig.topologyProcessor(
                    EnumSet.of(Subscription.LiveTopologies));
        topologyProcessor.addLiveTopologyListener(liveTopologyEntitiesListener());
        return topologyProcessor;
    }

    @Bean
    public TopologyCostCalculator topologyCostCalculator() {
        return new TopologyCostCalculator(cloudTopologyFactory(), topologyEntityInfoExtractor(),
                cloudCostCalculatorFactory(), localCostDataProvider());
    }

    @Bean
    public TopologyEntityCloudTopologyFactory cloudTopologyFactory() {
        return TopologyEntityCloudTopology.newFactory();
    }

    @Bean
    public TopologyEntityInfoExtractor topologyEntityInfoExtractor() {
        return new TopologyEntityInfoExtractor();
    }

    @Bean
    public CloudCostCalculatorFactory<TopologyEntityDTO> cloudCostCalculatorFactory() {
        return CloudCostCalculator.newFactory();
    }

    @Bean
    public LocalCostDataProvider localCostDataProvider() {
        return new LocalCostDataProvider(pricingConfig.priceTableStore());
    }
}
