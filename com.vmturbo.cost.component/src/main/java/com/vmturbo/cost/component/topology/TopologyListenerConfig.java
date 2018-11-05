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
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.ReservedInstanceApplicator;
import com.vmturbo.cost.calculation.ReservedInstanceApplicator.ReservedInstanceApplicatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory.DefaultTopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.cost.component.discount.CostConfig;
import com.vmturbo.cost.component.discount.DiscountConfig;
import com.vmturbo.cost.component.entity.cost.EntityCostConfig;
import com.vmturbo.cost.component.pricing.PricingConfig;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
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
        EntityCostConfig.class,
        DiscountConfig.class,
        ReservedInstanceConfig.class,
        CostConfig.class})
public class TopologyListenerConfig {

    @Autowired
    private TopologyProcessorClientConfig topologyClientConfig;

    @Autowired
    private ComputeTierDemandStatsConfig computeTierDemandStatsConfig;

    @Autowired
    private PricingConfig pricingConfig;

    @Autowired
    private EntityCostConfig entityCostConfig;

    @Autowired
    private DiscountConfig discountConfig;

    @Autowired
    private ReservedInstanceConfig reservedInstanceConfig;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Autowired
    private CostConfig costConfig;

    @Bean
    public LiveTopologyEntitiesListener liveTopologyEntitiesListener() {
        final LiveTopologyEntitiesListener entitiesListener =
            new LiveTopologyEntitiesListener(realtimeTopologyContextId,
                computeTierDemandStatsConfig.riDemandStatsWriter(),
                cloudTopologyFactory(), topologyCostCalculatorFactory(), entityCostConfig.entityCostStore(),
                reservedInstanceConfig.reservedInstanceCoverageUpload(),
                costConfig.businessAccountHelper(),
                costJournalRecorder());
        topologyProcessor().addLiveTopologyListener(entitiesListener);
        return entitiesListener;
    }

    @Bean
    public TopologyProcessor topologyProcessor() {
        return topologyClientConfig.topologyProcessor(EnumSet.of(Subscription.LiveTopologies));
    }

    @Bean
    public TopologyCostCalculatorFactory topologyCostCalculatorFactory() {
        return new DefaultTopologyCostCalculatorFactory(topologyEntityInfoExtractor(),
            cloudCostCalculatorFactory(), localCostDataProvider(), discountApplicatorFactory(),
            riApplicatorFactory());
    }

    @Bean
    public ReservedInstanceApplicatorFactory<TopologyEntityDTO> riApplicatorFactory() {
        return ReservedInstanceApplicator.newFactory();
    }

    @Bean
    public DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory() {
        return DiscountApplicator.newFactory();
    }

    @Bean
    public TopologyEntityCloudTopologyFactory cloudTopologyFactory() {
        return new DefaultTopologyEntityCloudTopologyFactory();
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
        return new LocalCostDataProvider(pricingConfig.priceTableStore(),
                discountConfig.discountStore(),
                reservedInstanceConfig.reservedInstanceBoughtStore(),
                reservedInstanceConfig.reservedInstanceSpecStore(),
                reservedInstanceConfig.entityReservedInstanceMappingStore());
    }

    @Bean
    public CostJournalRecorder costJournalRecorder() {
        return new CostJournalRecorder();
    }

    @Bean
    public CostJournalRecorderController costJournalRecorderController() {
        return new CostJournalRecorderController(costJournalRecorder());
    }
}
