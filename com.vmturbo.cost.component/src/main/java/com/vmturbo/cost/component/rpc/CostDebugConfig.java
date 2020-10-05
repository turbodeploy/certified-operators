package com.vmturbo.cost.component.rpc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cloud.commitment.analysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cost.CostDebugREST.CostDebugServiceController;
import com.vmturbo.common.protobuf.trax.TraxREST.TraxConfigurationServiceController;
import com.vmturbo.cost.component.cca.CloudCommitmentAnalysisStoreConfig;
import com.vmturbo.cost.component.cca.LocalCloudCommitmentAnalysisConfig;
import com.vmturbo.cost.component.entity.cost.EntityCostConfig;
import com.vmturbo.cost.component.reserved.instance.BuyRIAnalysisConfig;
import com.vmturbo.cost.component.reserved.instance.BuyRIImpactReportGenerator;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecConfig;
import com.vmturbo.cost.component.topology.TopologyListenerConfig;
import com.vmturbo.trax.rpc.TraxConfigurationRpcService;

/**
 * Configuration for the debug service.
 * It's kept separately from other configurations because the debug service imports a bunch of
 * random configurations to get its hands in all over the Spring context, and defining it in
 * a configuration class mixed with business logic can lead to circular dependencies.
 */
@Configuration
@Import({ReservedInstanceConfig.class,
        TopologyListenerConfig.class,
        BuyRIAnalysisConfig.class,
        ReservedInstanceSpecConfig.class,
        EntityCostConfig.class,
        CloudCommitmentAnalysisStoreConfig.class,
        CloudCommitmentAnalysisConfig.class,
        LocalCloudCommitmentAnalysisConfig.class
})
public class CostDebugConfig {

    @Autowired
    private ReservedInstanceConfig reservedInstanceConfig;

    @Autowired
    private TopologyListenerConfig topologyListenerConfig;

    @Autowired
    private BuyRIAnalysisConfig buyRIAnalysisConfig;

    @Autowired
    private ReservedInstanceSpecConfig reservedInstanceSpecConfig;

    @Autowired
    private EntityCostConfig entityCostConfig;

    @Autowired
    private CloudCommitmentAnalysisConfig cloudCommitmentAnalysisConfig;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    /**
     * Cost debug service, which provides various endpoints useful for debugging the cost component.
     *
     * @return The {@link CostDebugRpcService}.
     */
    @Bean
    public CostDebugRpcService costDebugRpcService() {
        return new CostDebugRpcService(
                topologyListenerConfig.costJournalRecorder(),
                reservedInstanceConfig.entityReservedInstanceMappingStore(),
                buyRIAnalysisConfig.reservedInstanceAnalysisInvoker(),
                buyRIImpactReportGenerator(),
                cloudCommitmentAnalysisConfig.cloudCommitmentAnalysisManager(),
                buyRIAnalysisConfig.repositoryServiceClient());
    }

    /**
     * Controller to present a REST interface to the {@link CostDebugRpcService}.
     *
     * @return The {@link CostDebugServiceController}.
     */
    @Bean
    public CostDebugServiceController costDebugServiceController() {
        return new CostDebugServiceController(costDebugRpcService());
    }

    /**
     * Create the traxConfigurationRpcService.
     *
     * @return A {@link TraxConfigurationRpcService} instance.
     */
    @Bean
    public TraxConfigurationRpcService traxConfigurationRpcService() {
        return new TraxConfigurationRpcService();
    }

    /**
     * Create the traxConfigurationServiceController.
     *
     * @return A {@link TraxConfigurationServiceController} instance.
     */
    @Bean
    public TraxConfigurationServiceController traxConfigurationServiceController() {
        return new TraxConfigurationServiceController(traxConfigurationRpcService());
    }

    @Bean
    public BuyRIImpactReportGenerator buyRIImpactReportGenerator() {
        return new BuyRIImpactReportGenerator(
                buyRIAnalysisConfig.repositoryServiceClient(),
                buyRIAnalysisConfig.buyReservedInstanceStore(),
                reservedInstanceSpecConfig.reservedInstanceSpecStore(),
                reservedInstanceConfig.projectedEntityRICoverageAndUtilStore(),
                entityCostConfig.projectedEntityCostStore(),
                reservedInstanceConfig.cloudTopologyFactory(),
                realtimeTopologyContextId);
    }
}
