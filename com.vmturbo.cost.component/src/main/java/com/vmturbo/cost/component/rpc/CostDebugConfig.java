package com.vmturbo.cost.component.rpc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.cost.CostDebugREST.CostDebugServiceController;
import com.vmturbo.cost.component.reserved.instance.BuyRIAnalysisConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.topology.TopologyListenerConfig;

/**
 * Configuration for the debug service.
 * It's kept separately from other configurations because the debug service imports a bunch of
 * random configurations to get its hands in all over the Spring context, and defining it in
 * a configuration class mixed with business logic can lead to circular dependencies.
 */
@Configuration
@Import({ReservedInstanceConfig.class,
    TopologyListenerConfig.class,
    BuyRIAnalysisConfig.class
})
public class CostDebugConfig {

    @Autowired
    private ReservedInstanceConfig reservedInstanceConfig;

    @Autowired
    private TopologyListenerConfig topologyListenerConfig;

    @Autowired
    private BuyRIAnalysisConfig buyRIAnalysisConfig;

    /**
     * Cost debug service, which provides various endpoints useful for debugging the cost component.
     *
     * @return The {@link CostDebugRpcService}.
     */
    @Bean
    public CostDebugRpcService costDebugRpcService() {
        return new CostDebugRpcService(topologyListenerConfig.costJournalRecorder(),
            reservedInstanceConfig.entityReservedInstanceMappingStore(),
            buyRIAnalysisConfig.reservedInstanceAnalysisInvoker());
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
}
