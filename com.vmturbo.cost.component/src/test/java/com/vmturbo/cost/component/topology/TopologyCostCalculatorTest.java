package com.vmturbo.cost.component.topology;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.CloudCostCalculator;
import com.vmturbo.cost.calculation.CloudCostCalculator.CloudCostCalculatorFactory;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit test for {@link TopologyCostCalculator}.
 */
public class TopologyCostCalculatorTest {

    private static final TopologyEntityDTO ENTITY =  TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOid(7)
            .build();

    private TopologyEntityCloudTopologyFactory cloudTopologyFactory = mock(TopologyEntityCloudTopologyFactory.class);

    private TopologyEntityInfoExtractor topologyEntityInfoExtractor = mock(TopologyEntityInfoExtractor.class);

    private CloudCostCalculatorFactory<TopologyEntityDTO> cloudCostCalculatorFactory = mock(CloudCostCalculatorFactory.class);

    private LocalCostDataProvider localCostDataProvider = mock(LocalCostDataProvider.class);

    private DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory = mock(DiscountApplicatorFactory.class);

    @Test
    public void testCalculateCosts() throws CloudCostDataRetrievalException {
        final TopologyCostCalculator topologyCostCalculator = new TopologyCostCalculator(
                cloudTopologyFactory, topologyEntityInfoExtractor,
                cloudCostCalculatorFactory, localCostDataProvider, discountApplicatorFactory);

        final Map<Long, TopologyEntityDTO> cloudEntities = ImmutableMap.of(ENTITY.getOid(), ENTITY);
        final TopologyEntityCloudTopology cloudTopology = mock(TopologyEntityCloudTopology.class);
        when(cloudTopologyFactory.newCloudTopology(cloudEntities)).thenReturn(cloudTopology);

        final CloudCostCalculator<TopologyEntityDTO> costCalculator = mock(CloudCostCalculator.class);
        final CostJournal<TopologyEntityDTO> journal = mock(CostJournal.class);
        when(costCalculator.calculateCost(ENTITY)).thenReturn(journal);
        when(cloudCostCalculatorFactory.newCalculator(localCostDataProvider, cloudTopology, topologyEntityInfoExtractor, discountApplicatorFactory))
                .thenReturn(costCalculator);


        final Map<Long, CostJournal<TopologyEntityDTO>> costs =
                topologyCostCalculator.calculateCosts(ImmutableMap.of(ENTITY.getOid(), ENTITY));
        assertThat(costs.get(ENTITY.getOid()), is(journal));
    }
}
