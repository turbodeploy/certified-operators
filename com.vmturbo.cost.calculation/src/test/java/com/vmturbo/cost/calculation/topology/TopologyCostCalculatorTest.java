package com.vmturbo.cost.calculation.topology;

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
import com.vmturbo.cost.calculation.ReservedInstanceApplicator.ReservedInstanceApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit test for {@link TopologyCostCalculator}.
 */
public class TopologyCostCalculatorTest {

    private static final TopologyEntityDTO ENTITY =  TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setOid(7)
            .build();

    private TopologyEntityInfoExtractor topologyEntityInfoExtractor = mock(TopologyEntityInfoExtractor.class);

    private CloudCostCalculatorFactory<TopologyEntityDTO> cloudCostCalculatorFactory = mock(CloudCostCalculatorFactory.class);

    private CloudCostDataProvider localCostDataProvider = mock(CloudCostDataProvider.class);

    private DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory = mock(DiscountApplicatorFactory.class);

    private ReservedInstanceApplicatorFactory<TopologyEntityDTO> reservedInstanceApplicatorFactory = mock(ReservedInstanceApplicatorFactory.class);

    @Test
    public void testCalculateCosts() throws CloudCostDataRetrievalException {
        final TopologyCostCalculator topologyCostCalculator = new TopologyCostCalculator(
                topologyEntityInfoExtractor, cloudCostCalculatorFactory, localCostDataProvider,
                discountApplicatorFactory, reservedInstanceApplicatorFactory);

        final Map<Long, TopologyEntityDTO> cloudEntities = ImmutableMap.of(ENTITY.getOid(), ENTITY);
        final TopologyEntityCloudTopology cloudTopology = mock(TopologyEntityCloudTopology.class);
        when(cloudTopology.getEntities()).thenReturn(cloudEntities);

        final CloudCostCalculator<TopologyEntityDTO> costCalculator = mock(CloudCostCalculator.class);
        final CostJournal<TopologyEntityDTO> journal = mock(CostJournal.class);
        when(costCalculator.calculateCost(ENTITY)).thenReturn(journal);
        when(cloudCostCalculatorFactory.newCalculator(localCostDataProvider, cloudTopology,
                topologyEntityInfoExtractor, discountApplicatorFactory, reservedInstanceApplicatorFactory))
            .thenReturn(costCalculator);


        final Map<Long, CostJournal<TopologyEntityDTO>> costs =
                topologyCostCalculator.calculateCosts(cloudTopology);
        assertThat(costs.get(ENTITY.getOid()), is(journal));
    }
}

