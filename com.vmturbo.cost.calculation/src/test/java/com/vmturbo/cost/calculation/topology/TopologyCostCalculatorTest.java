package com.vmturbo.cost.calculation.topology;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.CloudCostCalculator;
import com.vmturbo.cost.calculation.CloudCostCalculator.CloudCostCalculatorFactory;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.ReservedInstanceApplicator.ReservedInstanceApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory.DefaultTopologyCostCalculatorFactory;
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

    private CloudCostData cloudCostData = mock(CloudCostData.class);

    private DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory = mock(DiscountApplicatorFactory.class);

    private ReservedInstanceApplicatorFactory<TopologyEntityDTO> reservedInstanceApplicatorFactory = mock(ReservedInstanceApplicatorFactory.class);

    private TopologyCostCalculatorFactory factory = new DefaultTopologyCostCalculatorFactory(
            topologyEntityInfoExtractor, cloudCostCalculatorFactory, localCostDataProvider,
            discountApplicatorFactory, reservedInstanceApplicatorFactory);

    private TopologyInfo topoInfo = TopologyInfo.newBuilder().setTopologyContextId(1000l).build();
    @Test
    public void testCalculateCosts() throws CloudCostDataRetrievalException {
        when(cloudCostData.getCurrentRiCoverage()).thenReturn(Maps.newHashMap());
        final TopologyEntityCloudTopology cloudTopology = mock(TopologyEntityCloudTopology.class);
        when(localCostDataProvider.getCloudCostData(topoInfo, cloudTopology, topologyEntityInfoExtractor)).thenReturn(cloudCostData);
        final TopologyCostCalculator topologyCostCalculator = factory.newCalculator(topoInfo, cloudTopology);

        final Map<Long, TopologyEntityDTO> cloudEntities = ImmutableMap.of(ENTITY.getOid(), ENTITY);
        when(cloudTopology.getEntities()).thenReturn(cloudEntities);

        final CloudCostCalculator<TopologyEntityDTO> costCalculator = mock(CloudCostCalculator.class);
        final CostJournal<TopologyEntityDTO> journal = mock(CostJournal.class);
        when(costCalculator.calculateCost(ENTITY)).thenReturn(journal);
        when(cloudCostCalculatorFactory.newCalculator(
                eq(cloudCostData),
                eq(cloudTopology),
                eq(topologyEntityInfoExtractor),
                eq(reservedInstanceApplicatorFactory),
                any(), any()))
            .thenReturn(costCalculator);


        final Map<Long, CostJournal<TopologyEntityDTO>> costs =
                topologyCostCalculator.calculateCosts(cloudTopology);
        assertThat(costs.get(ENTITY.getOid()), is(journal));
    }

    @Test
    public void testCalculateCostsEmptyData() throws CloudCostDataRetrievalException {
        final TopologyEntityCloudTopology cloudTopology = mock(TopologyEntityCloudTopology.class);
        when(localCostDataProvider.getCloudCostData(topoInfo, cloudTopology, topologyEntityInfoExtractor)).thenThrow(CloudCostDataRetrievalException.class);

        final TopologyCostCalculator calculator = factory.newCalculator(topoInfo, cloudTopology);
        assertThat(calculator.getCloudCostData(), is(CloudCostData.empty()));
    }
}

