package com.vmturbo.cost.component.topology;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.cost.calculation.topology.TopologyCostCalculator.TopologyCostCalculatorFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.entity.cost.EntityCostStore;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsWriter;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceCoverageUpdate;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisInvoker;
import com.vmturbo.cost.component.util.BusinessAccountHelper;

public class LiveTopologyEntitiesListenerTest {

    @Test
    public void testSkipEmptyCloudEntity() {
        ComputeTierDemandStatsWriter computeTierDemandStatsWriter = mock(ComputeTierDemandStatsWriter.class);
        TopologyEntityCloudTopologyFactory topologyCostCalculatorFactory = mock(TopologyEntityCloudTopologyFactory.class);
        TopologyInfo topologyInfo = TopologyInfo.newBuilder().setTopologyContextId(1L).build();
        TopologyEntityCloudTopology cloudTopology = mock(TopologyEntityCloudTopology.class);
        when(cloudTopology.getEntities()).thenReturn(Collections.emptyMap());
        ReservedInstanceCoverageUpdate reservedInstanceCoverageUpdate = mock(ReservedInstanceCoverageUpdate.class);
        LiveTopologyEntitiesListener liveTopologyEntitiesListener =
            new LiveTopologyEntitiesListener(1L, computeTierDemandStatsWriter,
                topologyCostCalculatorFactory, mock(TopologyCostCalculatorFactory.class),
                mock(EntityCostStore.class), reservedInstanceCoverageUpdate, mock(BusinessAccountHelper.class),
                mock(CostJournalRecorder.class), mock(ReservedInstanceAnalysisInvoker.class));
        RemoteIterator remoteIterator = mock(RemoteIterator.class);
        when(remoteIterator.hasNext()).thenReturn(false);
        when(topologyCostCalculatorFactory.newCloudTopology(1L, remoteIterator)).thenReturn(cloudTopology);
        liveTopologyEntitiesListener.onTopologyNotification(topologyInfo, remoteIterator);
        verify(topologyCostCalculatorFactory).newCloudTopology(1L, remoteIterator);
        verify(computeTierDemandStatsWriter, never()).calculateAndStoreRIDemandStats(any(), any(), anyBoolean());
    }


}
