package com.vmturbo.plan.orchestrator.project.headroom;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcTestServer;

public class ClusterHeadroomPostProcessorTest {

    private static final long PLAN_ID = 7;
    private static final long CLUSTER_ID = 10;
    private static final long ADDED_CLONES = 50;

    private RepositoryServiceMole repositoryServiceMole = spy(new RepositoryServiceMole());

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(repositoryServiceMole);

    @Test
    public void testProjectedTopologyAvailable() {
        final ClusterHeadroomPlanPostProcessor processor =
                spy(new ClusterHeadroomPlanPostProcessor(PLAN_ID, CLUSTER_ID,
                        grpcTestServer.getChannel(), ADDED_CLONES));
        final long projectedTopologyId = 100;

        when(repositoryServiceMole.retrieveTopology(RetrieveTopologyRequest.newBuilder()
                .setTopologyId(projectedTopologyId)
                .setEntityFilter(TopologyEntityFilter.newBuilder()
                        .setUnplacedOnly(true))
                .build())).thenReturn(
                        Collections.singletonList(RetrieveTopologyResponse.newBuilder()
                                .addEntities(TopologyEntityDTO.newBuilder()
                                    .setEntityType(10)
                                    .setOid(7))
                                .build()));

        processor.onPlanStatusChanged(PlanInstance.newBuilder()
                .setPlanId(PLAN_ID)
                .setStatus(PlanStatus.WAITING_FOR_RESULT)
                .setProjectedTopologyId(projectedTopologyId)
                .build());

        verify(processor).storeHeadroom(49L);
    }
}
