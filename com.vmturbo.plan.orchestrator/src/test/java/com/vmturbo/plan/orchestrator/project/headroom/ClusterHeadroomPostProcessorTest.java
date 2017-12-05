package com.vmturbo.plan.orchestrator.project.headroom;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.function.Consumer;

import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.stats.Stats.SaveClusterHeadroomRequest;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.project.ProjectPlanPostProcessor;

public class ClusterHeadroomPostProcessorTest {

    private static final long PLAN_ID = 7;
    private static final long CLUSTER_ID = 10;
    private static final long ADDED_CLONES = 50;

    private static final Group CLUSTER = Group.newBuilder()
            .setType(Group.Type.CLUSTER)
            .setId(CLUSTER_ID)
            .setCluster(ClusterInfo.newBuilder()
                    .setName("foo"))
            .build();

    private RepositoryServiceMole repositoryServiceMole = spy(new RepositoryServiceMole());

    private StatsHistoryServiceMole historyServiceMole = spy(new StatsHistoryServiceMole());

    private PlanDao planDao = mock(PlanDao.class);

    @Rule
    public GrpcTestServer grpcTestServer =
            GrpcTestServer.newServer(repositoryServiceMole, historyServiceMole);

    @Test
    public void testProjectedTopologyAvailable() {
        final ClusterHeadroomPlanPostProcessor processor =
                spy(new ClusterHeadroomPlanPostProcessor(PLAN_ID, CLUSTER,
                        grpcTestServer.getChannel(), grpcTestServer.getChannel(), ADDED_CLONES, planDao));
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

        verify(historyServiceMole).saveClusterHeadroom(SaveClusterHeadroomRequest.newBuilder()
                .setClusterId(CLUSTER_ID)
                .setHeadroom(49L)
                .build());
    }

    @Test
    public void testPlanSucceeded() throws NoSuchObjectException {
        final ClusterHeadroomPlanPostProcessor processor =
                spy(new ClusterHeadroomPlanPostProcessor(PLAN_ID, CLUSTER,
                        grpcTestServer.getChannel(), grpcTestServer.getChannel(), ADDED_CLONES, planDao));
        Consumer<ProjectPlanPostProcessor> onCompleteHandler = mock(Consumer.class);
        processor.registerOnCompleteHandler(onCompleteHandler);

        processor.onPlanStatusChanged(PlanInstance.newBuilder()
                .setPlanId(PLAN_ID)
                .setStatus(PlanStatus.SUCCEEDED)
                // Don't set the projected topology ID so that we don't try to store headroom
                .build());

        verify(planDao).deletePlan(PLAN_ID);
        verify(onCompleteHandler).accept(processor);
    }

    @Test
    public void testPlanFailed() throws NoSuchObjectException {
        final ClusterHeadroomPlanPostProcessor processor =
                spy(new ClusterHeadroomPlanPostProcessor(PLAN_ID, CLUSTER,
                        grpcTestServer.getChannel(), grpcTestServer.getChannel(), ADDED_CLONES, planDao));
        Consumer<ProjectPlanPostProcessor> onCompleteHandler = mock(Consumer.class);
        processor.registerOnCompleteHandler(onCompleteHandler);

        processor.onPlanStatusChanged(PlanInstance.newBuilder()
                .setPlanId(PLAN_ID)
                .setStatus(PlanStatus.FAILED)
                // Don't set the projected topology ID so that we don't try to store headroom
                .build());

        verify(planDao).deletePlan(PLAN_ID);
        verify(onCompleteHandler).accept(processor);
    }
}
