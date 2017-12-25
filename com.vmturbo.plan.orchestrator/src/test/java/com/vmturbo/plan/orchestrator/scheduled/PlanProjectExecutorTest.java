package com.vmturbo.plan.orchestrator.scheduled;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTOMoles;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template.Type;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.plan.PlanRpcService;
import com.vmturbo.plan.orchestrator.project.PlanProjectExecutor;
import com.vmturbo.plan.orchestrator.project.ProjectPlanPostProcessorRegistry;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;

/**
 * Tests for the {@link com.vmturbo.plan.orchestrator.project.PlanProjectExecutor} class.
 */
public class PlanProjectExecutorTest {

    private PlanDao planDao = mock(PlanDao.class);

    private PlanProjectExecutor planProjectExecutor;

    private GroupDTOMoles.GroupServiceMole groupServiceMole = spy(new GroupDTOMoles.GroupServiceMole());

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(groupServiceMole);

    private TemplatesDao templatesDao = mock(TemplatesDao.class);

    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);
        ProjectPlanPostProcessorRegistry registry = mock(ProjectPlanPostProcessorRegistry.class);
        PlanRpcService planRpcService = mock(PlanRpcService.class);
        Channel repositoryChannel = mock(Channel.class);
        Channel historyChannel = mock(Channel.class);
        planProjectExecutor = new PlanProjectExecutor(planDao, grpcServer.getChannel(),
                planRpcService, registry, repositoryChannel, templatesDao, historyChannel);
        when(templatesDao.getTemplatesByName("headroomVM"))
            .thenReturn(Collections.singletonList(Template.newBuilder()
                    .setId(7L)
                    .setType(Type.SYSTEM)
                    .setTemplateInfo(TemplateInfo.newBuilder()
                        .setName("headroomVM"))
                    .build()));
    }

    @Test
    public void testExecutePlanTwoCluster() throws Exception {
        PlanDTO.PlanProject planProject = createHeadroomPlanProjectWithTwoScenarios();

        // 2 clusters
        List<Group> groupList = new ArrayList<>();
        groupList.add(GroupDTO.Group.newBuilder()
                .setId(100)
                .setType(Group.Type.CLUSTER)
                .build());
        groupList.add(GroupDTO.Group.newBuilder()
                .setId(101)
                .setType(Group.Type.CLUSTER)
                .build());

        when(groupServiceMole.getGroups(eq(GroupDTO.GetGroupsRequest.newBuilder()
                .setTypeFilter(GroupDTO.Group.Type.CLUSTER)
                .build())))
                .thenReturn(groupList);

        when(planDao.createPlanInstance(any(Scenario.class), eq(PlanProjectType.CLUSTER_HEADROOM)))
                .thenReturn(PlanDTO.PlanInstance.newBuilder()
                        .setPlanId(IdentityGenerator.next())
                        .setStatus(PlanStatus.READY)
                        .build());

        planProjectExecutor.executePlan(planProject);

        // 2 clusters, with 2 scenarios each.  So there are 4 plan instances created.
        verify(planDao, Mockito.times(4)).
                createPlanInstance(any(Scenario.class), eq(PlanProjectType.CLUSTER_HEADROOM));
    }

    /**
     * Create a plan project of type CLUSTER_HEADROOM, with 2 scenarios
     * @return a plan project
     */
    private PlanDTO.PlanProject createHeadroomPlanProjectWithTwoScenarios() {
        PlanDTO.ScenarioChange.TopologyAddition topologyAddition =
                PlanDTO.ScenarioChange.TopologyAddition.newBuilder()
                        .addChangeApplicationDays(0)
                        .build();
        PlanDTO.ScenarioChange scenarioChange1 = PlanDTO.ScenarioChange.newBuilder()
                .setTopologyAddition(topologyAddition)
                .build();
        PlanDTO.ScenarioChange scenarioChange2 = PlanDTO.ScenarioChange.newBuilder()
                .setTopologyAddition(topologyAddition)
                .build();
        PlanProjectInfo.PlanProjectScenario scenario1 =
                PlanProjectInfo.PlanProjectScenario.newBuilder()
                        .addChanges(scenarioChange1)
                        .build();
        PlanProjectInfo.PlanProjectScenario scenario2 =
                PlanProjectInfo.PlanProjectScenario.newBuilder()
                        .addChanges(scenarioChange2)
                        .build();
        PlanDTO.PlanProjectInfo planProjectInfo = PlanDTO.PlanProjectInfo.newBuilder()
                .setName("Test Project")
                .setType(PlanProjectType.CLUSTER_HEADROOM)
                .setPerClusterScope(true)
                .addScenarios(scenario1)
                .addScenarios(scenario2)
                .build();
        PlanDTO.PlanProject planProject = PlanDTO.PlanProject.newBuilder()
                .setPlanProjectInfo(planProjectInfo)
                .build();
        return planProject;
    }
}
