package com.vmturbo.plan.orchestrator.project;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Optional;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateClusterHeadroomTemplateRequest;
import com.vmturbo.common.protobuf.group.GroupDTOMoles;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectInfo.PlanProjectScenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template.Type;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTOMoles;
import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.plan.PlanInstanceQueue;
import com.vmturbo.plan.orchestrator.plan.PlanRpcService;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;

/**
 * Tests for the {@link com.vmturbo.plan.orchestrator.project.PlanProjectExecutor} class.
 */
public class PlanProjectExecutorTest {

    private PlanDao planDao = mock(PlanDao.class);

    private PlanProjectExecutor planProjectExecutor;

    private GroupDTOMoles.GroupServiceMole groupServiceMole = spy(new GroupDTOMoles.GroupServiceMole());

    private SettingProtoMoles.SettingServiceMole settingServiceMole = spy(new SettingServiceMole());

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(groupServiceMole, settingServiceMole);

    private TemplatesDao templatesDao = mock(TemplatesDao.class);

    @Before
    public void setup() throws Exception {
        IdentityGenerator.initPrefix(0);
        ProjectPlanPostProcessorRegistry registry = mock(ProjectPlanPostProcessorRegistry.class);
        PlanRpcService planRpcService = mock(PlanRpcService.class);
        Channel repositoryChannel = mock(Channel.class);
        Channel historyChannel = mock(Channel.class);
        PlanInstanceQueue planInstanceQueue = mock(PlanInstanceQueue.class);
        planProjectExecutor = new PlanProjectExecutor(planDao, grpcServer.getChannel(),
                planRpcService, registry, repositoryChannel, templatesDao, historyChannel,
                planInstanceQueue);
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
                .setCluster(ClusterInfo.newBuilder().setClusterType(ClusterInfo.Type.COMPUTE))
                .build());
        groupList.add(GroupDTO.Group.newBuilder()
                .setId(101)
                .setType(Group.Type.CLUSTER)
                .setCluster(ClusterInfo.newBuilder().setClusterType(ClusterInfo.Type.COMPUTE))
                .build());

        when(groupServiceMole.getGroups(any(GroupDTO.GetGroupsRequest.class)))
                .thenReturn(groupList);

        when(planDao.createPlanInstance(any(Scenario.class), eq(PlanProjectType.CLUSTER_HEADROOM)))
                .thenReturn(PlanDTO.PlanInstance.newBuilder()
                        .setPlanId(IdentityGenerator.next())
                        .setStatus(PlanStatus.READY)
                        .build());

        // Set maxPlanInstancesPerPlan to 10
        when(settingServiceMole.getGlobalSetting(any(GetSingleGlobalSettingRequest.class)))
            .thenReturn(GetGlobalSettingResponse.newBuilder()
                .setSetting(Setting.newBuilder()
                    .setNumericSettingValue(NumericSettingValue.newBuilder()
                            .setValue(10)
                            .build()))
                .build());

        planProjectExecutor.executePlan(planProject);

        // 2 clusters, with 2 scenarios each.  So there are 4 plan instances created.
        verify(planDao, Mockito.times(4)).
                createPlanInstance(any(Scenario.class), eq(PlanProjectType.CLUSTER_HEADROOM));
    }

    /**
     * When calling createPlanInstanceWithClusterHeadroomTemplate and the group has a headroom
     * template ID configured, and the template with that ID does exist, this template
     * will be used for execution.
     *
     * @throws Exception
     */
    @Test
    public void testCreatePlanInstanceWithClusterHeadroomTemplate() throws Exception {
        long headroomTempalteId = 3333;
        Group groupWithHeadroomTemplateId = Group.newBuilder()
                .setId(12345)
                .setType(Group.Type.CLUSTER)
                .setCluster(ClusterInfo.newBuilder()
                        .setClusterHeadroomTemplateId(headroomTempalteId)
                        .build())
                .build();

        when(templatesDao.getTemplate(headroomTempalteId)).thenReturn(Optional.of(Template.getDefaultInstance()));

        planProjectExecutor.createClusterPlanInstance(groupWithHeadroomTemplateId,
                PlanProjectScenario.getDefaultInstance(), PlanProjectType.CLUSTER_HEADROOM);
        verify(templatesDao).getTemplate(anyLong());
        verify(templatesDao, never()).getTemplatesByName("headroomVM");
    }

    /**
     * When calling createPlanInstanceWithClusterHeadroomTemplate and the group has a headroom
     * template ID configured, but the template with that ID does not exist, the default
     * headroom template will be used for execution. The headroom template ID of the group will
     * also be updated.
     *
     * @throws Exception
     */
    @Test
    public void testCreatePlanInstanceWithClusterHeadroomTemplateNotFound() throws Exception {
        long headroomTempalteId = 3333;
        Group groupWithHeadroomTemplateId = Group.newBuilder()
                .setId(12345)
                .setType(Group.Type.CLUSTER)
                .setCluster(ClusterInfo.newBuilder()
                        .setClusterHeadroomTemplateId(headroomTempalteId)
                        .build())
                .build();

        when(templatesDao.getTemplate(headroomTempalteId)).thenReturn(Optional.empty());

        planProjectExecutor.createClusterPlanInstance(groupWithHeadroomTemplateId,
                PlanProjectScenario.getDefaultInstance(), PlanProjectType.CLUSTER_HEADROOM);
        verify(templatesDao).getTemplate(anyLong());
        verify(templatesDao).getTemplatesByName("headroomVM");
        verify(groupServiceMole).updateClusterHeadroomTemplate(any(UpdateClusterHeadroomTemplateRequest.class));
    }

    /**
     * When calling createPlanInstanceWithClusterHeadroomTemplate, and the group does not have
     * a headroom plan ID. In this case, use the default headroom template.
     *
     * @throws Exception
     */
    @Test
    public void testCreatePlanInstanceWithoutClusterHeadroomTemplate() throws Exception {
        long headroomTempalteId = 3333;
        Group groupWithHeadroomTemplateId = Group.newBuilder()
                .setId(12345)
                .setType(Group.Type.CLUSTER)
                .setCluster(ClusterInfo.newBuilder()
                        .build())
                .build();

        planProjectExecutor.createClusterPlanInstance(groupWithHeadroomTemplateId,
                PlanProjectScenario.getDefaultInstance(), PlanProjectType.CLUSTER_HEADROOM);
        verify(templatesDao, never()).getTemplate(anyLong());
        verify(templatesDao).getTemplatesByName("headroomVM");
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

    @Test
    public void testRestrictNumberOfClustersMoreThanMax() throws Exception {
        int numberOfClusters = 100;
        Float maxNumberOfClusters = 20F;
        when(settingServiceMole.getGlobalSetting(any(GetSingleGlobalSettingRequest.class)))
            .thenReturn(GetGlobalSettingResponse.newBuilder()
                .setSetting(Setting.newBuilder()
                    .setNumericSettingValue(NumericSettingValue.newBuilder()
                            .setValue(maxNumberOfClusters)
                            .build()))
                .build());
        Set<Group> groupSet1 = new HashSet<>();
        for (int i = 0; i < numberOfClusters; i++) {
            Group group = Group.newBuilder()
                    .setId(i)
                    .build();
            groupSet1.add(group);
        }
        groupSet1 = planProjectExecutor.restrictNumberOfClusters(groupSet1);

        assertEquals(maxNumberOfClusters.intValue(), groupSet1.size());
    }

    @Test
    public void testRestrictNumberOfClustersLessThanMax() throws Exception {
        int numberOfClusters = 15;
        Float maxNumberOfClusters = 20F;
        when(settingServiceMole.getGlobalSetting(any(GetSingleGlobalSettingRequest.class)))
            .thenReturn(GetGlobalSettingResponse.newBuilder()
                .setSetting(Setting.newBuilder()
                    .setNumericSettingValue(NumericSettingValue.newBuilder()
                            .setValue(maxNumberOfClusters)
                            .build()))
                .build());
        Set<Group> groupSet1 = new HashSet<>();
        for (int i = 0; i < numberOfClusters; i++) {
            Group group = Group.newBuilder()
                    .setId(i)
                    .build();
            groupSet1.add(group);
        }
        groupSet1 = planProjectExecutor.restrictNumberOfClusters(groupSet1);

        Set<Group> groupSet2 = new HashSet<>();
        for (int i = 0; i < numberOfClusters; i++) {
            Group group = Group.newBuilder()
                    .setId(i)
                    .build();
            groupSet2.add(group);
        }
        groupSet2 = planProjectExecutor.restrictNumberOfClusters(groupSet2);

        assertTrue(groupSet1.containsAll(groupSet2));
        assertEquals(numberOfClusters, groupSet1.size());
        assertEquals(numberOfClusters, groupSet2.size());
    }
}
