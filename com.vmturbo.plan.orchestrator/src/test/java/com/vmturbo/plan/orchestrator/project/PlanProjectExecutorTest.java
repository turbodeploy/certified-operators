package com.vmturbo.plan.orchestrator.project;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.UpdateClusterHeadroomTemplateRequest;
import com.vmturbo.common.protobuf.group.GroupDTOMoles;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectInfo.PlanProjectScenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template.Type;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadInfoResponse;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadRecord;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.plan.PlanRpcService;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;

/**
 * Tests for the {@link com.vmturbo.plan.orchestrator.project.PlanProjectExecutor} class.
 */
public class PlanProjectExecutorTest {

    private PlanDao planDao = mock(PlanDao.class);

    private PlanProjectExecutor planProjectExecutor;

    private GroupServiceMole groupServiceMole = spy(new GroupDTOMoles.GroupServiceMole());

    private SettingServiceMole settingServiceMole = spy(new SettingServiceMole());

    private StatsHistoryServiceMole statsHistoryServiceMole = spy(new StatsHistoryServiceMole());

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(groupServiceMole, settingServiceMole, statsHistoryServiceMole);

    private TemplatesDao templatesDao = mock(TemplatesDao.class);

    @Before
    public void setup() {
        IdentityGenerator.initPrefix(0);
        ProjectPlanPostProcessorRegistry registry = mock(ProjectPlanPostProcessorRegistry.class);
        PlanRpcService planRpcService = mock(PlanRpcService.class);
        Channel repositoryChannel = mock(Channel.class);
        planProjectExecutor = new PlanProjectExecutor(planDao, grpcServer.getChannel(),
                planRpcService, registry, repositoryChannel, templatesDao, grpcServer.getChannel(), true);
        when(templatesDao.getFilteredTemplates(any()))
            .thenReturn(Collections.singleton(Template.newBuilder()
                    .setId(7L)
                    .setType(Type.SYSTEM)
                    .setTemplateInfo(TemplateInfo.newBuilder()
                        .setName(StringConstants.CLUSTER_HEADROOM_DEFAULT_TEMPLATE_NAME))
                    .build()));
        when(statsHistoryServiceMole.getSystemLoadInfo(any()))
            .thenReturn(Collections.singletonList(SystemLoadInfoResponse.newBuilder()
                .addRecord(SystemLoadRecord.newBuilder()
                    .setPropertyType("VCPU")
                    .setAvgValue(10)
                    .setRelationType(0))
                .build()));
    }

    @Test
    public void testExecutePlanOnePlanInstancePerClusterPerScenario() throws Exception {
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

        ReflectionTestUtils.setField(planProjectExecutor, "headroomCalculationForAllClusters", false);
        planProjectExecutor.executePlan(planProject);

        // 2 clusters, with 2 scenarios each.  So there are 4 plan instances created.
        verify(planDao, Mockito.times(4)).
                createPlanInstance(any(Scenario.class), eq(PlanProjectType.CLUSTER_HEADROOM));
    }

    /**
     * If the per_cluster_scope value of the plan is set to false, we will apply the plan project
     * on all clusters. We will create one plan instance for all clusters per scenario.
     *
     * @throws Exception if exceptions occur
     */
    @Test
    public void testExecutePlanOnePlanInstanceAllClusterPerScenario() throws Exception {
        // 3 clusters
        List<Group> groupList = new ArrayList<>();
        Arrays.asList(1, 2, 3).forEach(i -> groupList.add(
            GroupDTO.Group.newBuilder().setId(i)
                .setType(Group.Type.CLUSTER)
                .setCluster(ClusterInfo.newBuilder().setClusterType(ClusterInfo.Type.COMPUTE))
                .build()
        ));

        when(groupServiceMole.getGroups(any(GroupDTO.GetGroupsRequest.class)))
            .thenReturn(groupList);

        when(planDao.createPlanInstance(any(Scenario.class), eq(PlanProjectType.CLUSTER_HEADROOM)))
            .thenReturn(PlanDTO.PlanInstance.newBuilder()
                .setPlanId(IdentityGenerator.next())
                .setStatus(PlanStatus.READY)
                .build());

        PlanDTO.PlanProject planProject = createHeadroomPlanProjectWithTwoScenarios();
        planProjectExecutor.executePlan(planProject);

        // 3 clusters, with 2 scenarios each. So there are 2 plan instances created.
        verify(planDao, Mockito.times(2))
            .createPlanInstance(any(Scenario.class), eq(PlanProjectType.CLUSTER_HEADROOM));
    }

    /**
     * When calling createPlanInstanceWithClusterHeadroomTemplate and the group has a selected
     * template ID configured, and the template with that ID does exist, this template
     * will be used for execution. We verify that edit template is not called because
     * the template is not a headroom template (it does not have an appropriate name).
     *
     * @throws Exception if exception occurs
     */
    @Test
    public void testCreatePlanInstanceWithClusterSelectedTemplate() throws Exception {
        long selectedTemplateId = 3333;
        Group groupWithHeadroomTemplateId = Group.newBuilder()
                .setId(12345)
                .setType(Group.Type.CLUSTER)
                .setCluster(ClusterInfo.newBuilder()
                        .setClusterHeadroomTemplateId(selectedTemplateId)
                        .build())
                .build();

        when(templatesDao.getTemplate(selectedTemplateId)).thenReturn(Optional.of(Template.getDefaultInstance()));

        planProjectExecutor.createClusterPlanInstance(Collections.singleton(groupWithHeadroomTemplateId),
                PlanProjectScenario.getDefaultInstance(), PlanProjectType.CLUSTER_HEADROOM);

        // The use selects a template which is not a headroomVM or an average template and the code regarding
        // headroomVM or average is not executed, thus editTemplate() and getFilteredTemplates() are not called
        verify(templatesDao, never()).editTemplate(eq(selectedTemplateId), any());
        verify(templatesDao, never()).getFilteredTemplates(any());
        // usedTemplate is not updated, thus the part of the code where usedTemplate != null
        // is not executed and updateClusterHeadroomTemplate() is not called
        verify(groupServiceMole, never()).updateClusterHeadroomTemplate(any(UpdateClusterHeadroomTemplateRequest.class));
    }



    /**
     * When calling createPlanInstanceWithClusterHeadroomTemplate and the group has a selected
     * template ID configured, and the template with that ID does exist, this template
     * will be used for execution. We verify that edit template is called because
     * the template is an average headroom template.
     *
     * @throws Exception if exception occurs
     */
    @Test
    public void testCreatePlanInstanceWithClusterHeadroomTemplate() throws Exception {
        long averageTemplateId = 3333;
        Group groupWithHeadroomTemplateId = Group.newBuilder()
                .setId(12345)
                .setType(Group.Type.CLUSTER)
                .setCluster(ClusterInfo.newBuilder()
                        .setName("TestCluster")
                        .setDisplayName("TestCluster")
                        .setClusterHeadroomTemplateId(averageTemplateId)
                        .build())
                .build();

        TemplateInfo templateInfo = TemplateInfo.newBuilder()
                .setName("AVG:TestCluster for last 10 days")
                .build();

        Template template = Template.newBuilder()
                .setId(averageTemplateId)
                .setTemplateInfo(templateInfo)
                .build();

        when(templatesDao.getTemplate(averageTemplateId)).thenReturn(Optional.of(template));

        planProjectExecutor.createClusterPlanInstance(Collections.singleton(groupWithHeadroomTemplateId),
                PlanProjectScenario.getDefaultInstance(), PlanProjectType.CLUSTER_HEADROOM);
        // The user selects a template which is an average one thus the respective code is executed (where
        // headroomTemplateInfo.isPresent) and editTemplate() is called while getFilteredTemplates() is not called
        verify(templatesDao).editTemplate(eq(averageTemplateId), any());
        verify(templatesDao, never()).getFilteredTemplates(any());
        // usedTemplate is not updated, thus the part of code where usedTemplate != null is not executed and
        // updateClusterHeadroomTemplate() is not called
        verify(groupServiceMole, never()).updateClusterHeadroomTemplate(any(UpdateClusterHeadroomTemplateRequest.class));
    }



    /**
     *  When calling createPlanInstanceWithClusterHeadroomTemplate, and the group does not have
     * cluster information. In this case, we don't identify the template as a headroomVM or
     * average template and code for changing the template is not called at all
     *
     * @throws Exception
     */
    @Test
    public void testCreatePlanInstanceWithoutClusterInfo() throws Exception {
        Group groupWithoutHeadroomClusterInfo = Group.newBuilder()
                .setId(12345)
                .setType(Group.Type.CLUSTER)
                .build();

        when(templatesDao.createTemplate(any())).thenReturn(Template.getDefaultInstance());

        planProjectExecutor.createClusterPlanInstance(Collections.singleton(groupWithoutHeadroomClusterInfo),
                PlanProjectScenario.getDefaultInstance(), PlanProjectType.CLUSTER_HEADROOM);
        verify(groupServiceMole, never()).updateClusterHeadroomTemplate(any(UpdateClusterHeadroomTemplateRequest.class));
    }

    /**
     * When calling createPlanInstanceWithClusterHeadroomTemplate, and the group does not have
     * a headroom template ID. In this case, we don't identify the template as a headroomVM or
     * average template and code for changing the template is not called at all
     *
     * @throws Exception
     */
    @Test
    public void testCreatePlanInstanceWithoutClusterHeadroomTemplate() throws Exception {
        Group groupWithHeadroomTemplateId = Group.newBuilder()
                .setId(12345)
                .setType(Group.Type.CLUSTER)
                .setCluster(ClusterInfo.newBuilder()
                        .build())
                .build();
        when(templatesDao.createTemplate(any())).thenReturn(Template.getDefaultInstance());

        planProjectExecutor.createClusterPlanInstance(Collections.singleton(groupWithHeadroomTemplateId),
                PlanProjectScenario.getDefaultInstance(), PlanProjectType.CLUSTER_HEADROOM);
        verify(templatesDao).getTemplate(anyLong());
        verify(templatesDao, never()).createTemplate(any());
        verify(groupServiceMole, never()).updateClusterHeadroomTemplate(any(UpdateClusterHeadroomTemplateRequest.class));
    }

    /**
     * Create a plan project of type CLUSTER_HEADROOM, with 2 scenarios
     *
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
