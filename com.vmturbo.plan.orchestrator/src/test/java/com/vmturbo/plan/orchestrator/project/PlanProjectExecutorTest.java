package com.vmturbo.plan.orchestrator.project;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
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

import io.grpc.Channel;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTOMoles;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectInfo.PlanProjectScenario;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template.Type;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplatesFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadInfoResponse;
import com.vmturbo.common.protobuf.stats.Stats.SystemLoadRecord;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.plan.orchestrator.plan.NoSuchObjectException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.plan.PlanRpcService;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.plan.orchestrator.templates.exceptions.DuplicateTemplateException;
import com.vmturbo.plan.orchestrator.templates.exceptions.IllegalTemplateOperationException;

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
    }

    @Test
    public void testExecutePlanOnePlanInstancePerClusterPerScenario() throws Exception {
        final PlanProjectOuterClass.PlanProject planProject = createHeadroomPlanProjectWithTwoScenarios();

        // 2 clusters
        List<Grouping> groupList = new ArrayList<>();
        groupList.add(Grouping.newBuilder()
                .setId(100)
                .addExpectedTypes(MemberType.newBuilder().setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
                .setDefinition(GroupDefinition.newBuilder()
                                .setType(GroupType.COMPUTE_HOST_CLUSTER))
                .build());
        groupList.add(Grouping.newBuilder()
                        .setId(101)
                        .addExpectedTypes(MemberType.newBuilder().setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
                        .setDefinition(GroupDefinition.newBuilder()
                                        .setType(GroupType.COMPUTE_HOST_CLUSTER))
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

        when(templatesDao.getClusterHeadroomTemplateForGroup(anyLong())).thenReturn(Optional.empty());

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
        List<Grouping> groupList = new ArrayList<>();
        Arrays.asList(1, 2, 3).forEach(i -> groupList.add(
                        Grouping.newBuilder()
                        .setId(i)
                        .addExpectedTypes(MemberType.newBuilder().setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
                        .setDefinition(GroupDefinition.newBuilder()
                                        .setType(GroupType.COMPUTE_HOST_CLUSTER))
                        .build()
        ));

        when(groupServiceMole.getGroups(any(GroupDTO.GetGroupsRequest.class)))
            .thenReturn(groupList);

        when(planDao.createPlanInstance(any(Scenario.class), eq(PlanProjectType.CLUSTER_HEADROOM)))
            .thenReturn(PlanDTO.PlanInstance.newBuilder()
                .setPlanId(IdentityGenerator.next())
                .setStatus(PlanStatus.READY)
                .build());

        when(templatesDao.getClusterHeadroomTemplateForGroup(anyLong())).thenReturn(Optional.empty());

        PlanProjectOuterClass.PlanProject planProject = createHeadroomPlanProjectWithTwoScenarios();
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
        when(statsHistoryServiceMole.getSystemLoadInfo(any()))
            .thenReturn(Collections.singletonList(SystemLoadInfoResponse.newBuilder()
                .addRecord(SystemLoadRecord.newBuilder()
                    .setPropertyType("VCPU")
                    .setAvgValue(10)
                    .setRelationType(0))
                .build()));

        long selectedTemplateId = 3333;
        Grouping groupWithHeadroomTemplateId = Grouping.newBuilder()
        .setId(12345)
        .addExpectedTypes(MemberType.newBuilder().setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
        .setDefinition(GroupDefinition.newBuilder()
                        .setType(GroupType.COMPUTE_HOST_CLUSTER))
        .build();


        when(templatesDao.getClusterHeadroomTemplateForGroup(groupWithHeadroomTemplateId.getId()))
            .thenReturn(Optional.of(Template.getDefaultInstance()));

        planProjectExecutor.createClusterPlanInstance(Collections.singleton(groupWithHeadroomTemplateId),
                PlanProjectScenario.getDefaultInstance(), PlanProjectType.CLUSTER_HEADROOM);

        // The use selects a template which is not a headroomVM or an average template and the code regarding
        // headroomVM or average is not executed, thus editTemplate() and getFilteredTemplates() are not called
        verify(templatesDao, never()).editTemplate(eq(selectedTemplateId), any());
        verify(templatesDao).getFilteredTemplates(any());
        verify(templatesDao, never()).setOrUpdateHeadroomTemplateForCluster(anyLong(), anyLong());
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
        when(statsHistoryServiceMole.getSystemLoadInfo(any()))
            .thenReturn(Collections.singletonList(SystemLoadInfoResponse.newBuilder()
                .addRecord(SystemLoadRecord.newBuilder()
                    .setPropertyType("VCPU")
                    .setAvgValue(10)
                    .setRelationType(0))
                .build()));

        long averageTemplateId = 3333;
        Grouping groupWithHeadroomTemplateId = Grouping.newBuilder()
            .setId(12345L)
            .addExpectedTypes(MemberType.newBuilder().setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
            .setDefinition(GroupDefinition.newBuilder()
                            .setDisplayName("TestCluster")
                            .setType(GroupType.COMPUTE_HOST_CLUSTER))
            .build();

        TemplateInfo templateInfo = TemplateInfo.newBuilder()
                .setName("AVG:TestCluster for last 10 days")
                .build();

        Template template = Template.newBuilder()
                .setId(averageTemplateId)
                .setTemplateInfo(templateInfo)
                .build();

        when(templatesDao.getTemplate(averageTemplateId)).thenReturn(Optional.of(template));

        when(templatesDao.getClusterHeadroomTemplateForGroup(eq(12345L)))
            .thenReturn(Optional.of(template));

        planProjectExecutor.createClusterPlanInstance(Collections.singleton(groupWithHeadroomTemplateId),
                PlanProjectScenario.getDefaultInstance(), PlanProjectType.CLUSTER_HEADROOM);
        // The user selects a template which is an average one thus the respective code is executed (where
        // headroomTemplateInfo.isPresent) and editTemplate() is called while getFilteredTemplates() is not called
        verify(templatesDao).editTemplate(eq(averageTemplateId), any(), any());
        verify(templatesDao).getFilteredTemplates(TemplatesFilter.newBuilder()
            .addTemplateName(StringConstants.CLUSTER_HEADROOM_DEFAULT_TEMPLATE_NAME).build());
        verify(templatesDao, never()).setOrUpdateHeadroomTemplateForCluster(anyLong(), anyLong());
    }

    /**
     *  When calling createPlanInstanceWithClusterHeadroomTemplate, and the group does not have
     * cluster information. In this case the code for changing the template is called
     *
     * @throws Exception
     */
    @Test
    public void testCreatePlanInstanceWithoutClusterInfo() throws Exception {
        Grouping groupWithoutHeadroomClusterInfo = Grouping.newBuilder()
            .setId(12345)
            .addExpectedTypes(MemberType.newBuilder().setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
            .setDefinition(GroupDefinition.newBuilder()
                            .setDisplayName("TestCluster")
                            .setType(GroupType.COMPUTE_HOST_CLUSTER))
            .build();

        when(templatesDao.getClusterHeadroomTemplateForGroup(anyLong())).thenReturn(Optional.empty());
        when(templatesDao.createTemplate(any())).thenReturn(Template.getDefaultInstance());

        planProjectExecutor.createClusterPlanInstance(Collections.singleton(groupWithoutHeadroomClusterInfo),
                PlanProjectScenario.getDefaultInstance(), PlanProjectType.CLUSTER_HEADROOM);
    }

    /**
     * When calling createPlanInstanceWithClusterHeadroomTemplate, and the group does not have
     * a headroom template ID. In this case, the code for creating the template is called
     *
     * @throws Exception
     */
    @Test
    public void testCreatePlanInstanceWithoutClusterHeadroomTemplate() throws Exception {
        when(statsHistoryServiceMole.getSystemLoadInfo(any()))
            .thenReturn(Collections.singletonList(SystemLoadInfoResponse.newBuilder()
                .addRecord(SystemLoadRecord.newBuilder()
                    .setPropertyType("VCPU")
                    .setAvgValue(10)
                    .setRelationType(0))
                .build()));

        Grouping groupWithHeadroomTemplateId = Grouping.newBuilder()
            .setId(12345)
            .addExpectedTypes(MemberType.newBuilder().setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
            .setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.COMPUTE_HOST_CLUSTER))
            .build();

        when(templatesDao.createTemplate(any(), any())).thenReturn(Template.getDefaultInstance());

        when(templatesDao.getClusterHeadroomTemplateForGroup(anyLong())).thenReturn(Optional.empty());

        planProjectExecutor.createClusterPlanInstance(Collections.singleton(groupWithHeadroomTemplateId),
                PlanProjectScenario.getDefaultInstance(), PlanProjectType.CLUSTER_HEADROOM);
        verify(templatesDao).getClusterHeadroomTemplateForGroup(anyLong());
        verify(templatesDao).createTemplate(any(), any());
        verify(templatesDao).setOrUpdateHeadroomTemplateForCluster(anyLong(), anyLong());
    }

    /**
     * Create a plan project of type CLUSTER_HEADROOM, with 2 scenarios
     *
     * @return a plan project
     */
    private PlanProjectOuterClass.PlanProject createHeadroomPlanProjectWithTwoScenarios() {
        ScenarioOuterClass.ScenarioChange.TopologyAddition topologyAddition =
                ScenarioOuterClass.ScenarioChange.TopologyAddition.newBuilder()
                        .addChangeApplicationDays(0)
                        .build();
        ScenarioOuterClass.ScenarioChange scenarioChange1 = ScenarioOuterClass.ScenarioChange.newBuilder()
                .setTopologyAddition(topologyAddition)
                .build();
        ScenarioOuterClass.ScenarioChange scenarioChange2 = ScenarioOuterClass.ScenarioChange.newBuilder()
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
        PlanProjectOuterClass.PlanProjectInfo planProjectInfo = PlanProjectOuterClass.PlanProjectInfo.newBuilder()
                .setName("Test Project")
                .setType(PlanProjectType.CLUSTER_HEADROOM)
                .addScenarios(scenario1)
                .addScenarios(scenario2)
                .build();
        PlanProjectOuterClass.PlanProject planProject = PlanProjectOuterClass.PlanProject.newBuilder()
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
        Set<Grouping> groupSet1 = new HashSet<>();
        for (int i = 0; i < numberOfClusters; i++) {
            Grouping group = Grouping.newBuilder()
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
        Set<Grouping> groupSet1 = new HashSet<>();
        for (int i = 0; i < numberOfClusters; i++) {
            Grouping group = Grouping.newBuilder()
                    .setId(i)
                    .build();
            groupSet1.add(group);
        }
        groupSet1 = planProjectExecutor.restrictNumberOfClusters(groupSet1);

        Set<Grouping> groupSet2 = new HashSet<>();
        for (int i = 0; i < numberOfClusters; i++) {
            Grouping group = Grouping.newBuilder()
                    .setId(i)
                    .build();
            groupSet2.add(group);
        }
        groupSet2 = planProjectExecutor.restrictNumberOfClusters(groupSet2);

        assertTrue(groupSet1.containsAll(groupSet2));
        assertEquals(numberOfClusters, groupSet1.size());
        assertEquals(numberOfClusters, groupSet2.size());
    }

    /**
     * Test {@link PlanProjectExecutor#updateClusterHeadroomTemplate(Grouping, Optional)}
     * with sufficient systemLoad data.
     *
     * @throws NoSuchObjectException if default cluster headroom template not found
     * @throws IllegalTemplateOperationException if the operation is not allowed created template
     * @throws DuplicateTemplateException if there are errors when a user tries to create templates
     */
    @Test
    public void testUpdateClusterHeadroomTemplateWithSufficientSystemLoadData()
        throws NoSuchObjectException, IllegalTemplateOperationException, DuplicateTemplateException {

        when(statsHistoryServiceMole.getSystemLoadInfo(any()))
            .thenReturn(Collections.singletonList(SystemLoadInfoResponse.newBuilder()
                .addRecord(SystemLoadRecord.newBuilder()
                    .setPropertyType("VCPU")
                    .setAvgValue(10)
                    .setRelationType(0))
                .build()));

        final long defaultHeadroomTemplateId = 100;
        final Optional<Template> defaultHeadroomTemplate = Optional.of(
            Template.newBuilder().setId(defaultHeadroomTemplateId)
                .setTemplateInfo(TemplateInfo.newBuilder()
                    .setName(StringConstants.CLUSTER_HEADROOM_DEFAULT_TEMPLATE_NAME)).build());

        final long avgHeadroomTemplateId = 101;
        final Template avgHeadroomTemplate = Template.newBuilder().setId(avgHeadroomTemplateId)
            .setTemplateInfo(TemplateInfo.newBuilder()
                .setName("AVG:test_cluster for last 10 days")).build();

        final Grouping cluster = Grouping.newBuilder().setId(123)
            .addExpectedTypes(MemberType.newBuilder().setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
            .setDefinition(GroupDefinition.newBuilder()
                .setDisplayName("test_cluster")
                .setType(GroupType.COMPUTE_HOST_CLUSTER))
            .build();

        // 1. cluster template exists in the db
        // 1.1. associated template is avg template
        when(templatesDao.getClusterHeadroomTemplateForGroup(cluster.getId()))
            .thenReturn(Optional.of(avgHeadroomTemplate));
        planProjectExecutor.updateClusterHeadroomTemplate(cluster, defaultHeadroomTemplate);

        verify(templatesDao).getClusterHeadroomTemplateForGroup(cluster.getId());
        verify(templatesDao).editTemplate(eq(avgHeadroomTemplateId), any(), any());
        verify(templatesDao, never()).createTemplate(any());
        verify(templatesDao, never()).setOrUpdateHeadroomTemplateForCluster(anyLong(), anyLong());

        // 1.2. associated template is default headroom template
        reset(templatesDao);
        reset(groupServiceMole);
        when(templatesDao.getClusterHeadroomTemplateForGroup(cluster.getId()))
            .thenReturn(defaultHeadroomTemplate);
        when(templatesDao.createTemplate(any(), any())).thenReturn(avgHeadroomTemplate);
        planProjectExecutor.updateClusterHeadroomTemplate(cluster, defaultHeadroomTemplate);

        verify(templatesDao).getClusterHeadroomTemplateForGroup(cluster.getId());
        verify(templatesDao, never()).editTemplate(anyLong(), any());
        verify(templatesDao, never()).editTemplate(anyLong(), any(), any());
        verify(templatesDao).createTemplate(any(), any());
        verify(templatesDao).setOrUpdateHeadroomTemplateForCluster(cluster.getId(), avgHeadroomTemplateId);

        // 1.3. associated template is not avg template or default headroom template
        reset(templatesDao);
        reset(groupServiceMole);
        when(templatesDao.getClusterHeadroomTemplateForGroup(cluster.getId()))
            .thenReturn(Optional.of(Template.newBuilder().setId(1)
                .setTemplateInfo(TemplateInfo.newBuilder()
                    .setName("associated template is not avg template")).build()));
        planProjectExecutor.updateClusterHeadroomTemplate(cluster, defaultHeadroomTemplate);

        verify(templatesDao).getClusterHeadroomTemplateForGroup(cluster.getId());
        verify(templatesDao, never()).editTemplate(anyLong(), any());
        verify(templatesDao, never()).editTemplate(anyLong(), any(), any());
        verify(templatesDao).createTemplate(any(), any());
        verify(templatesDao, never()).setOrUpdateHeadroomTemplateForCluster(anyLong(), anyLong());

        // 2. associated template doesn't exist in the db
        reset(templatesDao);
        reset(groupServiceMole);
        when(templatesDao.getClusterHeadroomTemplateForGroup(cluster.getId()))
            .thenReturn(Optional.empty());
        when(templatesDao.createTemplate(any(), any())).thenReturn(avgHeadroomTemplate);
        planProjectExecutor.updateClusterHeadroomTemplate(cluster, defaultHeadroomTemplate);

        verify(templatesDao).getClusterHeadroomTemplateForGroup(cluster.getId());
        verify(templatesDao, never()).editTemplate(anyLong(), any());
        verify(templatesDao, never()).editTemplate(anyLong(), any(), any());
        verify(templatesDao).createTemplate(any(), any());
        verify(templatesDao).setOrUpdateHeadroomTemplateForCluster(cluster.getId(), avgHeadroomTemplateId);

        // 3. cluster doesn't have clusterHeadroomTemplateId (by default it will be 0)
        reset(templatesDao);
        reset(groupServiceMole);
        when(templatesDao.getClusterHeadroomTemplateForGroup(cluster.getId())).thenReturn(Optional.empty());
        when(templatesDao.createTemplate(any(), any())).thenReturn(avgHeadroomTemplate);
        planProjectExecutor.updateClusterHeadroomTemplate(cluster, defaultHeadroomTemplate);

        verify(templatesDao).getClusterHeadroomTemplateForGroup(cluster.getId());
        verify(templatesDao, never()).editTemplate(anyLong(), any());
        verify(templatesDao, never()).editTemplate(anyLong(), any(), any());
        verify(templatesDao).createTemplate(any(), any());
        verify(templatesDao).setOrUpdateHeadroomTemplateForCluster(cluster.getId(), avgHeadroomTemplateId);
    }

    /**
     * Test {@link PlanProjectExecutor#updateClusterHeadroomTemplate(Grouping, Optional)}
     * with insufficient systemLoad data.
     *
     * @throws NoSuchObjectException if default cluster headroom template not found
     * @throws IllegalTemplateOperationException if the operation is not allowed created template
     * @throws DuplicateTemplateException if there are errors when a user tries to create templates
     */
    @Test
    public void testUpdateClusterHeadroomTemplateWithInsufficientSystemLoadData()
        throws NoSuchObjectException, IllegalTemplateOperationException, DuplicateTemplateException {

        when(statsHistoryServiceMole.getSystemLoadInfo(any())).thenReturn(Collections.emptyList());

        final long defaultHeadroomTemplateId = 100;
        final Optional<Template> defaultHeadroomTemplate = Optional.of(
            Template.newBuilder().setId(defaultHeadroomTemplateId)
                .setTemplateInfo(TemplateInfo.newBuilder()
                    .setName(StringConstants.CLUSTER_HEADROOM_DEFAULT_TEMPLATE_NAME)).build());

        final Grouping cluster = Grouping.newBuilder().setId(123)
            .addExpectedTypes(MemberType.newBuilder().setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
            .setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.COMPUTE_HOST_CLUSTER))
            .build();

        // cluster template exists in the db, updateClusterHeadroomTemplate will not be invoked
        when(templatesDao.getClusterHeadroomTemplateForGroup(cluster.getId()))
            .thenReturn(Optional.of(Template.getDefaultInstance()));
        planProjectExecutor.updateClusterHeadroomTemplate(cluster, defaultHeadroomTemplate);

        verify(templatesDao).getClusterHeadroomTemplateForGroup(cluster.getId());
        verify(templatesDao, never()).editTemplate(anyLong(), any());
        verify(templatesDao, never()).createTemplate(any());
        verify(templatesDao, never()).setOrUpdateHeadroomTemplateForCluster(anyLong(), anyLong());

        // cluster template doesn't exist in the db
        reset(templatesDao);
        reset(groupServiceMole);
        when(templatesDao.getClusterHeadroomTemplateForGroup(cluster.getId()))
            .thenReturn(Optional.empty());
        planProjectExecutor.updateClusterHeadroomTemplate(cluster, defaultHeadroomTemplate);

        verify(templatesDao).getClusterHeadroomTemplateForGroup(cluster.getId());
        verify(templatesDao, never()).editTemplate(anyLong(), any());
        verify(templatesDao, never()).createTemplate(any());
        verify(templatesDao).setOrUpdateHeadroomTemplateForCluster(cluster.getId(), defaultHeadroomTemplateId);

        // cluster doesn't have clusterHeadroomTemplateId (by default it will be 0)
        reset(templatesDao);
        reset(groupServiceMole);
        when(templatesDao.getClusterHeadroomTemplateForGroup(cluster.getId()))
            .thenReturn(Optional.empty());
        planProjectExecutor.updateClusterHeadroomTemplate(cluster, defaultHeadroomTemplate);

        verify(templatesDao).getClusterHeadroomTemplateForGroup(cluster.getId());
        verify(templatesDao, never()).editTemplate(anyLong(), any());
        verify(templatesDao, never()).createTemplate(any());
        verify(templatesDao).setOrUpdateHeadroomTemplateForCluster(cluster.getId(), defaultHeadroomTemplateId);
    }

    /**
     * Test {@link PlanProjectExecutor#updateClusterHeadroomTemplate(Grouping, Optional)}
     * with insufficient systemLoad data and with no default headroom template.
     *
     * @throws NoSuchObjectException if default cluster headroom template not found
     * @throws IllegalTemplateOperationException if the operation is not allowed created template
     * @throws DuplicateTemplateException if there are errors when a user tries to create templates
     */
    @Test(expected = NoSuchObjectException.class)
    public void testUpdateClusterHeadroomTemplateNoDefaultHeadroomTemplate()
        throws NoSuchObjectException, IllegalTemplateOperationException, DuplicateTemplateException {

        when(statsHistoryServiceMole.getSystemLoadInfo(any())).thenReturn(Collections.emptyList());

        when(templatesDao.getClusterHeadroomTemplateForGroup(anyLong())).thenReturn(Optional.empty());

        final Grouping cluster = Grouping.newBuilder().setId(123)
            .addExpectedTypes(MemberType.newBuilder().setEntity(ApiEntityType.PHYSICAL_MACHINE.typeNumber()))
            .setDefinition(GroupDefinition.newBuilder()
                .setType(GroupType.COMPUTE_HOST_CLUSTER))
            .build();
        planProjectExecutor.updateClusterHeadroomTemplate(cluster, Optional.empty());

        verify(templatesDao, never()).getClusterHeadroomTemplateForGroup(anyLong());
        verify(templatesDao, never()).editTemplate(anyLong(), any());
        verify(templatesDao, never()).createTemplate(any());
        verify(templatesDao, never()).setOrUpdateHeadroomTemplateForCluster(anyLong(), anyLong());
    }
}
