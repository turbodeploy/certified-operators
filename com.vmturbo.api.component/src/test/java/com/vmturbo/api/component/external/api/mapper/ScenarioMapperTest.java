package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper.UIEntityType;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.service.PoliciesService;
import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.scenario.AddObjectApiDTO;
import com.vmturbo.api.dto.scenario.ConfigChangesApiDTO;
import com.vmturbo.api.dto.scenario.LoadChangesApiDTO;
import com.vmturbo.api.dto.scenario.RemoveConstraintApiDTO;
import com.vmturbo.api.dto.scenario.RemoveObjectApiDTO;
import com.vmturbo.api.dto.scenario.ReplaceObjectApiDTO;
import com.vmturbo.api.dto.scenario.ScenarioApiDTO;
import com.vmturbo.api.dto.scenario.TopologyChangesApiDTO;
import com.vmturbo.api.dto.scenario.UtilizationApiDTO;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.target.TargetApiDTO;
import com.vmturbo.api.dto.template.TemplateApiDTO;
import com.vmturbo.api.enums.ConstraintType;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse.Builder;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticGroupMembers;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceImplBase;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.DetailsCase;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.IgnoreConstraint;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.UtilizationLevel;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyRemoval;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.TopologyReplace;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.TemplateDTO.Template;
import com.vmturbo.common.protobuf.plan.TemplateDTO.TemplateInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.components.api.test.GrpcTestServer;

import io.grpc.stub.StreamObserver;

public class ScenarioMapperTest {
    private static final String SCENARIO_NAME = "MyScenario";
    private static final long SCENARIO_ID = 0xdeadbeef;

    private RepositoryApi repositoryApi;

    private TemplatesUtils templatesUtils;

    private PoliciesService policiesService;

    private ScenarioMapper scenarioMapper;

    private SettingsManagerMapping settingsManagerMapping = mock(SettingsManagerMapping.class);

    private SettingsMapper settingsMapper = mock(SettingsMapper.class);

    private TestGroupService testGroupService = new TestGroupService();

    private GrpcTestServer server = GrpcTestServer.newServer(testGroupService);

    @Before
    public void setup() throws IOException {
        repositoryApi = Mockito.mock(RepositoryApi.class);
        templatesUtils = Mockito.mock(TemplatesUtils.class);
        policiesService = Mockito.mock(PoliciesService.class);

        // Return empty by default to keep NPE's at bay.
        when(repositoryApi.getServiceEntitiesById(any()))
            .thenReturn(Collections.emptyMap());


        server.start();
        scenarioMapper = new ScenarioMapper(repositoryApi,
                templatesUtils, settingsManagerMapping, settingsMapper, policiesService,
                GroupServiceGrpc.newBlockingStub(server.getChannel()));
    }

    @Test
    public void testAdditionChange() {
        TopologyChangesApiDTO topoChanges = new TopologyChangesApiDTO();
        AddObjectApiDTO dto = new AddObjectApiDTO();
        dto.setProjectionDays(Collections.singletonList(2));
        dto.setTarget(entity(1));
        dto.setCount(6);
        topoChanges.setAddList(Collections.singletonList(dto));

        ScenarioInfo info = scenarioMapper.toScenarioInfo(SCENARIO_NAME, scenarioApiDTO(topoChanges));
        assertEquals(SCENARIO_NAME, info.getName());
        assertEquals(1, info.getChangesCount());
        assertEquals(DetailsCase.TOPOLOGY_ADDITION, info.getChanges(0).getDetailsCase());

        TopologyAddition addition = info.getChanges(0).getTopologyAddition();
        assertEquals(6, addition.getAdditionCount());
        assertEquals(1, addition.getEntityId());
        assertEquals(Collections.singletonList(2), addition.getChangeApplicationDaysList());
    }

    @Test
    public void testTemplateAdditionChange() {
        TopologyChangesApiDTO topoChanges = new TopologyChangesApiDTO();
        AddObjectApiDTO dto = new AddObjectApiDTO();
        dto.setProjectionDays(Collections.singletonList(2));
        dto.setTarget(template(1));
        dto.setCount(6);
        topoChanges.setAddList(Collections.singletonList(dto));

        Template template = Template.newBuilder()
            .setId(1)
            .setTemplateInfo(TemplateInfo.newBuilder()
                .setName("template"))
            .build();
        when(templatesUtils.getTemplatesByIds(eq(Sets.newHashSet(1L))))
            .thenReturn(Sets.newHashSet(template));

        ScenarioInfo info = scenarioMapper.toScenarioInfo(SCENARIO_NAME, scenarioApiDTO(topoChanges));

        assertEquals(SCENARIO_NAME, info.getName());
        assertEquals(1, info.getChangesCount());
        assertEquals(DetailsCase.TOPOLOGY_ADDITION, info.getChanges(0).getDetailsCase());

        TopologyAddition addition = info.getChanges(0).getTopologyAddition();
        assertEquals(6, addition.getAdditionCount());
        assertEquals(1, addition.getTemplateId());
        assertEquals(Collections.singletonList(2), addition.getChangeApplicationDaysList());

    }

    @Test
    public void testRemovalChange() {
        TopologyChangesApiDTO topoChanges = new TopologyChangesApiDTO();
        RemoveObjectApiDTO dto = new RemoveObjectApiDTO();
        dto.setProjectionDay(2);
        dto.setTarget(entity(1));
        topoChanges.setRemoveList(Collections.singletonList(dto));

        ScenarioInfo info = scenarioMapper.toScenarioInfo(SCENARIO_NAME, scenarioApiDTO(topoChanges));
        assertEquals(SCENARIO_NAME, info.getName());
        assertEquals(1, info.getChangesCount());
        assertEquals(DetailsCase.TOPOLOGY_REMOVAL, info.getChanges(0).getDetailsCase());

        TopologyRemoval removal = info.getChanges(0).getTopologyRemoval();
        assertEquals(2, removal.getChangeApplicationDay());
        assertEquals(1, removal.getEntityId());
    }

    @Test
    public void testInvalidChange() {
        ScenarioApiDTO scenarioDto = new ScenarioApiDTO();
        scenarioDto.setScope(Collections.singletonList(entity(1)));

        ScenarioInfo info = scenarioMapper.toScenarioInfo(SCENARIO_NAME, scenarioDto);
        assertEquals(0, info.getChangesCount());
    }

    @Test
    public void testReplaceChange() {
        TopologyChangesApiDTO topoChanges = new TopologyChangesApiDTO();
        ReplaceObjectApiDTO dto = new ReplaceObjectApiDTO();
        dto.setProjectionDay(5);
        dto.setTarget(entity(1));
        dto.setTemplate(template(2));
        topoChanges.setReplaceList(Collections.singletonList(dto));

        ScenarioInfo info = scenarioMapper.toScenarioInfo(SCENARIO_NAME, scenarioApiDTO(topoChanges));
        assertEquals(SCENARIO_NAME, info.getName());
        assertEquals(1, info.getChangesCount());

        assertEquals(DetailsCase.TOPOLOGY_REPLACE, info.getChanges(0).getDetailsCase());
        TopologyReplace replace = info.getChanges(0).getTopologyReplace();
        assertEquals(5, replace.getChangeApplicationDay());
        assertEquals(1, replace.getRemoveEntityId());
        assertEquals(2, replace.getAddTemplateId());
    }

    @Test
    public void testMultiplesChanges() {
        AddObjectApiDTO addDto = new AddObjectApiDTO();
        addDto.setProjectionDays(Collections.singletonList(5));
        addDto.setTarget(entity(1));
        addDto.setCount(9);

        RemoveObjectApiDTO removeDto = new RemoveObjectApiDTO();
        removeDto.setProjectionDay(1);
        removeDto.setTarget(entity(2));

        TopologyChangesApiDTO topoChanges = new TopologyChangesApiDTO();
        topoChanges.setAddList(Collections.singletonList(addDto));
        topoChanges.setRemoveList(Collections.singletonList(removeDto));

        ScenarioInfo info = scenarioMapper
            .toScenarioInfo(SCENARIO_NAME, scenarioApiDTO(topoChanges));
        assertEquals(2, info.getChangesCount());

        assertEquals(DetailsCase.TOPOLOGY_ADDITION, info.getChanges(0).getDetailsCase());
        TopologyAddition addition = info.getChanges(0).getTopologyAddition();
        assertEquals(1, addition.getEntityId());

        assertEquals(DetailsCase.TOPOLOGY_REMOVAL, info.getChanges(1).getDetailsCase());
        TopologyRemoval removal = info.getChanges(1).getTopologyRemoval();
        assertEquals(2, removal.getEntityId());
    }

    @Test
    public void testSettingOverride() {
        final SettingApiDTO setting = new SettingApiDTO();
        setting.setUuid("foo");
        setting.setValue("value");

        when(settingsMapper.toProtoSettings(Collections.singletonList(setting)))
            .thenReturn(ImmutableMap.of("foo", Setting.newBuilder()
                    .setSettingSpecName("foo")
                    .setStringSettingValue(StringSettingValue.newBuilder().setValue("value"))
                    .build()));

        // No plan setting mappings
        when(settingsManagerMapping.convertFromPlanSetting(any()))
            .thenAnswer(invocation -> invocation.getArguments()[0]);

        final ScenarioInfo scenarioInfo = getScenarioInfo(Collections.singletonList(setting), null);
        assertThat(scenarioInfo.getChangesCount(), is(1));
        final ScenarioChange change = scenarioInfo.getChanges(0);
        assertTrue(change.hasSettingOverride());
        assertTrue(change.getSettingOverride().hasSetting());
        final Setting overridenSetting = change.getSettingOverride().getSetting();
        assertThat(overridenSetting.getSettingSpecName(), is("foo"));
        assertTrue(overridenSetting.hasStringSettingValue());
        assertThat(overridenSetting.getStringSettingValue().getValue(), is("value"));
    }

    @Test
    public void testSettingOverridePlanSettingMapping() {
        final SettingApiDTO setting = new SettingApiDTO();
        setting.setUuid("foo");
        setting.setValue("value");

        final SettingApiDTO convertedSetting = new SettingApiDTO();
        convertedSetting.setUuid("foo");
        convertedSetting.setValue("1.2f");

        when(settingsMapper.toProtoSettings(Collections.singletonList(convertedSetting)))
                .thenReturn(ImmutableMap.of("foo", Setting.newBuilder()
                        .setSettingSpecName("foo")
                        .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(1.2f))
                        .build()));

        // Convert the input setting into a numeric setting
        when(settingsManagerMapping.convertFromPlanSetting(Collections.singletonList(setting)))
                .thenReturn(Collections.singletonList(convertedSetting));

        final ScenarioInfo scenarioInfo = getScenarioInfo(Collections.singletonList(setting), null);
        assertThat(scenarioInfo.getChangesCount(), is(1));
        final ScenarioChange change = scenarioInfo.getChanges(0);
        assertTrue(change.hasSettingOverride());
        assertTrue(change.getSettingOverride().hasSetting());
        final Setting overridenSetting = change.getSettingOverride().getSetting();
        assertThat(overridenSetting.getSettingSpecName(), is("foo"));
        assertTrue(overridenSetting.hasNumericSettingValue());
        assertThat(overridenSetting.getNumericSettingValue().getValue(), is(1.2f));
    }

    @Test
    public void testSettingOverrideUnknownSetting() {
        final SettingApiDTO setting = new SettingApiDTO();
        setting.setUuid("unknown");
        setting.setValue("value");
        final ScenarioInfo scenarioInfo = getScenarioInfo(Collections.singletonList(setting), null);
        assertThat(scenarioInfo.getChangesCount(), is(0));
    }

    /**
     * Tests converting of scenario with desired state setting to ScenarioApiDto.
     */
    @Test
    public void testSettingOverrideToApiDto() {
        final Scenario scenario = buildScenario(buildNumericSettingOverride("foo", 1.2f));

        // Pass-through plan settings conversion
        when(settingsManagerMapping.convertToPlanSetting(any()))
            .thenAnswer(invocation -> invocation.getArguments()[0]);

        final ScenarioApiDTO scenarioApiDTO = scenarioMapper.toScenarioApiDTO(scenario);
        final SettingApiDTO apiFoo = scenarioApiDTO.getConfigChanges().getAutomationSettingList().get(0);
        assertSettingApiDTOHasSetting(apiFoo, "1.2", "foo");

        verify(settingsManagerMapping).convertToPlanSetting(any());
    }

    @Test
    public void testSettingOverrideToApiDtoWithPlanSetting() {
        final Scenario scenario = buildScenario(buildNumericSettingOverride("foo", 1.2f));

        final SettingApiDTO convertedSetting = new SettingApiDTO();

        // Plan settings conversion substitutes the setting.
        when(settingsManagerMapping.convertToPlanSetting(any()))
                .thenAnswer(invocation -> Collections.singletonList(convertedSetting));

        final ScenarioApiDTO scenarioApiDTO = scenarioMapper.toScenarioApiDTO(scenario);
        final SettingApiDTO apiFoo = scenarioApiDTO.getConfigChanges().getAutomationSettingList().get(0);
        assertThat(apiFoo, is(convertedSetting));

        verify(settingsManagerMapping).convertToPlanSetting(any());
    }

    private void assertSettingApiDTOHasSetting(@Nonnull SettingApiDTO setting,
                                               @Nonnull String value, @Nonnull String settingName) {
        Assert.assertEquals(value, setting.getValue());
        Assert.assertEquals(settingName, setting.getUuid());
    }

    /**
     * Tests converting of ScenarioApiDto without config changes to ScenarioInfo.
     */
    @Test
    public void testToScenarioInfoWithoutConfigChanges() {
        final ScenarioApiDTO dto = new ScenarioApiDTO();
        dto.setConfigChanges(null);
        final ScenarioInfo scenarioInfo = scenarioMapper.toScenarioInfo("name", dto);
        Assert.assertTrue(scenarioInfo.getChangesList().isEmpty());
    }

    /**
     * Tests converting of ScenarioApiDto with empty config changes to ScenarioInfo.
     */
    @Test
    public void testToScenarioInfoWithEmptyConfigChanges() {
        final ScenarioInfo scenarioInfo = getScenarioInfo(null, null);
        Assert.assertTrue(scenarioInfo.getChangesList().stream().noneMatch(ScenarioChange::hasSettingOverride));
    }

    /**
     * Tests converting of ScenarioApiDto with empty automation settings list to ScenarioInfo.
     */
    @Test
    public void testToScenarioInfoWithEmptyAutomationSettingsList() {
        final ScenarioInfo scenarioInfo = getScenarioInfo(Collections.emptyList(), null);
        Assert.assertTrue(scenarioInfo.getChangesList().stream().noneMatch(ScenarioChange::hasSettingOverride));
    }

    @Test
    public void testToScenarioInfoUtilizationLevel() {
        final LoadChangesApiDTO loadChanges = new LoadChangesApiDTO();
        loadChanges.setUtilizationList(ImmutableList.of(createUtilizationApiDto(20)));
        final ScenarioInfo scenarioInfo = getScenarioInfo(Collections.emptyList(), loadChanges);
        final UtilizationLevel utilizationLevel =
                scenarioInfo.getChangesList().get(0).getPlanChanges().getUtilizationLevel();
        Assert.assertEquals(20, utilizationLevel.getPercentage());
    }

    @Test
    public void testToScenarioInfoUtilizationLevelEmptyLoadChanges() {
        final LoadChangesApiDTO loadChanges = new LoadChangesApiDTO();
        final ScenarioInfo scenarioInfo = getScenarioInfo(Collections.emptyList(), loadChanges);
        Assert.assertEquals(0, scenarioInfo.getChangesList().size());
    }

    private UtilizationApiDTO createUtilizationApiDto(int percentage) {
        final UtilizationApiDTO utilizationDto = new UtilizationApiDTO();
        utilizationDto.setPercentage(percentage);
        return utilizationDto;
    }

    private ScenarioInfo getScenarioInfo(@Nonnull List<SettingApiDTO> automationSettings,
            @Nonnull LoadChangesApiDTO loadChangesApiDTO) {
        final ScenarioApiDTO dto = new ScenarioApiDTO();
        final ConfigChangesApiDTO configChanges = new ConfigChangesApiDTO();
        configChanges.setAutomationSettingList(automationSettings);
        dto.setConfigChanges(configChanges);
        dto.setLoadChanges(loadChangesApiDTO);
        return scenarioMapper.toScenarioInfo("name", dto);
    }

    @Test
    public void testToApiAdditionChange() {
        Scenario scenario = buildScenario(ScenarioChange.newBuilder()
            .setTopologyAddition(TopologyAddition.newBuilder()
                .addChangeApplicationDays(3)
                .setEntityId(1234)
                .setAdditionCount(44))
            .build());

        ScenarioApiDTO dto = scenarioMapper.toScenarioApiDTO(scenario);
        assertEquals(Long.toString(SCENARIO_ID), dto.getUuid());
        assertEquals(SCENARIO_NAME, dto.getDisplayName());
        assertNotNull(dto.getTopologyChanges());
        assertEquals(1, dto.getTopologyChanges().getAddList().size());

        AddObjectApiDTO changeDto = dto.getTopologyChanges().getAddList().get(0);
        assertEquals(Collections.singletonList(3), changeDto.getProjectionDays());
        assertEquals(new Integer(44), changeDto.getCount());
        assertEquals("1234", changeDto.getTarget().getUuid());
    }

    @Test
    public void testToApiRemovalChange() {
        Scenario scenario = buildScenario(ScenarioChange.newBuilder()
            .setTopologyRemoval(TopologyRemoval.newBuilder()
                .setChangeApplicationDay(3)
                .setEntityId(1234))
            .build());

        ScenarioApiDTO dto = scenarioMapper.toScenarioApiDTO(scenario);
        assertNotNull(dto.getTopologyChanges());
        assertEquals(1, dto.getTopologyChanges().getRemoveList().size());

        RemoveObjectApiDTO changeDto = dto.getTopologyChanges().getRemoveList().get(0);
        assertEquals("1234", changeDto.getTarget().getUuid());
        assertEquals(new Integer(3), changeDto.getProjectionDay());
    }

    @Test
    public void testToApiReplaceChange() {
        Scenario scenario = buildScenario(ScenarioChange.newBuilder()
            .setTopologyReplace(TopologyReplace.newBuilder()
                .setChangeApplicationDay(3)
                .setAddTemplateId(1234)
                .setRemoveEntityId(5678)
            ).build());

        ScenarioApiDTO dto = scenarioMapper.toScenarioApiDTO(scenario);
        assertNotNull(dto.getTopologyChanges());
        assertEquals(1, dto.getTopologyChanges().getReplaceList().size());

        ReplaceObjectApiDTO changeDto = dto.getTopologyChanges().getReplaceList().get(0);
        assertEquals("1234", changeDto.getTemplate().getUuid());
        assertEquals("5678", changeDto.getTarget().getUuid());
        assertEquals(new Integer(3), changeDto.getProjectionDay());
    }

    @Test
    public void testToApiEntityType() {
        Scenario scenario = buildScenario(ScenarioChange.newBuilder()
            .setTopologyAddition(TopologyAddition.newBuilder()
                .setAdditionCount(1)
                .setEntityId(1))
            .build());

        ServiceEntityApiDTO vmDto = new ServiceEntityApiDTO();
        vmDto.setClassName("VirtualMachine");
        vmDto.setDisplayName("VM #100");

        when(repositoryApi.getServiceEntitiesById(any()))
                .thenReturn(ImmutableMap.of(1L, Optional.of(vmDto)));

        ScenarioApiDTO dto = scenarioMapper.toScenarioApiDTO(scenario);
        AddObjectApiDTO changeDto = dto.getTopologyChanges().getAddList().get(0);
        BaseApiDTO target = changeDto.getTarget();
        assertEquals("VirtualMachine", target.getClassName());
        assertEquals("VM #100", target.getDisplayName());
    }

    @Test
    public void testToApiAdditionWithTemplate() {
        Scenario scenario = buildScenario(ScenarioChange.newBuilder()
            .setTopologyAddition(TopologyAddition.newBuilder()
                .setAdditionCount(10)
                .setTemplateId(1))
            .build());

        TemplateApiDTO vmDto = new TemplateApiDTO();
        vmDto.setClassName("VirtualMachine");
        vmDto.setDisplayName("VM #100");

        when(templatesUtils.getTemplatesMapByIds(eq(Sets.newHashSet(1L))))
            .thenReturn(ImmutableMap.of(1L, vmDto));
        ScenarioApiDTO dto = scenarioMapper.toScenarioApiDTO(scenario);
        // The first two are "special" hack changes - SCOPE and PROJECTION_PERIOD.
        assertEquals(1, dto.getTopologyChanges().getAddList().size());

        AddObjectApiDTO changeDto = dto.getTopologyChanges().getAddList().get(0);
        assertEquals("1", changeDto.getTarget().getUuid());
        assertEquals(Integer.valueOf(10), changeDto.getCount());
    }

    @Test
    public void testToApiEntityTypeUnknown() {
        Scenario scenario = buildScenario(ScenarioChange.newBuilder()
                .setTopologyAddition(TopologyAddition.newBuilder()
                        .setAdditionCount(1)
                        .setEntityId(1))
                .build());

        ScenarioApiDTO dto = scenarioMapper.toScenarioApiDTO(scenario);
        AddObjectApiDTO changeDto = dto.getTopologyChanges().getAddList().get(0);
        BaseApiDTO target = changeDto.getTarget();
        assertEquals(UIEntityType.UNKNOWN.getValue(), target.getClassName());
        assertEquals(UIEntityType.UNKNOWN.getValue(), target.getDisplayName());
    }

    @Nonnull
    private ScenarioChange buildNumericSettingOverride(@Nonnull String name, float value) {
        return ScenarioChange.newBuilder().setSettingOverride(
                SettingOverride.newBuilder().setSetting(Setting.newBuilder()
                        .setSettingSpecName(name)
                        .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(value))
                        .build()).build()).build();
    }

    /**
     * Tests converting of empty scenario to scenarioApiDto.
     */
    @Test
    public void testToScenarioApiDtoEmptyScenario() {
        final Scenario scenario = buildScenario(ScenarioChange.newBuilder().build());
        final ScenarioApiDTO scenarioApiDTO = scenarioMapper.toScenarioApiDTO(scenario);
        Assert.assertTrue(scenarioApiDTO.getConfigChanges().getAutomationSettingList().isEmpty());
    }

    /**
     * Tests converting of ScenarioApiDto to ScenarioInfo when Ignore Constraint setting is provided
     * by scenarioApiDto
     */
    @Test
    public void testToScenarioInfoWithIgnoreConstraintSetting() {
        final ScenarioApiDTO dto = new ScenarioApiDTO();
        final ConfigChangesApiDTO configChanges = new ConfigChangesApiDTO();
        List<RemoveConstraintApiDTO> removeConstraints = ImmutableList.of(
                createRemoveConstraintApiDto("1", ConstraintType.ClusterCommodity),
                createRemoveConstraintApiDto("2", ConstraintType.DataCenterCommodity)
        );
        configChanges.setRemoveConstraintList(removeConstraints);
        dto.setConfigChanges(configChanges);
        final ScenarioInfo scenarioInfo = scenarioMapper.toScenarioInfo("name", dto);
        final ScenarioChange scenarioChange = scenarioInfo.getChangesList().get(0);
        Assert.assertTrue(scenarioChange.hasPlanChanges());
        final IgnoreConstraint ignoreClusterCommodity =
                scenarioChange.getPlanChanges().getIgnoreConstraints(0);
        final IgnoreConstraint ignoreDataCenterCommodity =
                scenarioChange.getPlanChanges().getIgnoreConstraints(1);
        Assert.assertEquals(ConstraintType.ClusterCommodity.name(),
                ignoreClusterCommodity.getCommodityType());
        Assert.assertEquals(ConstraintType.DataCenterCommodity.name(),
                ignoreDataCenterCommodity.getCommodityType());
        Assert.assertEquals(1l, ignoreClusterCommodity.getGroupUuid());
        Assert.assertEquals(2l, ignoreDataCenterCommodity.getGroupUuid());
    }

    @Test
    public void testAdditionFromGroup() {
        Group group = Group.newBuilder().setId(1).setGroup(GroupInfo.newBuilder()
                .setStaticGroupMembers(StaticGroupMembers.newBuilder().addStaticMemberOids(2)
                        .addStaticMemberOids(3).build()).build()).build();
        testGroupService.whenThenReturn(group.getId(), group);

        final TargetApiDTO target = new TargetApiDTO();
        target.setUuid("1");
        final AddObjectApiDTO addObject = new AddObjectApiDTO();
        addObject.setCount(10);
        addObject.setTarget(target);
        final TopologyChangesApiDTO topologyChanges = new TopologyChangesApiDTO();
        topologyChanges.setAddList(ImmutableList.of(addObject));
        final ScenarioApiDTO dto = new ScenarioApiDTO();
        dto.setTopologyChanges(topologyChanges);
        final ScenarioInfo scenarioInfo = scenarioMapper.toScenarioInfo("", dto);
        final ScenarioChange firstAddtion = scenarioInfo.getChanges(0);
        final ScenarioChange secondAddtion = scenarioInfo.getChanges(1);

        Assert.assertEquals(2, scenarioInfo.getChangesList().size());
        Assert.assertTrue(firstAddtion.hasTopologyAddition());
        Assert.assertTrue(secondAddtion.hasTopologyAddition());
        Assert.assertEquals(2, firstAddtion.getTopologyAddition().getEntityId());
        Assert.assertEquals(3, secondAddtion.getTopologyAddition().getEntityId());
        Assert.assertEquals(10, firstAddtion.getTopologyAddition().getAdditionCount());
        Assert.assertEquals(10, secondAddtion.getTopologyAddition().getAdditionCount());
    }

    @Nonnull
    private RemoveConstraintApiDTO createRemoveConstraintApiDto(@Nonnull String targetUuid,
            @Nonnull ConstraintType constraintType) {
        final RemoveConstraintApiDTO removeConstraint = new RemoveConstraintApiDTO();
        final BaseApiDTO target = new BaseApiDTO();
        target.setUuid(targetUuid);
        removeConstraint.setTarget(target);
        removeConstraint.setConstraintType(constraintType);
        return removeConstraint;
    }

    private Scenario buildScenario(ScenarioChange... changes) {
        return Scenario.newBuilder().setId(SCENARIO_ID)
            .setScenarioInfo(ScenarioInfo.newBuilder()
                .setName(SCENARIO_NAME)
                .addAllChanges(Arrays.asList(changes))
            ).build();
    }

    private BaseApiDTO entity(long entityId) {
        BaseApiDTO entity = new BaseApiDTO();

        entity.setClassName("Entity");
        entity.setDisplayName("Entity " + entityId);
        entity.setUuid(Long.toString(entityId));

        return entity;
    }

    private ScenarioApiDTO scenarioApiDTO(@Nonnull final TopologyChangesApiDTO topoChanges) {
        ScenarioApiDTO dto = new ScenarioApiDTO();
        dto.setTopologyChanges(topoChanges);

        return dto;
    }

    private BaseApiDTO template(long templateId) {
        BaseApiDTO template = new BaseApiDTO();
        template.setClassName("Template");
        template.setDisplayName("Template " + templateId);
        template.setUuid(Long.toString(templateId));
        return template;
    }

    /**
     * Implementation of group service for tests.
     */
    private static final class TestGroupService extends GroupServiceImplBase {

        private final Map<Long, Group> preparedGroups = new HashMap<>();

        private void whenThenReturn(long uuid, Group group) {
            preparedGroups.put(uuid, group);
        }

        @Override
        public void getGroup(GroupID request, StreamObserver<GetGroupResponse> responseObserver) {
            final Builder responseBuilder = GetGroupResponse.newBuilder();
            final Group group = preparedGroups.get(request.getId());
            if (group != null) {
                responseBuilder.setGroup(group);
            }
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }
    }
}
