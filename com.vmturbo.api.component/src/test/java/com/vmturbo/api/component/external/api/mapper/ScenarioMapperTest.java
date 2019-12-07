package com.vmturbo.api.component.external.api.mapper;

import static com.vmturbo.components.common.setting.GlobalSettingSpecs.AWSPreferredOfferingClass;
import static com.vmturbo.components.common.setting.GlobalSettingSpecs.AWSPreferredPaymentOption;
import static com.vmturbo.components.common.setting.GlobalSettingSpecs.AWSPreferredTerm;
import static com.vmturbo.components.common.setting.GlobalSettingSpecs.RIDemandType;
import static com.vmturbo.components.common.setting.GlobalSettingSpecs.RIPurchase;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.ApiTestUtils;
import com.vmturbo.api.component.communication.RepositoryApi;
import com.vmturbo.api.component.communication.RepositoryApi.MultiEntityRequest;
import com.vmturbo.api.component.external.api.mapper.ScenarioMapper.ScenarioChangeMappingContext;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.SettingApiDTOPossibilities;
import com.vmturbo.api.component.external.api.service.PoliciesService;
import com.vmturbo.api.component.external.api.util.TemplatesUtils;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.scenario.AddObjectApiDTO;
import com.vmturbo.api.dto.scenario.ConfigChangesApiDTO;
import com.vmturbo.api.dto.scenario.LoadChangesApiDTO;
import com.vmturbo.api.dto.scenario.MaxUtilizationApiDTO;
import com.vmturbo.api.dto.scenario.RelievePressureObjectApiDTO;
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
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupResponse;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MergePolicy.MergeType;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.DetailsCase;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.HistoricalBaseline;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.IgnoreConstraint;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.MaxUtilizationLevel;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.PlanChanges.UtilizationLevel;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.RISetting;
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
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DemandType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.PaymentOption;

public class ScenarioMapperTest {
    private static final String SCENARIO_NAME = "MyScenario";
    private static final long SCENARIO_ID = 0xdeadbeef;

    private static final String CUSTOM_SCENARIO_TYPE = "CUSTOM";
    private static final String DECOMMISSION_HOST_SCENARIO_TYPE = "DECOMMISSION_HOST";
    private static final String DISABLED = "DISABLED";
    private static final String AUTOMATIC = "AUTOMATIC";
    private RepositoryApi repositoryApi;

    private TemplatesUtils templatesUtils;

    private PoliciesService policiesService;

    private ScenarioMapper scenarioMapper;

    private GroupServiceMole groupServiceMole = spy(new GroupServiceMole());

    private SettingsManagerMapping settingsManagerMapping = mock(SettingsManagerMapping.class);

    private SettingsMapper settingsMapper = mock(SettingsMapper.class);

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(groupServiceMole);

    private GroupMapper groupMapper = mock(GroupMapper.class);

    private GroupServiceBlockingStub groupRpcService;

    @Before
    public void setup() throws IOException {
        repositoryApi = Mockito.mock(RepositoryApi.class);
        templatesUtils = Mockito.mock(TemplatesUtils.class);
        policiesService = Mockito.mock(PoliciesService.class);
        groupRpcService = GroupServiceGrpc.newBlockingStub(grpcTestServer.getChannel());

        // Return empty by default to keep NPE's at bay.
        MultiEntityRequest req = ApiTestUtils.mockMultiEntityReqEmpty();
        when(repositoryApi.entitiesRequest(any())).thenReturn(req);

        scenarioMapper = new ScenarioMapper(repositoryApi,
                templatesUtils, settingsManagerMapping, settingsMapper,
                policiesService, groupRpcService, groupMapper);
    }

    @Test
    public void testAdditionChange() {
        TopologyChangesApiDTO topoChanges = new TopologyChangesApiDTO();
        AddObjectApiDTO dto = new AddObjectApiDTO();
        dto.setProjectionDays(Collections.singletonList(2));
        dto.setTarget(entity(1));
        dto.setCount(6);
        topoChanges.setAddList(Collections.singletonList(dto));

        ScenarioInfo info = getScenarioInfo(SCENARIO_NAME, scenarioApiDTO(topoChanges));
        assertEquals(SCENARIO_NAME, info.getName());
        assertEquals(1, info.getChangesCount());
        List<ScenarioChange> change = info.getChangesList();
        assertEquals(DetailsCase.TOPOLOGY_ADDITION, change.get(0).getDetailsCase());

        TopologyAddition addition = change.get(0).getTopologyAddition();
        assertEquals(6, addition.getAdditionCount());
        assertEquals(1, addition.getEntityId());
        assertEquals(Collections.singletonList(2), addition.getChangeApplicationDaysList());
    }

    /**
     * Tests that {@link AddObjectApiDTO} converts to {@link TopologyAddition}.
     */
    @Test
    public void testMapTopologyAdditionWithTargetEntityType() {
        //GIVEN
        AddObjectApiDTO dto = new AddObjectApiDTO();
        dto.setProjectionDays(Collections.singletonList(2));
        dto.setTarget(entity(1));
        dto.setCount(6);
        dto.setTargetEntityType(UIEntityType.VIRTUAL_MACHINE.apiStr());

        //WHEN
        final List<ScenarioChange> changes = scenarioMapper.mapTopologyAddition(dto, new HashSet<>());

        //THEN
        assertEquals(1, changes.size());
        TopologyAddition addition = changes.get(0).getTopologyAddition();
        assertEquals(6, addition.getAdditionCount());
        assertEquals(1, addition.getEntityId());
        assertEquals(Collections.singletonList(2), addition.getChangeApplicationDaysList());
        assertEquals(UIEntityType.VIRTUAL_MACHINE.typeNumber(), addition.getTargetEntityType());
    }

    /**
     * Tests that {@link AddObjectApiDTO} converts to {@link TopologyAddition}.
     * Null of optional field should not throw error
     */
    @Test
    public void testMapTopologyAdditionWithNullTargetEntityType() {
        //GIVEN
        AddObjectApiDTO dto = new AddObjectApiDTO();
        dto.setProjectionDays(Collections.singletonList(2));
        dto.setTarget(entity(1));
        dto.setCount(6);

        //WHEN
        final List<ScenarioChange> changes = scenarioMapper.mapTopologyAddition(dto, new HashSet<>());

        //THEN
        TopologyAddition addition = changes.get(0).getTopologyAddition();
        assertEquals(6, addition.getAdditionCount());
        assertEquals(1, addition.getEntityId());
        assertFalse(addition.hasTargetEntityType());
        assertEquals(Collections.singletonList(2), addition.getChangeApplicationDaysList());
    }

    /**
     * Tests that {@link RemoveObjectApiDTO} converts to {@link TopologyRemoval}.
     */
    @Test
    public void testMapTopologyRemovalWithTargetEntityType() {
        //GIVEN
        RemoveObjectApiDTO dto = new RemoveObjectApiDTO();
        dto.setProjectionDay(2);
        dto.setTarget(entity(1));
        dto.setTargetEntityType(UIEntityType.VIRTUAL_MACHINE.apiStr());

        //WHEN
        final ScenarioChange change = scenarioMapper.mapTopologyRemoval(dto);

        //THEN;
        TopologyRemoval removal = change.getTopologyRemoval();
        assertEquals(1, removal.getEntityId());
        assertEquals(UIEntityType.VIRTUAL_MACHINE.typeNumber(), removal.getTargetEntityType());
        assertEquals(2, removal.getChangeApplicationDay());
    }

    /**
     * Tests that {@link RemoveObjectApiDTO} converts to {@link TopologyRemoval}.
     * Null of optional field should not throw error
     */
    @Test
    public void testMapTopologyRemovalWithNullTargetEntityType() {
        //GIVEN
        RemoveObjectApiDTO dto = new RemoveObjectApiDTO();
        dto.setProjectionDay(2);
        dto.setTarget(entity(1));

        //WHEN
        final ScenarioChange change = scenarioMapper.mapTopologyRemoval(dto);

        //THEN;
        TopologyRemoval removal = change.getTopologyRemoval();
        assertEquals(1, removal.getEntityId());
        assertFalse(removal.hasTargetEntityType());
        assertEquals(2, removal.getChangeApplicationDay());
    }

    /**
     * Tests that {@link ReplaceObjectApiDTO} converts to {@link TopologyReplace}.
     */
    @Test
    public void mapTopologyReplaceWithTargetEntityType() {
        //GIVEN
        ReplaceObjectApiDTO dto = new ReplaceObjectApiDTO();
        dto.setProjectionDay(5);
        dto.setTarget(entity(1));
        dto.setTemplate(template(2));
        dto.setTargetEntityType(UIEntityType.VIRTUAL_MACHINE.apiStr());

        //WHEN
        final ScenarioChange change = scenarioMapper.mapTopologyReplace(dto);

        //THEN
        TopologyReplace replace = change.getTopologyReplace();
        assertEquals(5, replace.getChangeApplicationDay());
        assertEquals(1, replace.getRemoveEntityId());
        assertEquals(2, replace.getAddTemplateId());
        assertEquals(UIEntityType.VIRTUAL_MACHINE.typeNumber(), replace.getTargetEntityType());
    }

    /**
     * Tests that {@link ReplaceObjectApiDTO} converts to {@link TopologyReplace}.
     */
    @Test
    public void mapTopologyReplaceWithNullTargetEntityType() {
        //GIVEN
        ReplaceObjectApiDTO dto = new ReplaceObjectApiDTO();
        dto.setProjectionDay(5);
        dto.setTarget(entity(1));
        dto.setTemplate(template(2));

        //WHEN
        final ScenarioChange change = scenarioMapper.mapTopologyReplace(dto);

        //THEN
        TopologyReplace replace = change.getTopologyReplace();
        assertEquals(5, replace.getChangeApplicationDay());
        assertEquals(1, replace.getRemoveEntityId());
        assertEquals(2, replace.getAddTemplateId());
        assertFalse(replace.hasTargetEntityType());
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

        ScenarioInfo info = getScenarioInfo(SCENARIO_NAME, scenarioApiDTO(topoChanges));

        assertEquals(SCENARIO_NAME, info.getName());
        assertEquals(1, info.getChangesCount());
        List<ScenarioChange> change = info.getChangesList();
        assertEquals(DetailsCase.TOPOLOGY_ADDITION, change.get(0).getDetailsCase());

        TopologyAddition addition = change.get(0).getTopologyAddition();
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

        ScenarioInfo info = getScenarioInfo(SCENARIO_NAME, scenarioApiDTO(topoChanges));
        assertEquals(SCENARIO_NAME, info.getName());
        assertEquals(1, info.getChangesCount());
        List<ScenarioChange> change = info.getChangesList();
        assertEquals(DetailsCase.TOPOLOGY_REMOVAL, change.get(0).getDetailsCase());

        TopologyRemoval removal = change.get(0).getTopologyRemoval();
        assertEquals(2, removal.getChangeApplicationDay());
        assertEquals(1, removal.getEntityId());
    }

    @Test
    public void testInvalidChange() {
        ScenarioApiDTO scenarioDto = new ScenarioApiDTO();
        scenarioDto.setScope(Collections.singletonList(entity(1)));

        ScenarioInfo info = getScenarioInfo(SCENARIO_NAME, scenarioDto);
        assertEquals(0, info.getChangesList().stream().filter(c -> !c.hasSettingOverride()).count());
    }

    @Test
    public void testReplaceChange() {
        TopologyChangesApiDTO topoChanges = new TopologyChangesApiDTO();
        ReplaceObjectApiDTO dto = new ReplaceObjectApiDTO();
        dto.setProjectionDay(5);
        dto.setTarget(entity(1));
        dto.setTemplate(template(2));
        topoChanges.setReplaceList(Collections.singletonList(dto));

        ScenarioInfo info = getScenarioInfo(SCENARIO_NAME, scenarioApiDTO(topoChanges));
        assertEquals(SCENARIO_NAME, info.getName());
        assertEquals(1, info.getChangesCount());

        List<ScenarioChange> change = info.getChangesList();
        assertEquals(DetailsCase.TOPOLOGY_REPLACE, change.get(0).getDetailsCase());
        TopologyReplace replace = change.get(0).getTopologyReplace();
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

        ScenarioInfo info = getScenarioInfo(SCENARIO_NAME, scenarioApiDTO(topoChanges));
        assertEquals(2, info.getChangesCount());

        List<ScenarioChange> change = info.getChangesList();
        assertEquals(DetailsCase.TOPOLOGY_ADDITION, change.get(0).getDetailsCase());
        TopologyAddition addition = change.get(0).getTopologyAddition();
        assertEquals(1, addition.getEntityId());

        assertEquals(DetailsCase.TOPOLOGY_REMOVAL, change.get(1).getDetailsCase());
        TopologyRemoval removal = change.get(1).getTopologyRemoval();
        assertEquals(2, removal.getEntityId());
    }

    @Test
    public void testSettingOverride() {
        final SettingApiDTO<String> setting = createStringSetting("foo", "value");

        SettingsMapper.SettingApiDtoKey key = SettingsMapper.getSettingApiDtoKey(setting);

        when(settingsMapper.toProtoSettings(Collections.singletonList(setting)))
            .thenReturn(ImmutableMap.of(key, Setting.newBuilder()
                    .setSettingSpecName("foo")
                    .setStringSettingValue(StringSettingValue.newBuilder().setValue("value"))
                    .build()));

        final ScenarioInfo scenarioInfo = getScenarioInfo(Collections.singletonList(setting), null);
        assertThat(scenarioInfo.getChangesCount(), is(1));
        final List<ScenarioChange> change = scenarioInfo.getChangesList();
        assertTrue(change.get(0).hasSettingOverride());
        assertTrue(change.get(0).getSettingOverride().hasSetting());
        final Setting overridenSetting = change.get(0).getSettingOverride().getSetting();
        assertThat(overridenSetting.getSettingSpecName(), is("foo"));
        assertTrue(overridenSetting.hasStringSettingValue());
        assertThat(overridenSetting.getStringSettingValue().getValue(), is("value"));
    }

    @Test
    public void testSettingOverridePlanSettingMapping() {
        final SettingApiDTO<String> setting = createStringSetting("foo", "value");

        SettingsMapper.SettingApiDtoKey key = SettingsMapper.getSettingApiDtoKey(setting);

        when(settingsMapper.toProtoSettings(Collections.singletonList(setting)))
                .thenReturn(ImmutableMap.of(key, Setting.newBuilder()
                        .setSettingSpecName("foo")
                        .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(1.2f))
                        .build()));

        final ScenarioInfo scenarioInfo = getScenarioInfo(Collections.singletonList(setting), null);
        assertThat(scenarioInfo.getChangesCount(), is(1));
        final List<ScenarioChange> change = scenarioInfo.getChangesList();
        assertTrue(change.get(0).hasSettingOverride());
        assertTrue(change.get(0).getSettingOverride().hasSetting());
        final Setting overridenSetting = change.get(0).getSettingOverride().getSetting();
        assertThat(overridenSetting.getSettingSpecName(), is("foo"));
        assertTrue(overridenSetting.hasNumericSettingValue());
        assertThat(overridenSetting.getNumericSettingValue().getValue(), is(1.2f));
    }

    @Test
    public void testSettingOverrideUnknownSetting() {
        final SettingApiDTO<String> setting = createStringSetting("unknown", "value");
        final ScenarioInfo scenarioInfo = getScenarioInfo(Collections.singletonList(setting), null);
        assertThat(scenarioInfo.getChangesCount(), is(0));
    }

    /**
     * Tests converting of scenario with desired state setting to ScenarioApiDto.
     */
    @Test
    public void testSettingOverrideToApiDto() {
        final Scenario scenario = buildScenario(buildNumericSettingOverride("foo", 1.2f));

        final SettingApiDTO<String> apiDto = new SettingApiDTO<>();
        final SettingApiDTOPossibilities possibilities = mock(SettingApiDTOPossibilities.class);
        when(possibilities.getAll()).thenReturn(Collections.singletonList(apiDto));
        when(settingsMapper.toSettingApiDto(any())).thenReturn(possibilities);
        // Pass-through plan settings conversion

        final ScenarioApiDTO scenarioApiDTO = scenarioMapper.toScenarioApiDTO(scenario);
        final SettingApiDTO<String> apiFoo = scenarioApiDTO.getConfigChanges().getAutomationSettingList().get(0);
        assertEquals(apiFoo, apiDto);
    }

    /**
     * Tests converting of ScenarioApiDto without config changes to ScenarioInfo.
     */
    @Test
    public void testToScenarioInfoWithoutConfigChanges() {
        final ScenarioApiDTO dto = new ScenarioApiDTO();
        dto.setConfigChanges(null);
        final ScenarioInfo scenarioInfo = getScenarioInfo("name", dto);
        Assert.assertTrue(scenarioInfo.getChangesList().isEmpty());
    }

    /**
     * Tests converting of ScenarioApiDto with empty config changes to ScenarioInfo.
     */
    @Test
    public void testToScenarioInfoWithEmptyConfigChanges() {
        final ScenarioInfo scenarioInfo = getScenarioInfo((List<SettingApiDTO<String>>)null, null);
        Assert.assertTrue(scenarioInfo.getChangesList().isEmpty());
    }

    /**
     * Tests converting of ScenarioApiDto with empty automation settings list to ScenarioInfo.
     */
    @Test
    public void testToScenarioInfoWithEmptyAutomationSettingsList() {
        final ScenarioApiDTO decommissionHostPlanDto = new ScenarioApiDTO();
        decommissionHostPlanDto.setConfigChanges(null);
        decommissionHostPlanDto.setType(DECOMMISSION_HOST_SCENARIO_TYPE);
        final ScenarioInfo decommissionScenarioInfo = getScenarioInfo("decommission plan", decommissionHostPlanDto);
        Assert.assertTrue(decommissionScenarioInfo.getChangesList().size() == 1);
        List<SettingOverride> pmp = decommissionScenarioInfo.getChangesList().stream()
            .map(ScenarioChange::getSettingOverride)
            .filter(s -> s.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)
            .collect(Collectors.toList());
        Assert.assertTrue(pmp.get(0).getSetting().getSettingSpecName()
            .equalsIgnoreCase(EntitySettingSpecs.Provision.getSettingName()));
        Assert.assertTrue(pmp.get(0).getSetting().getEnumSettingValue().getValue().equals(DISABLED));
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

    /**
     * Tests ScenarioChange Object is correctly built to match SettingApiDTO configurations.
     */
    @Test
    public void buildRISettingChangesShouldCreateScenarioChangeWithRISettingFromAWSRIsettings() {
        // GIVEN
        List<SettingApiDTO> riSettingList = new ArrayList<>();
        riSettingList.add(createStringSetting(RIPurchase.getSettingName(), "true"));
        riSettingList.add(createStringSetting(AWSPreferredOfferingClass.getSettingName(), "Convertible"));
        riSettingList.add(createStringSetting(AWSPreferredPaymentOption.getSettingName(), "Partial Upfront"));
        riSettingList.add(createStringSetting(AWSPreferredTerm.getSettingName(), "Years 1"));
        riSettingList.add(createStringSetting(RIDemandType.getSettingName(), "Consumption"));

        // WHEN
        final ScenarioChange scenarioChange = scenarioMapper.buildRISettingChanges(riSettingList);

        // THEN
        Assert.assertNotNull(scenarioChange);
        final RISetting riSetting = scenarioChange.getRiSetting();
        Assert.assertNotNull(riSetting);
        Assert.assertEquals(OfferingClass.CONVERTIBLE, riSetting.getPreferredOfferingClass());
        Assert.assertEquals(PaymentOption.PARTIAL_UPFRONT, riSetting.getPreferredPaymentOption());
        Assert.assertEquals(1, riSetting.getPreferredTerm());
        Assert.assertEquals(DemandType.CONSUMPTION, riSetting.getDemandType());
    }

    /**
     * Tests ScenarioChange Object is correctly built when there is no demand type set.
     */
    @Test
    public void buildRISettingChangesWithoutDemandType() {
        // GIVEN
        List<SettingApiDTO> riSettingList = new ArrayList<>();
        riSettingList.add(createStringSetting(RIPurchase.getSettingName(), "true"));

        // WHEN
        final ScenarioChange scenarioChange = scenarioMapper.buildRISettingChanges(riSettingList);

        // THEN
        Assert.assertNotNull(scenarioChange);
        final RISetting riSetting = scenarioChange.getRiSetting();
        Assert.assertNotNull(riSetting);
        Assert.assertFalse(riSetting.hasDemandType());
    }

    /**
     * Tests no ScenarioChange Object built when SettingApiDTO RIPurchase  set to false
     */
    @Test
    public void buildRISettingChangesShouldReturnNullWhenRiPurchaseIsDisabled() {
        // GIVEN
        List<SettingApiDTO> riSettingList = new ArrayList<>();
        riSettingList.add(createStringSetting(RIPurchase.getSettingName(), "false"));

        // WHEN
        final ScenarioChange scenarioChange = scenarioMapper.buildRISettingChanges(riSettingList);

        // THEN
        Assert.assertNull(scenarioChange);
    }

    /**
     * Tests ScenarioChange Object built when SettingApiDTO RIPurchase set to true
     */
    @Test
    public void buildRISettingChangesShouldCreateScenarioChangeWhenRiPurchaseIsEnabled() {
        // GIVEN
        List<SettingApiDTO> riSettingList = new ArrayList<>();
        riSettingList.add(createStringSetting(RIPurchase.getSettingName(), "true"));

        // WHEN
        final ScenarioChange scenarioChange = scenarioMapper.buildRISettingChanges(riSettingList);

        // THEN
        Assert.assertNotNull(scenarioChange);
    }

    /**
     * Tests ScenarioChange Object built when SettingApiDTO list is empty
     */
    @Test
    public void buildRISettingChangesShouldCreateScenarioChangeWhenRIPurchaseNotPresent() {
        // GIVEN
        List<SettingApiDTO> riSettingList = new ArrayList<>();

        // WHEN
        final ScenarioChange scenarioChange = scenarioMapper.buildRISettingChanges(riSettingList);

        // THEN
        Assert.assertNotNull(scenarioChange);
    }

    /**
     * Tests {@link PlanChanges.IgnoreConstraint} mapped to expected {@link RemoveConstraintApiDTO}
     */
    @Test
    public void testToRemoveConstraintApiDTO() {
        //GIVEN
        long groupId = 1234;
        ScenarioChangeMappingContext contextMock = mock(ScenarioChangeMappingContext.class);
        BaseApiDTO baseApiDto = new BaseApiDTO();
        when(contextMock.dtoForId(groupId)).thenReturn(baseApiDto);

        IgnoreConstraint ignoreConstraint = IgnoreConstraint.newBuilder().setIgnoreGroup(
                PlanChanges.ConstraintGroup.newBuilder()
                        .setCommodityType("ClusterCommodity")
                        .setGroupUuid(groupId).build())
                .build();

        //WHEN
        RemoveConstraintApiDTO removeConstraintApiDTO= scenarioMapper.toRemoveConstraintApiDTO(ignoreConstraint, contextMock);

        //THEN
        assertThat(removeConstraintApiDTO.getConstraintType(), is(ConstraintType.ClusterCommodity));
        assertThat(removeConstraintApiDTO.getTarget(), is(baseApiDto));
    }

    @Test
    public void testToScenarioInfoBaselineChanges() {
        final LoadChangesApiDTO loadChanges = new LoadChangesApiDTO();
        long testbaselineDate = 123456789;
        loadChanges.setBaselineDate(String.valueOf(testbaselineDate));
        final ScenarioInfo scenarioInfo = getScenarioInfo(Collections.emptyList(), loadChanges);
        final long baselineDate = scenarioInfo.getChangesList().get(0)
            .getPlanChanges().getHistoricalBaseline().getBaselineDate();
        Assert.assertEquals(testbaselineDate, baselineDate);
    }

    @Test
    public void testToScenarioInfoUtilizationLevelEmptyLoadChanges() {
        final LoadChangesApiDTO loadChanges = new LoadChangesApiDTO();
        final ScenarioInfo scenarioInfo = getScenarioInfo(Collections.emptyList(), loadChanges);
        Assert.assertEquals(0, scenarioInfo.getChangesList().size());
    }


    /**
     * Converts global {@link UtilizationApiDTO} setting to {@link UtilizationLevel}.
     * */
    @Test
    public void testGetUtilizationChangesGlobalSetting() {
        //GiVEN
        UtilizationApiDTO dto = new UtilizationApiDTO();
        dto.setPercentage(10);

        //WHEN
        List<ScenarioChange> changes = scenarioMapper.getUtilizationChanges(Lists.newArrayList(dto));

        //THEN
        assertEquals(1, changes.size());
        assertTrue(changes.get(0).getPlanChanges().hasUtilizationLevel());
        assertEquals(10, changes.get(0).getPlanChanges().getUtilizationLevel().getPercentage());
        assertFalse(changes.get(0).getPlanChanges().getUtilizationLevel().hasGroupOid());
    }

    /**
     * Converts group {@link UtilizationApiDTO} setting to {@link UtilizationLevel}.
     * */
    @Test
    public void testGetUtilizationChangesPerGroupSetting() {
        //GiVEN
        UtilizationApiDTO dto = new UtilizationApiDTO();
        dto.setPercentage(10);
        dto.setTarget(entity(1L));

        //WHEN
        List<ScenarioChange> changes = scenarioMapper.getUtilizationChanges(Lists.newArrayList(dto));

        //THEN
        assertEquals(1, changes.size());
        assertTrue(changes.get(0).getPlanChanges().hasUtilizationLevel());
        assertEquals(10, changes.get(0).getPlanChanges().getUtilizationLevel().getPercentage());
        assertEquals(1L, changes.get(0).getPlanChanges().getUtilizationLevel().getGroupOid());
    }

    private UtilizationApiDTO createUtilizationApiDto(int percentage) {
        final UtilizationApiDTO utilizationDto = new UtilizationApiDTO();
        utilizationDto.setPercentage(percentage);
        return utilizationDto;
    }

    private ScenarioInfo getScenarioInfo(@Nonnull List<SettingApiDTO<String>> automationSettings,
            @Nonnull LoadChangesApiDTO loadChangesApiDTO) {
        final ScenarioApiDTO dto = new ScenarioApiDTO();
        final ConfigChangesApiDTO configChanges = new ConfigChangesApiDTO();
        configChanges.setAutomationSettingList(automationSettings);
        dto.setConfigChanges(configChanges);
        dto.setLoadChanges(loadChangesApiDTO);
        return getScenarioInfo("name", dto);
    }

    private ScenarioInfo getScenarioInfo(String scenarioName, ScenarioApiDTO dto) {
        ScenarioInfo scenarioInfo = null;
        try {
            scenarioInfo = scenarioMapper.toScenarioInfo(scenarioName, dto);
        } catch (InvalidOperationException e) {
            assertEquals("Could not create scenario info.", true, false);
        }
        return scenarioInfo;
    }

    @Test
    public void testToApiAdditionChange() {
        Scenario scenario = buildScenario(ScenarioChange.newBuilder()
            .setTopologyAddition(TopologyAddition.newBuilder()
                .addChangeApplicationDays(3)
                .setEntityId(1234)
                .setAdditionCount(44)
                .setTargetEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
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
        assertEquals(UIEntityType.VIRTUAL_MACHINE.apiStr(), changeDto.getTargetEntityType());
    }

    /**
     * Convert {@link TopologyAddition} to {@link AddObjectApiDTO}.
     * Null targetEntityType should not throw error
     */
    @Test
    public void testToApiAdditionChangeWithNullTargetEntityType() {
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
        assertTrue(changeDto.getTargetEntityType() == null);
    }

    @Test
    public void testToApiRemovalChange() {
        Scenario scenario = buildScenario(ScenarioChange.newBuilder()
                .setTopologyRemoval(TopologyRemoval.newBuilder()
                        .setChangeApplicationDay(3)
                        .setEntityId(1234)
                        .setTargetEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
                .build());

        ScenarioApiDTO dto = scenarioMapper.toScenarioApiDTO(scenario);
        assertNotNull(dto.getTopologyChanges());
        assertEquals(1, dto.getTopologyChanges().getRemoveList().size());

        RemoveObjectApiDTO changeDto = dto.getTopologyChanges().getRemoveList().get(0);
        assertEquals("1234", changeDto.getTarget().getUuid());
        assertEquals(new Integer(3), changeDto.getProjectionDay());
        assertEquals(UIEntityType.VIRTUAL_MACHINE.apiStr(), changeDto.getTargetEntityType());
    }

    /**
     * Convert {@link TopologyRemoval} to {@link RemoveObjectApiDTO}.
     * Null targetEntityType should not throw error
     */
    @Test
    public void testToApiRemovalChangeWithNullTargetEntityType() {
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
        assertTrue(changeDto.getTargetEntityType() == null);
    }

    @Test
    public void testToApBaselineDateChange() {
        long currTimeinMillis = System.currentTimeMillis();
        String expectedDateTime = DateTimeUtil.toString(System.currentTimeMillis());
        Scenario scenario = buildScenario(ScenarioChange.newBuilder()
            .setPlanChanges(PlanChanges.newBuilder()
                .setHistoricalBaseline(HistoricalBaseline.newBuilder()
                    .setBaselineDate(currTimeinMillis)))
            .build());

        ScenarioApiDTO dto = scenarioMapper.toScenarioApiDTO(scenario);

        assertEquals(expectedDateTime, dto.getLoadChanges().getBaselineDate());
    }

    @Test
    public void testToApiReplaceChange() {
        Scenario scenario = buildScenario(ScenarioChange.newBuilder()
            .setTopologyReplace(TopologyReplace.newBuilder()
                .setChangeApplicationDay(3)
                .setAddTemplateId(1234)
                .setRemoveEntityId(5678)
                .setTargetEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
            ).build());

        ScenarioApiDTO dto = scenarioMapper.toScenarioApiDTO(scenario);
        assertNotNull(dto.getTopologyChanges());
        assertEquals(1, dto.getTopologyChanges().getReplaceList().size());

        ReplaceObjectApiDTO changeDto = dto.getTopologyChanges().getReplaceList().get(0);
        assertEquals("1234", changeDto.getTemplate().getUuid());
        assertEquals("5678", changeDto.getTarget().getUuid());
        assertEquals(new Integer(3), changeDto.getProjectionDay());
        assertEquals(UIEntityType.VIRTUAL_MACHINE.apiStr(), changeDto.getTargetEntityType());
    }

    /**
     * Convert {@link TopologyReplace} to {@link ReplaceObjectApiDTO}.
     * Null targetEntityType should not throw error
     */
    @Test
    public void testToApiReplaceChangeWithNullTargetEntityType() {
        Scenario scenario = buildScenario(ScenarioChange.newBuilder()
                .setTopologyReplace(TopologyReplace.newBuilder()
                        .setChangeApplicationDay(3)
                        .setAddTemplateId(1234)
                        .setRemoveEntityId(5678))
                .build());

        ScenarioApiDTO dto = scenarioMapper.toScenarioApiDTO(scenario);
        assertNotNull(dto.getTopologyChanges());
        assertEquals(1, dto.getTopologyChanges().getReplaceList().size());

        ReplaceObjectApiDTO changeDto = dto.getTopologyChanges().getReplaceList().get(0);
        assertEquals("1234", changeDto.getTemplate().getUuid());
        assertEquals("5678", changeDto.getTarget().getUuid());
        assertEquals(new Integer(3), changeDto.getProjectionDay());
        assertTrue(changeDto.getTargetEntityType() == null);
    }

    @Test
    public void testToApiEntityType() {
        Scenario scenario = buildScenario(ScenarioChange.newBuilder()
            .setTopologyAddition(TopologyAddition.newBuilder()
                .setAdditionCount(1)
                .setEntityId(1)
                .setTargetEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber()))
            .build());

        ServiceEntityApiDTO vmDto = new ServiceEntityApiDTO();
        vmDto.setUuid("1");
        vmDto.setClassName("VirtualMachine");
        vmDto.setDisplayName("VM #100");

        MultiEntityRequest req = ApiTestUtils.mockMultiSEReq(Lists.newArrayList(vmDto));
        when(repositoryApi.entitiesRequest(any())).thenReturn(req);

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
        vmDto.setUuid("1");
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
        assertNull(target.getClassName());
        assertEquals(UIEntityType.UNKNOWN.apiStr(), target.getDisplayName());
    }

    @Nonnull
    private ScenarioChange buildNumericSettingOverride(String name, float value) {
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
    public void testToScenarioInfoWithIgnoreGroupConstraintSetting() {
        final ScenarioApiDTO dto = new ScenarioApiDTO();
        final ConfigChangesApiDTO configChanges = new ConfigChangesApiDTO();
        List<RemoveConstraintApiDTO> removeConstraints = ImmutableList.of(
                createRemoveConstraintApiDto("1", ConstraintType.ClusterCommodity),
                createRemoveConstraintApiDto("2", ConstraintType.DataCenterCommodity)
        );
        configChanges.setRemoveConstraintList(removeConstraints);
        dto.setConfigChanges(configChanges);
        final ScenarioInfo scenarioInfo = getScenarioInfo("name", dto);
        final ScenarioChange scenarioChange = scenarioInfo.getChangesList().get(0);
        Assert.assertTrue(scenarioChange.hasPlanChanges());
        final IgnoreConstraint ignoreClusterCommodity =
                scenarioChange.getPlanChanges().getIgnoreConstraints(0);
        final IgnoreConstraint ignoreDataCenterCommodity =
                scenarioChange.getPlanChanges().getIgnoreConstraints(1);
        Assert.assertEquals(ConstraintType.ClusterCommodity.name(),
                ignoreClusterCommodity.getIgnoreGroup().getCommodityType());
        Assert.assertEquals(ConstraintType.DataCenterCommodity.name(),
                ignoreDataCenterCommodity.getIgnoreGroup().getCommodityType());
        Assert.assertEquals(1l, ignoreClusterCommodity.getIgnoreGroup().getGroupUuid());
        Assert.assertEquals(2l, ignoreDataCenterCommodity.getIgnoreGroup().getGroupUuid());
    }

    @Test
    public void testAdditionFromGroup() {
        final Grouping group = Grouping.newBuilder().setId(1)
                .setDefinition(GroupDefinition.getDefaultInstance())
                .build();
        when(groupServiceMole.getGroup(GroupID.newBuilder().setId(1L).build()))
            .thenReturn(GetGroupResponse.newBuilder()
                .setGroup(group)
                .build());

        final TargetApiDTO target = new TargetApiDTO();
        target.setUuid("1");
        final AddObjectApiDTO addObject = new AddObjectApiDTO();
        addObject.setCount(10);
        addObject.setTarget(target);
        final TopologyChangesApiDTO topologyChanges = new TopologyChangesApiDTO();
        topologyChanges.setAddList(ImmutableList.of(addObject));
        final ScenarioApiDTO dto = new ScenarioApiDTO();
        dto.setTopologyChanges(topologyChanges);
        final ScenarioInfo scenarioInfo = getScenarioInfo("", dto);

        Assert.assertEquals(1, scenarioInfo.getChangesList().size());
        List<ScenarioChange> changes = scenarioInfo.getChangesList();
        final ScenarioChange firstAddtion = changes.get(0);
        Assert.assertTrue(firstAddtion.hasTopologyAddition());
        Assert.assertEquals(1, firstAddtion.getTopologyAddition().getGroupId());
        Assert.assertEquals(10, firstAddtion.getTopologyAddition().getAdditionCount());
    }

    @Test
    public void testMappingContextGroup() {
        final long groupId = 10L;
        final Grouping group = Grouping.newBuilder()
                .setId(groupId)
                .build();
        final GroupApiDTO groupApiDTO = new GroupApiDTO();
        final List<ScenarioChange> changes = Collections.singletonList(ScenarioChange.newBuilder()
                .setTopologyAddition(TopologyAddition.newBuilder()
                        .setGroupId(groupId))
                .build());
        when(groupServiceMole.getGroups(GetGroupsRequest.newBuilder().setGroupFilter(GroupFilter
                        .newBuilder()
                        .addId(groupId))
                .build()))
            .thenReturn(Collections.singletonList(group));
        when(groupMapper.toGroupApiDto(group)).thenReturn(groupApiDTO);

        final ScenarioChangeMappingContext context =
                new ScenarioChangeMappingContext(repositoryApi, templatesUtils,
                        groupRpcService, groupMapper, changes);
        // This will compare object references, which is fine for testing purposes - it means
        // the right lookup is happening in the group mapper.
        assertThat(context.dtoForId(groupId), is(groupApiDTO));
    }

    /**
     * Tests user disabling storage suspension in plan configuration.
     */
    @Test
    public void testStorageSuspensionDisabledScenario() {
        String suspendStorageSetting = "suspendDS";
        String suspendStorageSettingFalse = "false";
        final ConfigChangesApiDTO configChanges = new ConfigChangesApiDTO();
        SettingApiDTO<String> automationSetting = new SettingApiDTO<>();
        automationSetting.setUuid(suspendStorageSetting);
        automationSetting.setValue(suspendStorageSettingFalse);
        configChanges.setAutomationSettingList(Lists.newArrayList(automationSetting));
        ScenarioApiDTO dto = new ScenarioApiDTO();
        dto.setConfigChanges(configChanges);

        // assert that there is an automation settings change
        Assert.assertFalse(dto.getConfigChanges().getAutomationSettingList().isEmpty());
        assertSame(dto.getConfigChanges().getAutomationSettingList().get(0).getUuid(),
                        suspendStorageSetting);
        assertSame(dto.getConfigChanges().getAutomationSettingList().get(0).getValue(),
                        suspendStorageSettingFalse);
    }

    /**
     * Tests converting of ScenarioApiDto to ScenarioInfo for alleviate pressure plan.
     */
    @Test
    public void testToScenarioInfoForAlleviatePressurePlan() {
        final ScenarioApiDTO dto = scenarioApiForAlleviatePressurePlan(1000, 2000);

        final ScenarioInfo scenarioInfo = getScenarioInfo("name", dto);
        assertEquals(ScenarioMapper.ALLEVIATE_PRESSURE_PLAN_TYPE, scenarioInfo.getType());

        // Expected changes : 6
        // Merge Policy Change, Ignore constraints on hot cluster
        // and disable 4 (suspend, provision, resize, reconfigure) actions.
        final List<ScenarioChange> scenarioChanges = scenarioInfo.getChangesList();
        assertEquals(6, scenarioChanges.size());

        // Contains Merge Policy
        ScenarioChange mergePolicyChange = scenarioChanges.stream()
            .filter(change -> change.hasPlanChanges() && change.getPlanChanges().hasPolicyChange())
            .findFirst()
            .orElse(null);

        assertNotNull(mergePolicyChange);
        assertEquals(MergeType.CLUSTER, mergePolicyChange.getPlanChanges().getPolicyChange()
                        .getPlanOnlyPolicy().getPolicyInfo().getMerge().getMergeType());

        // 3 Ignore Constraints expected for hot cluster
        ScenarioChange ignoreConstraintsChange = scenarioChanges.stream()
                        .filter(change -> change.hasPlanChanges() && change.getPlanChanges()
                                        .getIgnoreConstraintsList().size() == 3)
                        .findFirst().orElse(null);

        assertNotNull(ignoreConstraintsChange);

        Arrays.asList(ConstraintType.NetworkCommodity.name(),
            ConstraintType.StorageClusterCommodity.name(),
            ConstraintType.DataCenterCommodity.name())
                .equals(ignoreConstraintsChange.getPlanChanges()
                    .getIgnoreConstraintsList().stream()
                        .map(constraint -> constraint.getIgnoreGroup().getCommodityType())
                        .collect(Collectors.toList()));

        // Contains Disabled Actions.
        ScenarioChange provisionDisabledChange = scenarioChanges.stream()
                        .filter(change -> change.hasSettingOverride() && change.getSettingOverride()
                                        .getSetting().getSettingSpecName()
                                        .equals(EntitySettingSpecs.Provision.getSettingName()))
                        .findFirst().orElse(null);
        assertNotNull(provisionDisabledChange);

        Arrays.asList(EntitySettingSpecs.Suspend.getSettingName(),
                        EntitySettingSpecs.Resize.getSettingName(),
                        EntitySettingSpecs.Provision.getSettingName(),
                        EntitySettingSpecs.Reconfigure.getSettingName())
                            .equals(scenarioChanges.stream()
                                .filter(change -> change.hasSettingOverride())
                                .map(change -> change.getSettingOverride().getSetting())
                                .collect(Collectors.toList()));
    }

    /**
     * Tests convert {@link MaxUtilizationLevel} objects to {@link MaxUtilizationApiDTO}.
     * All fields with values
     */
    @Test
    public void testGetMaxUtilizationApiDTOs() {
        //GIVEN
        final MaxUtilizationLevel maxUtilLevel = MaxUtilizationLevel.newBuilder()
                .setPercentage(100)
                .setGroupOid(12)
                .setSelectedEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
                .build();
        List<ScenarioChange> changes = new ArrayList<ScenarioChange>();
        changes.add(ScenarioChange.newBuilder()
                .setPlanChanges(PlanChanges.newBuilder()
                        .setMaxUtilizationLevel(maxUtilLevel))
                .build());

        ScenarioChangeMappingContext contextMock = mock(ScenarioChangeMappingContext.class);
        BaseApiDTO baseApiDto = new BaseApiDTO();
        when(contextMock.dtoForId(maxUtilLevel.getGroupOid())).thenReturn(baseApiDto);


        //WHEN
        List<MaxUtilizationApiDTO> dtos = scenarioMapper.getMaxUtilizationApiDTOs(changes, contextMock);

        //THEN
        assertEquals(1, dtos.size());
        assertEquals(baseApiDto, dtos.get(0).getTarget());
        assertEquals(UIEntityType.VIRTUAL_MACHINE.apiStr(), dtos.get(0).getSelectedEntityType());
        assertEquals(Integer.valueOf(100), dtos.get(0).getMaxPercentage());
    }

    /**
     * Tests convert {@link MaxUtilizationLevel} objects to {@link MaxUtilizationApiDTO}.
     * Group field is null, indicates global setting
     */
    @Test
    public void testGetMaxUtilizationApiDTOsWithoutGroup() {
        //GIVEN
        final MaxUtilizationLevel maxUtilLevel = MaxUtilizationLevel.newBuilder()
                .setPercentage(100)
                .setSelectedEntityType(UIEntityType.VIRTUAL_MACHINE.typeNumber())
                .build();
        List<ScenarioChange> changes = new ArrayList<ScenarioChange>();
        changes.add(ScenarioChange.newBuilder()
                .setPlanChanges(PlanChanges.newBuilder()
                        .setMaxUtilizationLevel(maxUtilLevel))
                .build());

        ScenarioChangeMappingContext contextMock = mock(ScenarioChangeMappingContext.class);
        BaseApiDTO baseApiDto = new BaseApiDTO();
        when(contextMock.dtoForId(maxUtilLevel.getGroupOid())).thenReturn(baseApiDto);


        //WHEN
        List<MaxUtilizationApiDTO> dtos = scenarioMapper.getMaxUtilizationApiDTOs(changes, contextMock);

        //THEN
        assertEquals(1, dtos.size());
        assertNull(dtos.get(0).getTarget());
        assertEquals(UIEntityType.VIRTUAL_MACHINE.apiStr(), dtos.get(0).getSelectedEntityType());
        assertEquals(Integer.valueOf(100), dtos.get(0).getMaxPercentage());
    }

    /**
     * Tests convert {@link MaxUtilizationLevel} objects to {@link MaxUtilizationApiDTO}.
     * targetEntity
     */
    @Test
    public void testGetMaxUtilizationApiDTOsWithoutTargetEntityType() {
        //GIVEN
        final MaxUtilizationLevel maxUtilLevel = MaxUtilizationLevel.newBuilder()
                .setPercentage(100)
                .setGroupOid(12)
                .build();
        List<ScenarioChange> changes = new ArrayList<ScenarioChange>();
        changes.add(ScenarioChange.newBuilder()
                .setPlanChanges(PlanChanges.newBuilder()
                        .setMaxUtilizationLevel(maxUtilLevel))
                .build());

        ScenarioChangeMappingContext contextMock = mock(ScenarioChangeMappingContext.class);
        BaseApiDTO baseApiDto = new BaseApiDTO();
        when(contextMock.dtoForId(maxUtilLevel.getGroupOid())).thenReturn(baseApiDto);

        //WHEN
        List<MaxUtilizationApiDTO> dtos = scenarioMapper.getMaxUtilizationApiDTOs(changes, contextMock);

        //THEN
        assertEquals(1, dtos.size());
        assertEquals(baseApiDto, dtos.get(0).getTarget());
        assertNull(dtos.get(0).getSelectedEntityType());
        assertEquals(Integer.valueOf(100), dtos.get(0).getMaxPercentage());
    }

    /**
     * Tests convert {@link MaxUtilizationApiDTO} objects to {@link MaxUtilizationLevel}.
     * All dto fields have values
     */
    @Test
    public void testGetMaxUtilizationChanges() {
        //GIVEN
        BaseApiDTO baseApiDTO = new BaseApiDTO();
        baseApiDTO.setUuid("23");

        MaxUtilizationApiDTO maxUtil = new MaxUtilizationApiDTO();
        maxUtil.setMaxPercentage(100);
        maxUtil.setSelectedEntityType(UIEntityType.VIRTUAL_MACHINE.apiStr());
        maxUtil.setTarget(baseApiDTO);

        List<MaxUtilizationApiDTO> maxUtilizations = new ArrayList<MaxUtilizationApiDTO>() {{
            add(maxUtil);
        }};

        //WHEN
        List<ScenarioChange> scenarioChanges = scenarioMapper.getMaxUtilizationChanges(maxUtilizations);

        //THEN
        assertEquals(1, scenarioChanges.size());
        MaxUtilizationLevel mLevel = scenarioChanges.get(0).getPlanChanges().getMaxUtilizationLevel();
        assertEquals(100, mLevel.getPercentage());
        assertEquals(UIEntityType.VIRTUAL_MACHINE.typeNumber(), mLevel.getSelectedEntityType());
        assertEquals(baseApiDTO.getUuid(), String.valueOf(mLevel.getGroupOid()));
    }

    /**
     * Tests convert {@link MaxUtilizationApiDTO} objects to {@link MaxUtilizationLevel}.
     * Target object missing
     */
    @Test
    public void testGetMaxUtilizationChangesWithNullGroup() {
        //GIVEN
        MaxUtilizationApiDTO maxUtil = new MaxUtilizationApiDTO();
        maxUtil.setMaxPercentage(100);
        maxUtil.setSelectedEntityType(UIEntityType.VIRTUAL_MACHINE.apiStr());
        List<MaxUtilizationApiDTO> maxUtilizations = new ArrayList<MaxUtilizationApiDTO>() {{
            add(maxUtil);
        }};

        //WHEN
        List<ScenarioChange> scenarioChanges = scenarioMapper.getMaxUtilizationChanges(maxUtilizations);

        //THEN
        assertEquals(1, scenarioChanges.size());
        MaxUtilizationLevel mLevel = scenarioChanges.get(0).getPlanChanges().getMaxUtilizationLevel();
        assertEquals(100, mLevel.getPercentage());
        assertEquals(UIEntityType.VIRTUAL_MACHINE.typeNumber(), mLevel.getSelectedEntityType());
        assertEquals(0, mLevel.getGroupOid());
    }

    /**
     * Tests convert {@link MaxUtilizationApiDTO} objects to {@link MaxUtilizationLevel}.
     * Tests conversion with targetEntityType null
     */
    @Test
    public void testGetMaxUtilizationChangesWithNullTargetEntityType() {
        //GIVEN
        BaseApiDTO baseApiDTO = new BaseApiDTO();
        baseApiDTO.setUuid("23");

        MaxUtilizationApiDTO maxUtil = new MaxUtilizationApiDTO();
        maxUtil.setMaxPercentage(100);
        maxUtil.setTarget(baseApiDTO);
        maxUtil.setMaxPercentage(100);

        List<MaxUtilizationApiDTO> maxUtilizations = new ArrayList<MaxUtilizationApiDTO>() {{
            add(maxUtil);
        }};

        //WHEN
        List<ScenarioChange> scenarioChanges = scenarioMapper.getMaxUtilizationChanges(maxUtilizations);

        //THEN
        assertEquals(1, scenarioChanges.size());
        MaxUtilizationLevel mLevel = scenarioChanges.get(0).getPlanChanges().getMaxUtilizationLevel();
        assertEquals(100, mLevel.getPercentage());
        assertEquals(baseApiDTO.getUuid(), String.valueOf(mLevel.getGroupOid()));
    }


    private SettingApiDTO<String> createStringSetting(final String uuid, final String value) {
        SettingApiDTO<String> riSetting = new SettingApiDTO<>();
        riSetting.setUuid(uuid);
        riSetting.setValue(value);
        return riSetting;
    }

    private ScenarioApiDTO scenarioApiForAlleviatePressurePlan(long sourceId, long destinationId) {
        RelievePressureObjectApiDTO relievePressureObjectApiDTO = new RelievePressureObjectApiDTO();
        relievePressureObjectApiDTO.setSources(Arrays.asList(template(sourceId)));
        relievePressureObjectApiDTO.setDestinations(Arrays.asList(template(destinationId)));
        TopologyChangesApiDTO topologyChanges = new TopologyChangesApiDTO();
        topologyChanges.setRelievePressureList(Arrays.asList(relievePressureObjectApiDTO));
        ScenarioApiDTO dto = scenarioApiDTO(topologyChanges);
        dto.setType(ScenarioMapper.ALLEVIATE_PRESSURE_PLAN_TYPE);
        return dto;
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
}
