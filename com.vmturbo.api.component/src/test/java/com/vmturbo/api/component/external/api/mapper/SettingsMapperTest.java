package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.component.external.api.mapper.SettingSpecStyleMappingLoader.SettingSpecStyleMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.PlanSettingInfo;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerInfo;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.DefaultSettingPolicyMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.DefaultSettingSpecMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.SettingApiDTOPossibilities;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.SettingPolicyMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.SettingSpecMapper;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.group.GroupApiDTO;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingOptionApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.dto.settingspolicy.RecurrenceApiDTO;
import com.vmturbo.api.dto.settingspolicy.ScheduleApiDTO;
import com.vmturbo.api.dto.settingspolicy.SettingsPolicyApiDTO;
import com.vmturbo.api.enums.DayOfWeek;
import com.vmturbo.api.enums.InputValueType;
import com.vmturbo.api.enums.RecurrenceType;
import com.vmturbo.api.enums.SettingScope;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GlobalSettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.Daily;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.Monthly;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.OneTime;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.Perpetual;
import com.vmturbo.common.protobuf.setting.SettingProto.Schedule.Weekly;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.SearchSettingSpecsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath.SettingCategoryPathNode;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.setting.OsMigrationSettingsEnum.OperatingSystem;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

public class SettingsMapperTest {
    private final String mgrId1 = "mgr1";
    private final String mgrName1 = "Manager1";
    private final String mgrCategory1 = "Category1";

    private final String mgrId2 = "mgr2";
    private final String mgrName2 = "Manager2";
    private final String mgrCategory2 = "Category2";

    private final long startTimestamp = 1564507800425L;
    private final long endTimestamp = 1564522200425L;
    private final long endDatestamp = 1564506000425L;

    private final int minutePeriod = 240;

    private final String startDateString = DateTimeUtil.toString(startTimestamp);
    private final String endTimeString = DateTimeUtil.toString(endTimestamp);
    private final String endDateString = DateTimeUtil.toString(endDatestamp);

    private final String settingSpec1EntityType = "VirtualMachine";
    private final SettingSpec settingSpec1 = SettingSpec.newBuilder()
            .setName("move")
            .setDisplayName("Move")
            .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                    .setTiebreaker(SettingTiebreaker.SMALLER)
                    .setEntitySettingScope(EntitySettingScope.newBuilder()
                            .setEntityTypeSet(EntityTypeSet.newBuilder().addEntityType(10))))
            .setEnumSettingValueType(EnumSettingValueType.newBuilder()
                    .addAllEnumValues(Arrays.asList("DISABLED", "MANUAL"))
                    .setDefault("MANUAL"))
            .build();

    private final SettingSpec settingSpec2 = SettingSpec.newBuilder(settingSpec1)
            .setName("alternativeMoveVM")
            .build();

    private final SettingSpec settingSpec3 = EntitySettingSpecs.ExcludedTemplates.getSettingSpec();

    private final SettingSpec settingSpec4 = GlobalSettingSpecs.RhelTargetOs.createSettingSpec();

    private SettingsManagerMapping settingMgrMapping = mock(SettingsManagerMapping.class);

    private SettingSpecStyleMapping settingStyleMapping = mock(SettingSpecStyleMapping.class);

    private SettingSpecMapper settingSpecMapper = mock(SettingSpecMapper.class);

    private SettingPolicyMapper policyMapper = mock(SettingPolicyMapper.class);

    private final SettingsManagerInfo mgr1Info = new SettingsManagerInfo(mgrName1, mgrCategory1,
                Collections.singleton(settingSpec1.getName()), mock(PlanSettingInfo.class));

    private final GroupServiceMole groupBackend = spy(new GroupServiceMole());

    private final SettingServiceMole settingBackend = spy(new SettingServiceMole());

    private final SettingPolicyServiceMole settingPolicyBackend =
            spy(new SettingPolicyServiceMole());

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(groupBackend,
            settingBackend, settingPolicyBackend);

    /**
     * Verify that the json file loading is correct.
     */
    @Test
    public void testLoadEndToEnd() throws IOException {
        SettingsMapper mapper = new SettingsMapper(
                SettingServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                GroupServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                SettingPolicyServiceGrpc.newBlockingStub(grpcServer.getChannel()),
                (new SettingsManagerMappingLoader("settingManagersTest.json")).getMapping(),
                (new SettingSpecStyleMappingLoader("settingSpecStyleTest.json")).getMapping());

        final Map<String, SettingsManagerApiDTO> mgrsByUuid = mapper.toManagerDtos(
                Arrays.asList(settingSpec1, settingSpec3, settingSpec4), Optional.empty()).stream()
                .collect(Collectors.toMap(BaseApiDTO::getUuid, Function.identity()));
        assertEquals(3, mgrsByUuid.size());

        final SettingsManagerApiDTO mgr = mgrsByUuid.get("automationmanager");
        assertEquals("automationmanager", mgr.getUuid());
        assertEquals("Action Mode Settings", mgr.getDisplayName());
        assertEquals("Automation", mgr.getCategory());
        assertEquals(1, mgr.getSettings().size());

        final SettingApiDTO<?> settingApiDTO = mgr.getSettings().get(0);
        assertEquals("move", settingApiDTO.getUuid());
        assertEquals("Move", settingApiDTO.getDisplayName());
        assertEquals("MANUAL", settingApiDTO.getDefaultValue());

        assertThat(settingApiDTO.getEntityType(), is("VirtualMachine"));
        assertEquals(InputValueType.STRING, settingApiDTO.getValueType());

        assertEquals(2, settingApiDTO.getOptions().size());
        // The order is important
        assertEquals("Disabled", settingApiDTO.getOptions().get(0).getLabel());
        assertEquals("DISABLED", settingApiDTO.getOptions().get(0).getValue());
        assertEquals("Manual", settingApiDTO.getOptions().get(1).getLabel());
        assertEquals("MANUAL", settingApiDTO.getOptions().get(1).getValue());

        //verify style info
        assertThat(settingApiDTO.getRange().getStep(), is(5.0));
        assertThat(settingApiDTO.getRange().getCustomStepValues(), containsInAnyOrder(90, 30, 7));
        assertThat(settingApiDTO.getRange().getLabels(),
            containsInAnyOrder("Performance", "Efficiency"));

        final SettingsManagerApiDTO mktomgr = mgrsByUuid.get("marketsettingsmanager");
        assertEquals("marketsettingsmanager", mktomgr.getUuid());
        assertEquals("Operational Constraints", mktomgr.getDisplayName());
        assertEquals("Analysis", mktomgr.getCategory());
        assertEquals(3, mktomgr.getSettings().size());

        final SettingApiDTO<?> teSettingApiDTO = mktomgr.getSettings().get(0);
        assertEquals("excludedTemplatesOids", teSettingApiDTO.getUuid());
        assertEquals("Excluded templates", teSettingApiDTO.getDisplayName());
        assertNull(teSettingApiDTO.getDefaultValue());

        assertThat(mktomgr.getSettings().stream().map(SettingApiDTO::getEntityType).collect(Collectors.toList()),
            containsInAnyOrder("VirtualMachine", "Database", "DatabaseServer"));
        assertEquals(InputValueType.LIST, teSettingApiDTO.getValueType());

        // verify the label for osmigrationmanager is set correctly
        final SettingsManagerApiDTO osMigrationMgr = mgrsByUuid.get("osmigrationmanager");
        Map<String, String> labelByEnum = osMigrationMgr.getSettings().get(0).getOptions().stream()
                .collect(Collectors.toMap(SettingOptionApiDTO::getValue,
                        SettingOptionApiDTO::getLabel));
        assertEquals("Linux", labelByEnum.get(OperatingSystem.LINUX.name()));
        assertEquals("RHEL", labelByEnum.get(OperatingSystem.RHEL.name()));
        assertEquals("SLES", labelByEnum.get(OperatingSystem.SLES.name()));
        assertEquals("Windows", labelByEnum.get(OperatingSystem.WINDOWS.name()));
    }

    @Test
    public void testToMgrDto() {
        final SettingsManagerMapping mapping = mock(SettingsManagerMapping.class);
        final SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        final SettingPolicyMapper policyMapper = mock(SettingPolicyMapper.class);
        when(mapping.getManagerInfo(mgrId1))
            .thenReturn(Optional.of(new SettingsManagerInfo(mgrName1, mgrCategory1,
                    Collections.singleton(settingSpec1.getName()), mock(PlanSettingInfo.class))));
        when(mapping.getManagerUuid(settingSpec1.getName())).thenReturn(Optional.of(mgrId1));

        final SettingApiDTO<String> settingApiDTO = new SettingApiDTO<>();
        settingApiDTO.setDisplayName("mockSetting");
        final SettingApiDTOPossibilities possibilities = mock(SettingApiDTOPossibilities.class);
        when(possibilities.getAll()).thenReturn(Collections.singletonList(settingApiDTO));
        when(specMapper.settingSpecToApi(eq(Optional.of(settingSpec1)), eq(Optional.empty())))
            .thenReturn(possibilities);
        when(settingStyleMapping.getStyleInfo(any())).thenReturn(Optional.empty());

        final SettingsMapper mapper = new SettingsMapper(mapping, settingStyleMapping, specMapper,
                policyMapper, grpcServer.getChannel());
        Optional<SettingsManagerApiDTO> mgrDtoOpt =
            mapper.toManagerDto(Collections.singletonList(settingSpec1), Optional.empty(), mgrId1);
        assertTrue(mgrDtoOpt.isPresent());

        SettingsManagerApiDTO mgrDto = mgrDtoOpt.get();
        assertEquals(mgrName1, mgrDto.getDisplayName());
        assertEquals(mgrCategory1, mgrDto.getCategory());
        assertEquals(1, mgrDto.getSettings().size());
        assertEquals("mockSetting", mgrDto.getSettings().get(0).getDisplayName());
    }

    @Test
    public void testMgrDtoNoMgr() {
        final SettingsManagerMapping mapping = mock(SettingsManagerMapping.class);
        final SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        final SettingPolicyMapper policyMapper = mock(SettingPolicyMapper.class);
        when(mapping.getManagerInfo(mgrId1)).thenReturn(Optional.empty());
        when(mapping.getManagerUuid(any())).thenReturn(Optional.empty());
        final SettingsMapper mapper = new SettingsMapper(mapping, settingStyleMapping, specMapper,
                policyMapper, grpcServer.getChannel());
        assertFalse(mapper.toManagerDto(Collections.singleton(settingSpec1), Optional.empty(),
                mgrId1).isPresent());
    }

    @Test
    public void testMgrDtoNoSetting() {
        final SettingsManagerMapping mapping = mock(SettingsManagerMapping.class);
        final SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        final SettingPolicyMapper policyMapper = mock(SettingPolicyMapper.class);
        when(mapping.getManagerInfo(mgrId1))
                .thenReturn(Optional.of(new SettingsManagerInfo(mgrName1, mgrCategory1,
                        Collections.emptySet(), mock(PlanSettingInfo.class))));
        when(mapping.getManagerUuid(settingSpec1.getName())).thenReturn(Optional.empty());
        when(mapping.getManagerUuid(settingSpec2.getName())).thenReturn(Optional.of(mgrId1 + "suffix"));

        final SettingsMapper mapper = new SettingsMapper(mapping, settingStyleMapping, specMapper,
                policyMapper, grpcServer.getChannel());
        Optional<SettingsManagerApiDTO> mgrDtoOpt =
            mapper.toManagerDto(Arrays.asList(settingSpec1, settingSpec2), Optional.empty(), mgrId1);
        assertTrue(mgrDtoOpt.isPresent());

        SettingsManagerApiDTO mgrDto = mgrDtoOpt.get();
        assertEquals(mgrName1, mgrDto.getDisplayName());
        assertEquals(mgrCategory1, mgrDto.getCategory());
        assertEquals(0, mgrDto.getSettings().size());
    }

    @Test
    public void testMgrDtos() {
        final SettingsManagerMapping mapping = mock(SettingsManagerMapping.class);
        final SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        final SettingPolicyMapper policyMapper = mock(SettingPolicyMapper.class);

        final SettingApiDTO<String> settingApiDTO1 = new SettingApiDTO<>();
        settingApiDTO1.setDisplayName("mockSetting1");
        final SettingApiDTOPossibilities possibilities1 = mock(SettingApiDTOPossibilities.class);
        when(possibilities1.getAll()).thenReturn(Collections.singletonList(settingApiDTO1));

        final SettingApiDTO<String> settingApiDTO2 = new SettingApiDTO<>();
        settingApiDTO2.setDisplayName("mockSetting2");
        final SettingApiDTOPossibilities possibilities2 = mock(SettingApiDTOPossibilities.class);
        when(possibilities2.getAll()).thenReturn(Collections.singletonList(settingApiDTO2));

        when(mapping.getManagerUuid(settingSpec1.getName())).thenReturn(Optional.of(mgrId1));
        when(mapping.getManagerUuid(settingSpec2.getName())).thenReturn(Optional.of(mgrId2));
        when(mapping.getManagerInfo(mgrId1))
                .thenReturn(Optional.of(new SettingsManagerInfo(mgrName1, mgrCategory1,
                        Collections.singleton(settingSpec1.getName()), mock(PlanSettingInfo.class))));
        when(mapping.getManagerInfo(mgrId2))
                .thenReturn(Optional.of(new SettingsManagerInfo(mgrName2, mgrCategory2,
                        Collections.singleton(settingSpec2.getName()), mock(PlanSettingInfo.class))));
        when(specMapper.settingSpecToApi(eq(Optional.of(settingSpec1)), eq(Optional.empty()))).thenReturn(possibilities1);
        when(specMapper.settingSpecToApi(eq(Optional.of(settingSpec2)), eq(Optional.empty()))).thenReturn(possibilities2);
        when(settingStyleMapping.getStyleInfo(any())).thenReturn(Optional.empty());

        final SettingsMapper mapper = new SettingsMapper(mapping, settingStyleMapping, specMapper,
                policyMapper, grpcServer.getChannel());
        final Map<String, SettingsManagerApiDTO> results = mapper.toManagerDtos(
                Arrays.asList(settingSpec1, settingSpec2), Optional.empty()).stream()
            .collect(Collectors.toMap(SettingsManagerApiDTO::getUuid, Function.identity()));

        assertTrue(results.containsKey(mgrId1));

        final SettingsManagerApiDTO mgrDto1 = results.get(mgrId1);
        assertEquals(mgrId1, mgrDto1.getUuid());
        assertEquals(mgrName1, mgrDto1.getDisplayName());
        assertEquals(mgrCategory1, mgrDto1.getCategory());
        assertEquals(1, mgrDto1.getSettings().size());
        assertEquals("mockSetting1", mgrDto1.getSettings().get(0).getDisplayName());
        assertTrue(results.containsKey(mgrId2));

        final SettingsManagerApiDTO mgrDto2 = results.get(mgrId2);
        assertEquals(mgrName2, mgrDto2.getDisplayName());
        assertEquals(mgrCategory2, mgrDto2.getCategory());
        assertEquals(1, mgrDto2.getSettings().size());
        assertEquals("mockSetting2", mgrDto2.getSettings().get(0).getDisplayName());
    }

    @Test
    public void testMgrDtosUnhandledSpecs() {
        final SettingsManagerMapping mapping = mock(SettingsManagerMapping.class);
        final SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        final SettingPolicyMapper policyMapper = mock(SettingPolicyMapper.class);
        final SettingsMapper mapper = new SettingsMapper(mapping, settingStyleMapping, specMapper,
                policyMapper, grpcServer.getChannel());
        when(mapping.getManagerUuid(settingSpec1.getName())).thenReturn(Optional.empty());
        assertTrue(mapper.toManagerDtos(Collections.singleton(settingSpec1), Optional.empty()).isEmpty());
    }

    @Test
    public void testSpecMapperSinglePath() {
        final DefaultSettingSpecMapper specMapper = new DefaultSettingSpecMapper();
        final SettingSpec singlePathSpec = SettingSpec.newBuilder(settingSpec1)
                .setPath(SettingCategoryPath.newBuilder()
                        .setRootPathNode(SettingCategoryPathNode.newBuilder()
                                .setNodeName("node1")))
                .build();

        final SettingApiDTO dto = specMapper.settingSpecToApi(Optional.of(singlePathSpec), Optional.empty())
                .getSettingForEntityType(settingSpec1EntityType).get();
        assertEquals(Collections.singletonList("node1"), dto.getCategories());
    }

    @Test
    public void testSpecMapperDoublePath() {
        final DefaultSettingSpecMapper specMapper = new DefaultSettingSpecMapper();
        final SettingSpec doublePathSpec = SettingSpec.newBuilder(settingSpec1)
                .setPath(SettingCategoryPath.newBuilder()
                        .setRootPathNode(SettingCategoryPathNode.newBuilder()
                                .setNodeName("node1")
                                .setChildNode(SettingCategoryPathNode.newBuilder()
                                        .setNodeName("node2"))))
                .build();

        final SettingApiDTO dto = specMapper.settingSpecToApi(Optional.of(doublePathSpec), Optional.empty())
                .getSettingForEntityType(settingSpec1EntityType)
                .get();
        assertEquals(Arrays.asList("node1", "node2"), dto.getCategories());
    }

    @Test
    public void testSpecMapperBoolean() {
        final DefaultSettingSpecMapper specMapper = new DefaultSettingSpecMapper();
        final SettingSpec boolSpec = SettingSpec.newBuilder(settingSpec1)
                .setBooleanSettingValueType(BooleanSettingValueType.newBuilder()
                        .setDefault(true))
                .build();
        final Setting setting = Setting.newBuilder()
                .setSettingSpecName(settingSpec1.getName())
                .setBooleanSettingValue(BooleanSettingValue.newBuilder()
                        .setValue(false))
                .build();
        final SettingApiDTO dto = specMapper.settingSpecToApi(Optional.of(boolSpec), Optional.of(setting))
                .getSettingForEntityType(settingSpec1EntityType)
                .get();
        assertThat(dto.getDefaultValue(), is("true"));
        assertThat(dto.getValue(), is("false"));
        assertThat(dto.getValueType(), is(InputValueType.BOOLEAN));
    }

    @Test
    public void testSpecMapperNumeric() {
        final DefaultSettingSpecMapper specMapper = new DefaultSettingSpecMapper();
        final SettingSpec numSpec = SettingSpec.newBuilder(settingSpec1)
                .setNumericSettingValueType(NumericSettingValueType.newBuilder()
                        .setDefault(10.5f)
                        .setMax(100f)
                        .setMin(1.7f))
                .build();
        final Setting setting = Setting.newBuilder()
                .setSettingSpecName(settingSpec1.getName())
                .setNumericSettingValue(NumericSettingValue.newBuilder()
                        .setValue(7.0f))
                .build();
        final SettingApiDTO dto = specMapper.settingSpecToApi(Optional.of(numSpec), Optional.of(setting))
                .getSettingForEntityType(settingSpec1EntityType)
                .get();
        assertThat(dto.getDefaultValue(), is("10.5"));
        assertThat(dto.getMin(), is((double)1.7f));
        assertThat(dto.getMax(), is((double)100f));
        assertThat(dto.getValueType(), is(InputValueType.NUMERIC));
        assertThat(dto.getValue(), is("7.0"));
    }

    @Test
    public void testSpecMapperString() {
        final DefaultSettingSpecMapper specMapper = new DefaultSettingSpecMapper();
        final SettingSpec stringSpec = SettingSpec.newBuilder(settingSpec1)
                .setStringSettingValueType(StringSettingValueType.newBuilder()
                        .setDefault("BOO!")
                        .setValidationRegex(".*"))
                .build();
        final Setting setting = Setting.newBuilder()
                .setSettingSpecName(settingSpec1.getName())
                .setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue("goat"))
                .build();
        final SettingApiDTO dto = specMapper.settingSpecToApi(Optional.of(stringSpec), Optional.of(setting))
                .getSettingForEntityType(settingSpec1EntityType)
                .get();
        assertThat(dto.getDefaultValue(), is("BOO!"));
        assertThat(dto.getValue(), is("goat"));
        assertThat(dto.getValueType(), is(InputValueType.STRING));
    }

    @Test
    public void testSpecMapperEnum() {
        final DefaultSettingSpecMapper specMapper = new DefaultSettingSpecMapper();
        final SettingSpec stringSpec = SettingSpec.newBuilder(settingSpec1)
            .setEnumSettingValueType(EnumSettingValueType.newBuilder()
                .setDefault("FOO")
                .addEnumValues("BAR")
                .addEnumValues("CAR"))
            .build();
        final Setting setting = Setting.newBuilder()
            .setSettingSpecName(settingSpec1.getName())
            .setEnumSettingValue(EnumSettingValue.newBuilder()
                .setValue("CAR"))
            .build();
        final SettingApiDTO dto = specMapper.settingSpecToApi(Optional.of(stringSpec), Optional.of(setting))
            .getSettingForEntityType(settingSpec1EntityType)
            .get();
        assertThat(dto.getDefaultValue(), is("FOO"));
        assertThat(dto.getValue(), is("CAR"));
        assertThat(dto.getValueType(), is(InputValueType.STRING));
    }

    @Test
    public void testSpecMapperGlobalScope() {
        final DefaultSettingSpecMapper specMapper = new DefaultSettingSpecMapper();
        final SettingSpec stringSpec = SettingSpec.newBuilder(settingSpec1)
                .setGlobalSettingSpec(GlobalSettingSpec.getDefaultInstance())
                .build();
        final SettingApiDTO dto = specMapper.settingSpecToApi(Optional.of(stringSpec), Optional.empty())
                .getGlobalSetting().get();
        assertEquals(SettingScope.GLOBAL, dto.getScope());
    }

    private SettingsMapper setUpMapper() {
        final SettingsManagerMapping mapping = mock(SettingsManagerMapping.class);
        final SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        final SettingPolicyMapper policyMapper = mock(SettingPolicyMapper.class);

        return new SettingsMapper(mapping, settingStyleMapping, specMapper,
                policyMapper, grpcServer.getChannel());
    }

    private SettingApiDTO makeSetting(String uuid, String value) {
        final SettingApiDTO<String> setting = new SettingApiDTO<>();
        setting.setUuid(uuid);
        setting.setValue(value);
        return setting;
    }

    /**
     * Make the default {@link SettingsPolicyApiDTO}.
     *
     * @return a {@link SettingsPolicyApiDTO}
     */
    private SettingsPolicyApiDTO makeSettingsPolicyApiDto() {
        final SettingApiDTO boolSetting = makeSetting("bool setting", "true");

        final SettingApiDTO numSetting = makeSetting("num setting", "10");

        final SettingApiDTO stringSetting = makeSetting("string setting", "foo");

        final SettingApiDTO enumSetting = makeSetting("enum setting", "VAL");

        final SettingApiDTO setOfOidSetting = makeSetting("set of oid setting", "1,2,3");

        return makeSettingsPolicyApiDto(
            boolSetting, numSetting, stringSetting, enumSetting, setOfOidSetting);
    }

    private SettingsPolicyApiDTO makeSettingsPolicyApiDto(SettingApiDTO<?> boolSetting,
                                                          SettingApiDTO<?> numSetting,
                                                          SettingApiDTO<?> stringSetting,
                                                          SettingApiDTO<?> enumSetting,
                                                          SettingApiDTO<?> setOfOidSetting) {
        final SettingsPolicyApiDTO settingsPolicyApiDTO = new SettingsPolicyApiDTO();

        when(settingBackend.searchSettingSpecs(any())).thenReturn(ImmutableList.of(
            SettingSpec.newBuilder()
                .setName(boolSetting.getUuid())
                .setBooleanSettingValueType(BooleanSettingValueType.getDefaultInstance())
                .build(),
            SettingSpec.newBuilder()
                .setName(numSetting.getUuid())
                .setNumericSettingValueType(NumericSettingValueType.getDefaultInstance())
                .build(),
            SettingSpec.newBuilder()
                .setName(stringSetting.getUuid())
                .setStringSettingValueType(StringSettingValueType.getDefaultInstance())
                .build(),
            SettingSpec.newBuilder()
                .setName(enumSetting.getUuid())
                .setEnumSettingValueType(EnumSettingValueType.getDefaultInstance())
                .build(),
            SettingSpec.newBuilder()
                .setName(setOfOidSetting.getUuid())
                .setSortedSetOfOidSettingValueType(SortedSetOfOidSettingValueType.getDefaultInstance())
                .build()
        ));

        final SettingsManagerApiDTO settingMgr1 = new SettingsManagerApiDTO();
        settingMgr1.setSettings(Arrays.asList(boolSetting, numSetting));
        final SettingsManagerApiDTO settingMgr2 = new SettingsManagerApiDTO();
        settingMgr2.setSettings(Arrays.asList(stringSetting, enumSetting, setOfOidSetting));

        settingsPolicyApiDTO.setSettingsManagers(Arrays.asList(settingMgr1, settingMgr2));
        settingsPolicyApiDTO.setDefault(false);

        final GroupApiDTO group = new GroupApiDTO();
        group.setUuid("7");
        settingsPolicyApiDTO.setScopes(Collections.singletonList(group));

        settingsPolicyApiDTO.setDisplayName("A Setting Policy is neither a Setting, not a Policy." +
                " Like a pineapple.");
        settingsPolicyApiDTO.setEntityType(UIEntityType.VIRTUAL_MACHINE.apiStr());
        settingsPolicyApiDTO.setDisabled(false);

        return settingsPolicyApiDTO;
    }

    @Test
    public void testMapInputPolicy() throws InvalidOperationException {

        final SettingsMapper mapper = setUpMapper();

        final SettingApiDTO boolSetting = makeSetting("bool setting", "true");
        final Setting boolSettingProto = Setting.newBuilder()
                .setSettingSpecName(boolSetting.getUuid())
                .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(true))
                .build();

        final SettingApiDTO numSetting = makeSetting("num setting", "10");
        final Setting numSettingProto = Setting.newBuilder()
                .setSettingSpecName(numSetting.getUuid())
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(10))
                .build();

        final SettingApiDTO stringSetting = makeSetting("string setting", "foo");
        final Setting strSettingProto = Setting.newBuilder()
                .setSettingSpecName(stringSetting.getUuid())
                .setStringSettingValue(StringSettingValue.newBuilder().setValue("foo"))
                .build();

        final SettingApiDTO enumSetting = makeSetting("enum setting", "VAL");
        final Setting enumSettingProto = Setting.newBuilder()
                .setSettingSpecName(enumSetting.getUuid())
                .setEnumSettingValue(EnumSettingValue.newBuilder().setValue("VAL"))
                .build();

        final SettingApiDTO setOfOidSetting = makeSetting("set of oid setting", "4,2,1,2,3,4,3");
        final Setting setOfOidSettingProto = Setting.newBuilder()
            .setSettingSpecName(setOfOidSetting.getUuid())
            .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                .addAllOids(Arrays.asList(1L, 2L, 3L, 4L))).build();

        final SettingsPolicyApiDTO settingsPolicyApiDTO = makeSettingsPolicyApiDto(boolSetting, numSetting,
                stringSetting, enumSetting, setOfOidSetting);

        final int entityType = EntityType.VIRTUAL_MACHINE.getNumber();

        final ScheduleApiDTO once = makeBasicScheduleDTO();
        settingsPolicyApiDTO.setSchedule(once);

        final SettingPolicyInfo info = mapper.convertInputPolicy(settingsPolicyApiDTO, entityType);
        assertEquals(settingsPolicyApiDTO.getDisplayName(), info.getName());
        assertEquals(EntityType.VIRTUAL_MACHINE.getNumber(), info.getEntityType());
        assertEquals(true, info.getEnabled());
        assertTrue(info.hasScope());
        assertThat(info.getScope().getGroupsList(), containsInAnyOrder(7L));
        assertThat(info.getSettingsList(),
                containsInAnyOrder(boolSettingProto, numSettingProto,
                        strSettingProto, enumSettingProto, setOfOidSettingProto));
        assertTrue(info.hasSchedule());
        final Schedule schedule = info.getSchedule();
        verifyBasicSchedule(schedule);
        assertTrue(schedule.hasOneTime());
    }

    @Test
    public void testMapInputPolicyDailyEndDate() throws InvalidOperationException {

        final SettingsMapper mapper = setUpMapper();

        final SettingsPolicyApiDTO settingsPolicyApiDTO = makeSettingsPolicyApiDto();

        final int entityType = EntityType.VIRTUAL_MACHINE.getNumber();

        final ScheduleApiDTO dailyLastDay = makeBasicScheduleDTO();
        dailyLastDay.setEndDate(Long.toString(endDatestamp));
        final RecurrenceApiDTO dailyRec = new RecurrenceApiDTO();
        dailyRec.setType(RecurrenceType.DAILY);
        dailyLastDay.setRecurrence(dailyRec);
        settingsPolicyApiDTO.setSchedule(dailyLastDay);

        final SettingPolicyInfo info = mapper.convertInputPolicy(settingsPolicyApiDTO, entityType);
        assertTrue(info.hasSchedule());
        final Schedule schedule = info.getSchedule();
        verifyBasicSchedule(schedule);
        assertTrue(schedule.hasDaily());
        assertTrue(schedule.hasLastDate());
        assertEquals(schedule.getLastDate(), endDatestamp);

    }

    @Test
    public void testMapInputPolicyDailyPerpetual() throws InvalidOperationException {
        final SettingsMapper mapper = setUpMapper();

        final SettingsPolicyApiDTO settingsPolicyApiDTO = makeSettingsPolicyApiDto();

        final int entityType = EntityType.VIRTUAL_MACHINE.getNumber();

        final ScheduleApiDTO dailyPerpet = makeBasicScheduleDTO();
        final RecurrenceApiDTO dailyRec = new RecurrenceApiDTO();
        dailyRec.setType(RecurrenceType.DAILY);
        dailyPerpet.setRecurrence(dailyRec);
        settingsPolicyApiDTO.setSchedule(dailyPerpet);

        final SettingPolicyInfo info = mapper.convertInputPolicy(settingsPolicyApiDTO, entityType);
        assertTrue(info.hasSchedule());
        final Schedule schedule = info.getSchedule();
        verifyBasicSchedule(schedule);
        assertTrue(schedule.hasDaily());
        assertTrue(schedule.hasPerpetual());
    }

    @Test
    public void testMapInputPolicyWeeklyUnspecifiedDay() throws InvalidOperationException {
        final SettingsMapper mapper = setUpMapper();

        final SettingsPolicyApiDTO settingsPolicyApiDTO = makeSettingsPolicyApiDto();

        final int entityType = EntityType.VIRTUAL_MACHINE.getNumber();

        final ScheduleApiDTO weeklyUnspecified = makeBasicScheduleDTO();
        final RecurrenceApiDTO weeklyRec = new RecurrenceApiDTO();
        weeklyRec.setType(RecurrenceType.WEEKLY);
        weeklyUnspecified.setRecurrence(weeklyRec);
        settingsPolicyApiDTO.setSchedule(weeklyUnspecified);

        // Because no day of the week was specified, the day of the week will
        // be set based on the start timestamp.  We need to convert the start time
        // to a day of the week
        final Date startDateTime = new Date(startTimestamp);
        final int weekdayNumber = startDateTime.toInstant()
                        .atOffset(ZoneOffset.UTC).toLocalDate().getDayOfWeek().getValue();

        final SettingPolicyInfo info = mapper.convertInputPolicy(settingsPolicyApiDTO, entityType);
        assertTrue(info.hasSchedule());
        final Schedule schedule = info.getSchedule();
        verifyBasicSchedule(schedule);
        assertTrue(schedule.hasPerpetual());
        assertTrue(schedule.hasWeekly());
        assertEquals(1, schedule.getWeekly().getDaysOfWeekCount());
        assertEquals(weekdayNumber, schedule.getWeekly().getDaysOfWeek(0).getNumber());
    }

    @Test
    public void testMapInputPolicyWeeklySpecificDay() throws InvalidOperationException {
        final SettingsMapper mapper = setUpMapper();

        final SettingsPolicyApiDTO settingsPolicyApiDTO = makeSettingsPolicyApiDto();

        final int entityType = EntityType.VIRTUAL_MACHINE.getNumber();

        final ScheduleApiDTO weeklySpecified = makeBasicScheduleDTO();
        final RecurrenceApiDTO weeklyRec = new RecurrenceApiDTO();
        weeklyRec.setType(RecurrenceType.WEEKLY);
        weeklyRec.setDaysOfWeek(Lists.newArrayList(DayOfWeek.Fri, DayOfWeek.Sun));
        weeklySpecified.setRecurrence(weeklyRec);
        settingsPolicyApiDTO.setSchedule(weeklySpecified);
        final SettingPolicyInfo info = mapper.convertInputPolicy(settingsPolicyApiDTO, entityType);
        assertTrue(info.hasSchedule());
        final Schedule schedule = info.getSchedule();
        verifyBasicSchedule(schedule);
        assertTrue(schedule.hasPerpetual());
        assertTrue(schedule.hasWeekly());
        assertEquals(2, schedule.getWeekly().getDaysOfWeekList().size());
        assertThat(schedule.getWeekly().getDaysOfWeekList(), containsInAnyOrder(Schedule.DayOfWeek.FRIDAY,
                Schedule.DayOfWeek.SUNDAY));
    }

    @Test
    public void testMapInputPolicyMonthlyUnspecifiedDay() throws InvalidOperationException {
        final SettingsMapper mapper = setUpMapper();

        final SettingsPolicyApiDTO settingsPolicyApiDTO = makeSettingsPolicyApiDto();

        final int entityType = EntityType.VIRTUAL_MACHINE.getNumber();

        final ScheduleApiDTO monthlyUnspecified = makeBasicScheduleDTO();
        final RecurrenceApiDTO monthlyRec = new RecurrenceApiDTO();
        monthlyRec.setType(RecurrenceType.MONTHLY);
        monthlyUnspecified.setRecurrence(monthlyRec);
        settingsPolicyApiDTO.setSchedule(monthlyUnspecified);

        final SettingPolicyInfo info = mapper.convertInputPolicy(settingsPolicyApiDTO, entityType);

        // Because no day of the month was specified, the day of the month will
        // be set based on the start timestamp.
        final Date startDateTime = new Date(startTimestamp);
        final int dayOfMonth = startDateTime.toInstant()
                        .atOffset(ZoneOffset.UTC).toLocalDate().getDayOfMonth();

        assertTrue(info.hasSchedule());
        final Schedule schedule = info.getSchedule();
        verifyBasicSchedule(schedule);
        assertTrue(schedule.hasPerpetual());
        assertTrue(schedule.hasMonthly());
        assertEquals(1, schedule.getMonthly().getDaysOfMonthCount());
        assertEquals(dayOfMonth, schedule.getMonthly().getDaysOfMonth(0));
    }

    @Test
    public void testMapInputPolicyMonthlySpecificDay() throws InvalidOperationException {
        final SettingsMapper mapper = setUpMapper();

        final SettingsPolicyApiDTO settingsPolicyApiDTO = makeSettingsPolicyApiDto();

        final int entityType = EntityType.VIRTUAL_MACHINE.getNumber();

        final ScheduleApiDTO monthlySpecified = makeBasicScheduleDTO();
        final RecurrenceApiDTO monthlyRec = new RecurrenceApiDTO();
        monthlyRec.setType(RecurrenceType.MONTHLY);
        monthlyRec.setDaysOfMonth(Collections.singletonList(3));
        monthlySpecified.setRecurrence(monthlyRec);
        settingsPolicyApiDTO.setSchedule(monthlySpecified);

        final SettingPolicyInfo info = mapper.convertInputPolicy(settingsPolicyApiDTO, entityType);
        assertTrue(info.hasSchedule());
        final Schedule schedule = info.getSchedule();
        verifyBasicSchedule(schedule);
        assertTrue(schedule.hasPerpetual());
        assertTrue(schedule.hasMonthly());
        assertEquals(schedule.getMonthly().getDaysOfMonthList(), Collections.singletonList(3));

    }

    private ScheduleApiDTO makeBasicScheduleDTO() {
        ScheduleApiDTO scheduleApiDTO = new ScheduleApiDTO();
        scheduleApiDTO.setEndTime(Long.toString(endTimestamp));
        scheduleApiDTO.setStartTime(Long.toString(startTimestamp));
        scheduleApiDTO.setStartDate(Long.toString(startTimestamp));
        return scheduleApiDTO;
    }

    private void verifyBasicSchedule(Schedule schedule) {
        assertTrue(schedule.hasStartTime());
        assertEquals(schedule.getStartTime(), startTimestamp);
        assertTrue(schedule.hasEndTime());
        assertEquals(schedule.getEndTime(), endTimestamp);
    }

    @Test(expected = InvalidOperationException.class)
    public void testInputPolicyNoSpecs() throws InvalidOperationException {
        final SettingsManagerMapping mapping = mock(SettingsManagerMapping.class);
        final SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        final SettingPolicyMapper policyMapper = mock(SettingPolicyMapper.class);

        final SettingsMapper mapper = new SettingsMapper(mapping, settingStyleMapping, specMapper,
                policyMapper, grpcServer.getChannel());

        final SettingsPolicyApiDTO inputDto = new SettingsPolicyApiDTO();
        final SettingsManagerApiDTO mgrDto = new SettingsManagerApiDTO();
        final SettingApiDTO<?> setting = new SettingApiDTO<>();
        setting.setUuid("testSetting");
        mgrDto.setSettings(Collections.singletonList(setting));
        inputDto.setSettingsManagers(Collections.singletonList(mgrDto));

        mapper.convertInputPolicy(inputDto, 1);
    }

    private DefaultSettingPolicyMapper setUpPolicyMapperInfoToDtoTest() {
        final SettingsManagerMapping mapping = mock(SettingsManagerMapping.class);
        final SettingsMapper mapper = mock(SettingsMapper.class);
        final SettingApiDTOPossibilities possibilities = mock(SettingApiDTOPossibilities.class);
        when(possibilities.getSettingForEntityType(any())).thenReturn(Optional.empty());
        when(possibilities.getAll()).thenReturn(Collections.emptyList());
        when(possibilities.getGlobalSetting()).thenReturn(Optional.empty());
        when(mapper.toSettingApiDto(any())).thenReturn(possibilities);
        when(mapper.getManagerMapping()).thenReturn(mapping);
        when(mapping.getManagerUuid(eq(settingSpec1.getName()))).thenReturn(Optional.of(mgrId1));
        when(mapping.getManagerInfo(mgrId1))
                .thenReturn(Optional.of(mgr1Info));

        final SettingServiceBlockingStub settingServiceClient =
               SettingServiceGrpc.newBlockingStub(grpcServer.getChannel());
        return new DefaultSettingPolicyMapper(mapper, settingServiceClient);
    }

    @Test
    public void testMapPolicyInfoToApiDto() {

        final SettingsManagerMapping mapping = mock(SettingsManagerMapping.class);
        final SettingsMapper mapper = mock(SettingsMapper.class);
        when(mapper.getManagerMapping()).thenReturn(mapping);
        when(mapping.getManagerUuid(eq(settingSpec1.getName()))).thenReturn(Optional.of(mgrId1));
        when(mapping.getManagerInfo(mgrId1))
                .thenReturn(Optional.of(mgr1Info));

        final DefaultSettingPolicyMapper policyMapper =
                new DefaultSettingPolicyMapper(mapper,
                        SettingServiceGrpc.newBlockingStub(grpcServer.getChannel()));

        final long groupId = 7;
        final String groupName = "goat";

        final Setting setting = Setting.newBuilder()
                .setSettingSpecName(settingSpec1.getName())
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                        .setValue("AUTOMATIC"))
                .build();
        final SettingApiDTO<String> mappedDto = new SettingApiDTO<>();
        final SettingApiDTOPossibilities possibilities = mock(SettingApiDTOPossibilities.class);
        when(possibilities.getSettingForEntityType("VirtualMachine")).thenReturn(Optional.of(mappedDto));
        when(mapper.toSettingApiDto(setting)).thenReturn(possibilities);

        final SettingPolicy settingPolicy = SettingPolicy.newBuilder()
                .setId(1)
                .setSettingPolicyType(Type.USER)
                .setInfo(SettingPolicyInfo.newBuilder()
                        .setName("foo")
                        .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                        .setEnabled(true)
                        .setScope(Scope.newBuilder()
                                .addGroups(7L))
                        .addSettings(setting)
                    .setSchedule(Schedule.newBuilder().setStartTime(startTimestamp)
                            .setEndTime(endTimestamp)
                            .setOneTime(OneTime.newBuilder())))
                .build();

        final SettingsPolicyApiDTO retDto = policyMapper.convertSettingPolicy(settingPolicy,
                ImmutableMap.of(groupId, groupName), new HashMap<>(), Collections.emptySet());
        assertEquals("foo", retDto.getDisplayName());
        assertEquals("1", retDto.getUuid());
        assertFalse(retDto.getDisabled());
        assertFalse(retDto.isDefault());
        final List<SettingsManagerApiDTO> mgrDtos = retDto.getSettingsManagers();
        assertEquals(1, mgrDtos.size());
        final SettingsManagerApiDTO mgr = mgrDtos.get(0);
        assertEquals(mgrId1, mgr.getUuid());
        assertEquals(mgrName1, mgr.getDisplayName());
        assertEquals(mgrCategory1, mgr.getCategory());
        assertThat(mgr.getSettings(), containsInAnyOrder(mappedDto));

        final ScheduleApiDTO scheduleApiDTO = retDto.getSchedule();
        verifyBasicScheduleDTO(scheduleApiDTO);
        assertEquals(scheduleApiDTO.getEndDate(), null);

    }

    @Test
    public void testMapPolicyInfoToApiDtoMinuteDuration() {

        final DefaultSettingPolicyMapper policyMapper = setUpPolicyMapperInfoToDtoTest();

        final SettingPolicy settingPolicy = SettingPolicy.newBuilder()
                .setId(1)
                .setSettingPolicyType(Type.USER)
                .setInfo(makeStandardSettingPolicyInfoBuilder()
                        .setSchedule(Schedule.newBuilder().setStartTime(startTimestamp)
                                .setOneTime(OneTime.newBuilder())
                                .setMinutes(minutePeriod).build())
                        .build())
                .build();

        final SettingsPolicyApiDTO retDto = policyMapper.convertSettingPolicy(settingPolicy,
                ImmutableMap.of(7L, "goat"), new HashMap<>(), Collections.emptySet());

        final ScheduleApiDTO scheduleApiDTO = retDto.getSchedule();
        verifyBasicScheduleDTO(scheduleApiDTO);
        assertEquals(scheduleApiDTO.getEndDate(), null);
    }

    @Test
    public void testMapPolicyInfoToApiDtoDailyPerpetual() {

        final DefaultSettingPolicyMapper policyMapper = setUpPolicyMapperInfoToDtoTest();

        final SettingPolicy settingPolicy = SettingPolicy.newBuilder()
                .setId(1)
                .setSettingPolicyType(Type.USER)
                .setInfo(makeStandardSettingPolicyInfoBuilder()
                        .setSchedule(Schedule.newBuilder().setStartTime(startTimestamp)
                                .setDaily(Daily.newBuilder().build())
                                .setMinutes(minutePeriod)
                                .setPerpetual(Perpetual.newBuilder().build()).build())
                        .build())
                .build();

        final SettingsPolicyApiDTO retDto = policyMapper.convertSettingPolicy(settingPolicy,
                ImmutableMap.of(7L, "goat"), new HashMap<>(), Collections.emptySet());

        final ScheduleApiDTO scheduleApiDTO = retDto.getSchedule();
        verifyBasicScheduleDTO(scheduleApiDTO);
        assertEquals(scheduleApiDTO.getEndDate(), null);
        assertEquals(scheduleApiDTO.getRecurrence().getType(), RecurrenceType.DAILY);
    }

    @Test
    public void testMapPolicyInfoToApiDtoDailyEndDate() {

        final DefaultSettingPolicyMapper policyMapper = setUpPolicyMapperInfoToDtoTest();

        final SettingPolicy settingPolicy = SettingPolicy.newBuilder()
                .setId(1)
                .setSettingPolicyType(Type.USER)
                .setInfo(makeStandardSettingPolicyInfoBuilder()
                        .setSchedule(Schedule.newBuilder().setStartTime(startTimestamp)
                                .setDaily(Daily.newBuilder())
                                .setMinutes(minutePeriod)
                                .setLastDate(endDatestamp)))
                .build();

        final SettingsPolicyApiDTO retDto = policyMapper.convertSettingPolicy(settingPolicy,
                ImmutableMap.of(7L, "goat"), new HashMap<>(), Collections.emptySet());

        final ScheduleApiDTO scheduleApiDTO = retDto.getSchedule();
        verifyBasicScheduleDTO(scheduleApiDTO);
        assertEquals(scheduleApiDTO.getEndDate(), endDateString);
        assertEquals(scheduleApiDTO.getRecurrence().getType(), RecurrenceType.DAILY);
    }

    @Test
    public void testMapPolicyInfoToApiDtoWeeklyUnspecified() {

        final DefaultSettingPolicyMapper policyMapper = setUpPolicyMapperInfoToDtoTest();

        final SettingPolicy settingPolicy = SettingPolicy.newBuilder()
                .setId(1)
                .setSettingPolicyType(Type.USER)
                .setInfo(makeStandardSettingPolicyInfoBuilder()
                        .setSchedule(Schedule.newBuilder().setStartTime(startTimestamp)
                                .setWeekly(Weekly.newBuilder().build())
                                .setMinutes(minutePeriod)
                                .setPerpetual(Perpetual.newBuilder().build()).build())
                        .build())
                .build();

        final SettingsPolicyApiDTO retDto = policyMapper.convertSettingPolicy(settingPolicy,
                ImmutableMap.of(7L, "goat"), new HashMap<>(), Collections.emptySet());

        // Because no day of the week was specified, the day of the week will
        // be set based on the start timestamp.  We need to convert the start time
        // to a day of the week
        final Date startDateTime = new Date(startTimestamp);
        // Also the api and the internal day representations are different
        final int weekdayNumber = startDateTime.toInstant()
                        .atOffset(ZoneOffset.UTC)
                        .toLocalDate().getDayOfWeek().getValue() % 7 + 1;

        final ScheduleApiDTO scheduleApiDTO = retDto.getSchedule();
        verifyBasicScheduleDTO(scheduleApiDTO);
        assertEquals(scheduleApiDTO.getEndDate(), null);
        assertEquals(scheduleApiDTO.getRecurrence().getType(), RecurrenceType.WEEKLY);
        assertEquals(1, scheduleApiDTO.getRecurrence().getDaysOfWeek().size());
        assertEquals(weekdayNumber, scheduleApiDTO.getRecurrence().getDaysOfWeek().get(0).getValue());
    }

    @Test
    public void testMapPolicyInfoToApiDtoWeeklySpecified() {

        final DefaultSettingPolicyMapper policyMapper = setUpPolicyMapperInfoToDtoTest();

        final SettingPolicy settingPolicy = SettingPolicy.newBuilder()
                .setId(1)
                .setSettingPolicyType(Type.USER)
                .setInfo(makeStandardSettingPolicyInfoBuilder()
                        .setSchedule(Schedule.newBuilder().setStartTime(startTimestamp)
                                .setWeekly(Weekly.newBuilder()
                                        .addDaysOfWeek(Schedule.DayOfWeek.FRIDAY))
                                .setMinutes(minutePeriod)
                                .setPerpetual(Perpetual.newBuilder().build()).build())
                        .build())
                .build();

        final SettingsPolicyApiDTO retDto = policyMapper.convertSettingPolicy(settingPolicy,
                ImmutableMap.of(7L, "goat"), new HashMap<>(), Collections.emptySet());

        final ScheduleApiDTO scheduleApiDTO = retDto.getSchedule();
        verifyBasicScheduleDTO(scheduleApiDTO);
        assertEquals(scheduleApiDTO.getEndDate(), null);
        assertEquals(scheduleApiDTO.getRecurrence().getType(), RecurrenceType.WEEKLY);
        assertEquals(scheduleApiDTO.getRecurrence().getDaysOfWeek(),
                Collections.singletonList(DayOfWeek.Fri));
    }

    @Test
    public void testMapPolicyInfoToApiDtoMonthlyUnspecified() {

        final DefaultSettingPolicyMapper policyMapper = setUpPolicyMapperInfoToDtoTest();

        final SettingPolicy settingPolicy = SettingPolicy.newBuilder()
                .setId(1)
                .setSettingPolicyType(Type.USER)
                .setInfo(makeStandardSettingPolicyInfoBuilder()
                        .setSchedule(Schedule.newBuilder().setStartTime(startTimestamp)
                                .setMonthly(Monthly.newBuilder().build())
                                .setMinutes(minutePeriod)
                                .setPerpetual(Perpetual.newBuilder().build()).build())
                        .build())
                .build();

        final SettingsPolicyApiDTO retDto = policyMapper.convertSettingPolicy(settingPolicy,
                ImmutableMap.of(7L, "goat"), new HashMap<>(), Collections.emptySet());

        // Because no day of the month was specified, the day of the month will
        // be set based on the start timestamp.
        final Date startDateTime = new Date(startTimestamp);
        final int dayOfMonth = startDateTime.toInstant()
                        .atOffset(ZoneOffset.UTC).toLocalDate().getDayOfMonth();

        final ScheduleApiDTO scheduleApiDTO = retDto.getSchedule();
        verifyBasicScheduleDTO(scheduleApiDTO);
        assertEquals(scheduleApiDTO.getEndDate(), null);
        assertEquals(scheduleApiDTO.getRecurrence().getType(), RecurrenceType.MONTHLY);
        assertEquals(1, scheduleApiDTO.getRecurrence().getDaysOfMonth().size());
        assertEquals(dayOfMonth, scheduleApiDTO.getRecurrence().getDaysOfMonth().get(0).intValue());
    }

    @Test
    public void testMapPolicyInfoToApiDtoMonthlySpecified() {

        final DefaultSettingPolicyMapper policyMapper = setUpPolicyMapperInfoToDtoTest();

        final SettingPolicy settingPolicy = SettingPolicy.newBuilder()
                .setId(1)
                .setSettingPolicyType(Type.USER)
                .setInfo(makeStandardSettingPolicyInfoBuilder()
                        .setSchedule(Schedule.newBuilder().setStartTime(startTimestamp)
                                .setMonthly(Monthly.newBuilder().addDaysOfMonth(3).build())
                                .setMinutes(minutePeriod)
                                .setPerpetual(Perpetual.newBuilder().build()).build())
                        .build())
                .build();

        final SettingsPolicyApiDTO retDto = policyMapper.convertSettingPolicy(settingPolicy,
                ImmutableMap.of(7L, "goat"), new HashMap<>(), Collections.emptySet());

        final ScheduleApiDTO scheduleApiDTO = retDto.getSchedule();
        verifyBasicScheduleDTO(scheduleApiDTO);
        assertEquals(scheduleApiDTO.getEndDate(), null);
        assertEquals(scheduleApiDTO.getRecurrence().getType(), RecurrenceType.MONTHLY);
        assertEquals(scheduleApiDTO.getRecurrence().getDaysOfMonth(), Collections.singletonList(3));
    }

    private SettingPolicyInfo.Builder makeStandardSettingPolicyInfoBuilder() {
        return SettingPolicyInfo.newBuilder()
                        .setName("foo")
                        .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                        .setEnabled(true)
                        .setScope(Scope.newBuilder()
                                .addGroups(7L))
                        .addSettings(Setting.newBuilder()
                                .setSettingSpecName(settingSpec1.getName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder()
                                        .setValue("AUTOMATIC"))
                                .build());
    }

    private void verifyBasicScheduleDTO(ScheduleApiDTO scheduleApiDTO) {
        assertNotEquals(scheduleApiDTO, null);
        assertEquals(scheduleApiDTO.getStartDate(), startDateString);
        assertEquals(scheduleApiDTO.getStartTime(), startDateString);
        assertEquals(scheduleApiDTO.getEndTime(), endTimeString);
    }

    @Test
    public void testValMgrDtoBool() {
        final SettingsMapper mapper = mock(SettingsMapper.class);
        final SettingServiceBlockingStub settingServiceClient =
             SettingServiceGrpc.newBlockingStub(grpcServer.getChannel());
        final DefaultSettingPolicyMapper policyMapper =
                new DefaultSettingPolicyMapper(mapper, settingServiceClient);

        final String entityType = "testType";
        final Setting setting = Setting.newBuilder()
                        .setSettingSpecName("setting")
                        .setBooleanSettingValue(BooleanSettingValue.newBuilder()
                                .setValue(true))
                        .build();
        final SettingApiDTO<String> mappedDto = new SettingApiDTO<>();
        final SettingApiDTOPossibilities possibilities = mock(SettingApiDTOPossibilities.class);
        when(possibilities.getSettingForEntityType(entityType)).thenReturn(Optional.of(mappedDto));
        when(mapper.toSettingApiDto(setting)).thenReturn(possibilities);
        final SettingsManagerApiDTO mgr = policyMapper.createValMgrDto(mgrId1, mgr1Info, entityType,
                Collections.singletonList(setting));

        assertEquals(mgrId1, mgr.getUuid());
        assertEquals(mgrName1, mgr.getDisplayName());
        assertEquals(mgrCategory1, mgr.getCategory());
        assertEquals(1, mgr.getSettings().size());
        assertThat(mgr.getSettings().get(0), is(mappedDto));
    }


    @Test
    public void testConvertSettingPoliciesNoInvolvedGroups() {
        final SettingsManagerMapping mapping = mock(SettingsManagerMapping.class);
        final SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        final SettingPolicyMapper policyMapper = mock(SettingPolicyMapper.class);
        final SettingsMapper mapper = new SettingsMapper(mapping, settingStyleMapping, specMapper,
                policyMapper, grpcServer.getChannel());

        final SettingPolicy policy = SettingPolicy.getDefaultInstance();
        final SettingsPolicyApiDTO retDto = new SettingsPolicyApiDTO();
        when(policyMapper.convertSettingPolicy(policy, Collections.emptyMap(), new HashMap<>(),
                Collections.emptySet())).thenReturn(retDto);
        final List<SettingsPolicyApiDTO> result =
                mapper.convertSettingPolicies(Collections.singletonList(policy));
        assertThat(result, containsInAnyOrder(retDto));

        verify(groupBackend, never()).getGroups(any(), any());
    }

    @Test
    public void testConvertSettingPoliciesWithInvolvedGroups() {
        final SettingsManagerMapping mapping = mock(SettingsManagerMapping.class);
        final SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        final SettingPolicyMapper policyMapper = mock(SettingPolicyMapper.class);
        final SettingsMapper mapper = new SettingsMapper(mapping, settingStyleMapping, specMapper,
                policyMapper, grpcServer.getChannel());

        final long groupId = 7L;
        final String groupName = "krew";
        final SettingPolicy policy = SettingPolicy.newBuilder()
                .setSettingPolicyType(Type.USER)
                .setInfo(SettingPolicyInfo.newBuilder()
                    .setScope(Scope.newBuilder()
                        .addGroups(groupId)))
                .build();
        final Grouping group = Grouping.newBuilder()
                .setId(groupId)
                .setDefinition(
                        GroupDefinition.newBuilder()
                            .setType(GroupType.REGULAR)
                            .setDisplayName(groupName)
                                )
                .build();

        final SettingsPolicyApiDTO retDto = new SettingsPolicyApiDTO();
        when(policyMapper.convertSettingPolicy(policy, ImmutableMap.of(groupId, groupName),
                new HashMap<>(), Collections.emptySet())).thenReturn(retDto);
        when(groupBackend.getGroups(GetGroupsRequest.newBuilder()
                        .setGroupFilter(GroupFilter.newBuilder().addId(groupId)).build()))
            .thenReturn(Collections.singletonList(group));

        final List<SettingsPolicyApiDTO> result =
                mapper.convertSettingPolicies(Collections.singletonList(policy));
        assertThat(result, containsInAnyOrder(retDto));
        verify(groupBackend).getGroups(any(), any());
    }

    @Test
    public void testResolveEntityType() throws Exception {
        final SettingsMapper mapper =
                new SettingsMapper(settingMgrMapping, settingStyleMapping, settingSpecMapper,
                    policyMapper, grpcServer.getChannel());

        final int entityType = 1;
        final long groupId = 10;
        final GroupApiDTO groupDTO = new GroupApiDTO();
        groupDTO.setUuid(Long.toString(groupId));

        final Grouping group = Grouping.newBuilder()
                .setId(groupId)
                .addExpectedTypes(MemberType.newBuilder().setEntity(entityType))
                .setDefinition(GroupDefinition
                                .newBuilder()
                                .setStaticGroupMembers(StaticMembers.newBuilder()
                                    .addMembersByType(StaticMembersByType.newBuilder()
                                                .setType(MemberType.newBuilder().setEntity(entityType))
                                                )
                                    ))
                .build();

        final SettingsPolicyApiDTO apiDTO = new SettingsPolicyApiDTO();
        apiDTO.setScopes(Collections.singletonList(groupDTO));

        when(groupBackend.getGroups(GetGroupsRequest.newBuilder()
                       .setGroupFilter(GroupFilter.newBuilder()
                                       .addId(groupId))
                       .build()))
            .thenReturn(Collections.singletonList(group));

        assertThat(mapper.resolveEntityType(apiDTO), is(entityType));
    }

    @Test(expected = InvalidOperationException.class)
    public void testResolveEntityTypeNoScope1() throws Exception {
        final SettingsMapper mapper =
                new SettingsMapper(settingMgrMapping, settingStyleMapping, settingSpecMapper,
                    policyMapper, grpcServer.getChannel());
        mapper.resolveEntityType(new SettingsPolicyApiDTO());
    }

    @Test(expected = InvalidOperationException.class)
    public void testResolveEntityTypeNoScope2() throws Exception {
        final SettingsMapper mapper =
                new SettingsMapper(settingMgrMapping, settingStyleMapping, settingSpecMapper,
                    policyMapper, grpcServer.getChannel());
        final SettingsPolicyApiDTO dto = new SettingsPolicyApiDTO();
        dto.setScopes(Collections.emptyList());
        mapper.resolveEntityType(dto);
    }

    @Test(expected = InvalidOperationException.class)
    public void testResolveEntityTypeInvalidScope() throws Exception {
        final SettingsMapper mapper =
                new SettingsMapper(settingMgrMapping, settingStyleMapping, settingSpecMapper,
                    policyMapper, grpcServer.getChannel());
        final SettingsPolicyApiDTO dto = new SettingsPolicyApiDTO();
        dto.setScopes(Collections.singletonList(new GroupApiDTO()));
        mapper.resolveEntityType(dto);
    }

    @Test(expected = UnknownObjectException.class)
    public void testResolveEntityTypeGroupsNotFound() throws Exception {
        final SettingsMapper mapper =
                new SettingsMapper(settingMgrMapping, settingStyleMapping, settingSpecMapper,
                    policyMapper, grpcServer.getChannel());
        final long groupId = 10;
        final GroupApiDTO groupDTO = new GroupApiDTO();
        groupDTO.setUuid(Long.toString(groupId));

        final SettingsPolicyApiDTO apiDTO = new SettingsPolicyApiDTO();
        apiDTO.setScopes(Collections.singletonList(groupDTO));

        mapper.resolveEntityType(apiDTO);
    }

    @Test(expected = InvalidOperationException.class)
    public void testResolveEntityTypeMultipleTypes() throws Exception {
        final SettingsMapper mapper =
                new SettingsMapper(settingMgrMapping, settingStyleMapping, settingSpecMapper,
                    policyMapper, grpcServer.getChannel());
        final long groupId1 = 10;
        final GroupApiDTO groupDTO1 = new GroupApiDTO();
        groupDTO1.setUuid(Long.toString(groupId1));

        final long groupId2 = 11;
        final GroupApiDTO groupDTO2 = new GroupApiDTO();
        groupDTO2.setUuid(Long.toString(groupId2));

        final Grouping group1 = Grouping.newBuilder()
                        .setId(groupId1)
                        .addExpectedTypes(MemberType.newBuilder().setEntity(1))
                        .setDefinition(GroupDefinition
                                        .newBuilder()
                                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                            .addMembersByType(StaticMembersByType.newBuilder()
                                                        .setType(MemberType.newBuilder().setEntity(1))
                                                        )
                                            ))
                        .build();

        final Grouping group2 = Grouping.newBuilder()
                        .setId(groupId2)
                        .addExpectedTypes(MemberType.newBuilder().setEntity(2))
                        .setDefinition(GroupDefinition
                                        .newBuilder()
                                        .setStaticGroupMembers(StaticMembers.newBuilder()
                                            .addMembersByType(StaticMembersByType.newBuilder()
                                                        .setType(MemberType.newBuilder().setEntity(2))
                                                        )
                                            ))
                        .build();

        final SettingsPolicyApiDTO apiDTO = new SettingsPolicyApiDTO();
        apiDTO.setScopes(Arrays.asList(groupDTO1, groupDTO2));

        when(groupBackend.getGroups(any())).thenReturn(Arrays.asList(group1, group2));

        mapper.resolveEntityType(apiDTO);
    }

    @Test
    public void testCreateNewInputPolicy() throws Exception {
        final SettingsMapper mapper =
                spy(new SettingsMapper(settingMgrMapping, settingStyleMapping, settingSpecMapper,
                    policyMapper, grpcServer.getChannel()));

        final SettingsPolicyApiDTO dto = new SettingsPolicyApiDTO();
        doReturn(1).when(mapper).resolveEntityType(eq(dto));
        doReturn(SettingPolicyInfo.getDefaultInstance())
            .when(mapper).convertInputPolicy(eq(dto), eq(1));
        assertThat(mapper.convertNewInputPolicy(dto), is(SettingPolicyInfo.getDefaultInstance()));
    }

    @Test
    public void testCreateEditedInputPolicy() throws Exception {
        final SettingsMapper mapper =
                spy(new SettingsMapper(settingMgrMapping, settingStyleMapping, settingSpecMapper,
                    policyMapper, grpcServer.getChannel()));
        final SettingsPolicyApiDTO dto = new SettingsPolicyApiDTO();
        doReturn(SettingPolicyInfo.getDefaultInstance())
                .when(mapper).convertInputPolicy(eq(dto), eq(1));

        when(settingPolicyBackend.getSettingPolicy(any()))
            .thenReturn(GetSettingPolicyResponse.newBuilder()
                    .setSettingPolicy(SettingPolicy.newBuilder()
                            .setInfo(SettingPolicyInfo.newBuilder()
                                    .setEntityType(1)))
                    .build());

        assertThat(mapper.convertEditedInputPolicy(77, dto),
                is(SettingPolicyInfo.getDefaultInstance()));

    }

    @Test
    public void testCreateToProtoSettings() {
        final SettingsMapper mapper =
                new SettingsMapper(settingMgrMapping, settingStyleMapping, settingSpecMapper,
                    policyMapper, grpcServer.getChannel());
        final SettingApiDTO<String> setting = new SettingApiDTO<>();
        setting.setUuid("foo");
        setting.setValue("value");

        when(settingBackend.searchSettingSpecs(SearchSettingSpecsRequest.newBuilder()
                .addSettingSpecName("foo")
                .build()))
            .thenReturn(Collections.singletonList(SettingSpec.newBuilder()
                    .setName("foo")
                    .setStringSettingValueType(StringSettingValueType.getDefaultInstance())
                    .build()));

        final Map<SettingsMapper.SettingApiDtoKey, Setting> convertedSettings =
                mapper.toProtoSettings(Collections.singletonList(setting));

        SettingsMapper.SettingApiDtoKey key = SettingsMapper.getSettingApiDtoKey(setting);

        assertTrue(convertedSettings.containsKey(key));
        assertThat(convertedSettings.get(key), is(Setting.newBuilder()
                .setSettingSpecName("foo")
                .setStringSettingValue(StringSettingValue.newBuilder()
                        .setValue("value"))
                .build()));
    }

    @Test
    public void testTranslateDayOfWeekFromDTO() {
        final SettingsMapper mapper = setUpMapper();
        final List<DayOfWeek> dayOfWeeksApi = Lists.newArrayList(DayOfWeek.Mon, DayOfWeek.Tue,
                DayOfWeek.Wed, DayOfWeek.Thu, DayOfWeek.Fri, DayOfWeek.Sat, DayOfWeek.Sun);
        final List<Schedule.DayOfWeek> dayOfWeeks = Lists.newArrayList(Schedule.DayOfWeek.MONDAY,
                Schedule.DayOfWeek.TUESDAY, Schedule.DayOfWeek.WEDNESDAY, Schedule.DayOfWeek.THURSDAY,
                Schedule.DayOfWeek.FRIDAY, Schedule.DayOfWeek.SATURDAY, Schedule.DayOfWeek.SUNDAY);
        final List<Schedule.DayOfWeek> convertedDayOfWeeks = dayOfWeeksApi.stream()
                .map(mapper::translateDayOfWeekFromDTO)
                .collect(Collectors.toList());
        assertEquals(dayOfWeeks, convertedDayOfWeeks);
    }

    @Test
    public void testGetLegacyDayOfWeekForDatestamp() {
        final DefaultSettingPolicyMapper defaultSettingPolicyMapper = setUpPolicyMapperInfoToDtoTest();
        final List<Integer> weeks = Lists.newArrayList(Calendar.MONDAY,
                        Calendar.TUESDAY, Calendar.WEDNESDAY,
                        Calendar.THURSDAY, Calendar.FRIDAY,
                        Calendar.SATURDAY, Calendar.SUNDAY);
        final Date dateTime = new Date(startTimestamp);
        List<Date> dateTimeList = weeks.stream()
               .map(day -> updateDayOfWeek(dateTime, day))
               .collect(Collectors.toList());
        final List<DayOfWeek> dayOfWeeksApi = Lists.newArrayList(DayOfWeek.Mon, DayOfWeek.Tue,
                DayOfWeek.Wed, DayOfWeek.Thu, DayOfWeek.Fri, DayOfWeek.Sat, DayOfWeek.Sun);
        final List<DayOfWeek> convertedDayOfWeeks = dateTimeList.stream()
                .map(defaultSettingPolicyMapper::getLegacyDayOfWeekForDatestamp)
                .collect(Collectors.toList());
        assertEquals(dayOfWeeksApi, convertedDayOfWeeks);
    }

    private Date updateDayOfWeek(Date date, Integer dayOfWeek) {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.setTime(date);
        cal.set(Calendar.DAY_OF_WEEK, dayOfWeek);
        return cal.getTime();
    }
}
