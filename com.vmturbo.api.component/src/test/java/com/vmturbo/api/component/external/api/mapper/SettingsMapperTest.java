package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.component.external.api.mapper.SettingsMapper.DefaultSettingSpecMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.SettingManagerInfo;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.SettingManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.SettingSpecMapper;
import com.vmturbo.api.dto.GroupApiDTO;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.dto.setting.SettingsPolicyApiDTO;
import com.vmturbo.api.enums.InputValueType;
import com.vmturbo.api.enums.SettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.AllEntityType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.GlobalSettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath.SettingCategoryPathNode;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class SettingsMapperTest {
    private final String mgrId1 = "mgr1";
    private final String mgrName1 = "Manager1";
    private final String mgrCategory1 = "Category1";

    private final String mgrId2 = "mgr2";
    private final String mgrName2 = "Manager2";
    private final String mgrCategory2 = "Category2";

    private final SettingSpec settingSpec1 = SettingSpec.newBuilder()
            .setName("moveVM")
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

    private final SettingSpec settingSpec3 = SettingSpec.newBuilder(settingSpec1)
            .setName("thirdMoveVM")
            .build();

    private final SettingManagerInfo mgr1Info = new SettingManagerInfo(mgrName1, mgrCategory1,
                Collections.singleton(settingSpec1.getName()));

    /**
     * Verify that a manager loaded from JSON file gets used to map
     * a setting spec as expected.
     */
    @Test
    public void testLoadEndToEnd() {
        SettingsMapper mapper = new SettingsMapper("settingManagersTest.json");

        final List<SettingsManagerApiDTO> ret =
                mapper.toManagerDtos(Collections.singletonList(settingSpec1));
        assertEquals(1, ret.size());

        final SettingsManagerApiDTO mgr = ret.get(0);
        assertEquals("automationmanager", mgr.getUuid());
        assertEquals("Action Mode Settings", mgr.getDisplayName());
        assertEquals("Automation", mgr.getCategory());
        assertEquals(1, mgr.getSettings().size());

        final SettingApiDTO settingApiDTO = mgr.getSettings().get(0);
        assertEquals("moveVM", settingApiDTO.getUuid());
        assertEquals("Move", settingApiDTO.getDisplayName());
        assertEquals("MANUAL", settingApiDTO.getDefaultValue());
        assertEquals(SettingScope.LOCAL, settingApiDTO.getScope());

        assertEquals("VirtualMachine", settingApiDTO.getEntityType());
        assertEquals(InputValueType.STRING, settingApiDTO.getValueType());

        assertEquals(2, settingApiDTO.getOptions().size());
        // The order is important
        assertEquals("DISABLED", settingApiDTO.getOptions().get(0).getLabel());
        assertEquals("DISABLED", settingApiDTO.getOptions().get(0).getValue());
        assertEquals("MANUAL", settingApiDTO.getOptions().get(1).getLabel());
        assertEquals("MANUAL", settingApiDTO.getOptions().get(1).getValue());
    }

    @Test
    public void testToMgrDto() {
        SettingManagerMapping mapping = mock(SettingManagerMapping.class);
        SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        when(mapping.getManagerInfo(mgrId1))
            .thenReturn(Optional.of(new SettingManagerInfo(mgrName1, mgrCategory1,
                    Collections.singleton(settingSpec1.getName()))));
        when(mapping.getManagerUuid(settingSpec1.getName())).thenReturn(Optional.of(mgrId1));

        SettingApiDTO settingApiDTO = new SettingApiDTO();
        settingApiDTO.setDisplayName("mockSetting");
        when(specMapper.settingSpecToApi(settingSpec1)).thenReturn(Optional.of(settingApiDTO));

        SettingsMapper mapper = new SettingsMapper(mapping,  specMapper);
        Optional<SettingsManagerApiDTO> mgrDtoOpt =
                mapper.toManagerDto(Collections.singletonList(settingSpec1), mgrId1);
        assertTrue(mgrDtoOpt.isPresent());

        SettingsManagerApiDTO mgrDto = mgrDtoOpt.get();
        assertEquals(mgrName1, mgrDto.getDisplayName());
        assertEquals(mgrCategory1, mgrDto.getCategory());
        assertEquals(1, mgrDto.getSettings().size());
        assertEquals("mockSetting", mgrDto.getSettings().get(0).getDisplayName());
    }

    @Test
    public void testMgrDtoNoMgr() {
        SettingManagerMapping mapping = mock(SettingManagerMapping.class);
        SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        when(mapping.getManagerInfo(mgrId1)).thenReturn(Optional.empty());
        when(mapping.getManagerUuid(any())).thenReturn(Optional.empty());
        SettingsMapper mapper = new SettingsMapper(mapping, specMapper);
        assertFalse(mapper.toManagerDto(Collections.singleton(settingSpec1), mgrId1).isPresent());
    }

    @Test
    public void testMgrDtoNoSetting() {
        SettingManagerMapping mapping = mock(SettingManagerMapping.class);
        SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        when(mapping.getManagerInfo(mgrId1))
                .thenReturn(Optional.of(new SettingManagerInfo(mgrName1, mgrCategory1,
                        Collections.emptySet())));
        when(mapping.getManagerUuid(settingSpec1.getName())).thenReturn(Optional.empty());
        when(mapping.getManagerUuid(settingSpec2.getName())).thenReturn(Optional.of(mgrId1 + "suffix"));

        SettingsMapper mapper = new SettingsMapper(mapping,  specMapper);
        Optional<SettingsManagerApiDTO> mgrDtoOpt =
                mapper.toManagerDto(Arrays.asList(settingSpec1, settingSpec2), mgrId1);
        assertTrue(mgrDtoOpt.isPresent());

        SettingsManagerApiDTO mgrDto = mgrDtoOpt.get();
        assertEquals(mgrName1, mgrDto.getDisplayName());
        assertEquals(mgrCategory1, mgrDto.getCategory());
        assertEquals(0, mgrDto.getSettings().size());
    }

    @Test
    public void testMgrDtos() {
        final SettingManagerMapping mapping = mock(SettingManagerMapping.class);
        final SettingSpecMapper specMapper = mock(SettingSpecMapper.class);

        final SettingApiDTO settingApiDTO1 = new SettingApiDTO();
        settingApiDTO1.setDisplayName("mockSetting1");
        final SettingApiDTO settingApiDTO2 = new SettingApiDTO();
        settingApiDTO2.setDisplayName("mockSetting2");

        when(mapping.getManagerUuid(settingSpec1.getName())).thenReturn(Optional.of(mgrId1));
        when(mapping.getManagerUuid(settingSpec2.getName())).thenReturn(Optional.of(mgrId2));
        when(mapping.getManagerInfo(mgrId1))
                .thenReturn(Optional.of(new SettingManagerInfo(mgrName1, mgrCategory1,
                        Collections.singleton(settingSpec1.getName()))));
        when(mapping.getManagerInfo(mgrId2))
                .thenReturn(Optional.of(new SettingManagerInfo(mgrName2, mgrCategory2,
                        Collections.singleton(settingSpec2.getName()))));
        when(specMapper.settingSpecToApi(settingSpec1)).thenReturn(Optional.of(settingApiDTO1));
        when(specMapper.settingSpecToApi(settingSpec2)).thenReturn(Optional.of(settingApiDTO2));

        final SettingsMapper mapper = new SettingsMapper(mapping,  specMapper);
        final Map<String, SettingsManagerApiDTO> results = mapper.toManagerDtos(
                Arrays.asList(settingSpec1, settingSpec2)).stream()
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
        final SettingManagerMapping mapping = mock(SettingManagerMapping.class);
        final SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        final SettingsMapper mapper = new SettingsMapper(mapping,  specMapper);
        when(mapping.getManagerUuid(settingSpec1.getName())).thenReturn(Optional.empty());
        assertTrue(mapper.toManagerDtos(Collections.singleton(settingSpec1)).isEmpty());
    }

    @Test
    public void testSpecMapperSinglePath() {
        final DefaultSettingSpecMapper specMapper = new DefaultSettingSpecMapper();
        final SettingSpec singlePathSpec = SettingSpec.newBuilder(settingSpec1)
                .setPath(SettingCategoryPath.newBuilder()
                        .setRootPathNode(SettingCategoryPathNode.newBuilder()
                                .setNodeName("node1")))
                .build();

        final SettingApiDTO dto = specMapper.settingSpecToApi(singlePathSpec).get();
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

        final SettingApiDTO dto = specMapper.settingSpecToApi(doublePathSpec).get();
        assertEquals(Arrays.asList("node1", "node2"), dto.getCategories());
    }

    @Test
    public void testSpecMapperBoolean() {
        final DefaultSettingSpecMapper specMapper = new DefaultSettingSpecMapper();
        final SettingSpec boolSpec = SettingSpec.newBuilder(settingSpec1)
                .setBooleanSettingValueType(BooleanSettingValueType.newBuilder()
                        .setDefault(true))
                .build();
        final SettingApiDTO dto = specMapper.settingSpecToApi(boolSpec).get();
        assertEquals("true", dto.getDefaultValue());
        assertEquals(InputValueType.BOOLEAN, dto.getValueType());
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
        final SettingApiDTO dto = specMapper.settingSpecToApi(numSpec).get();
        assertEquals("10.5", dto.getDefaultValue());
        assertEquals(Double.valueOf(1.7f), dto.getMin());
        assertEquals(Double.valueOf(100), dto.getMax());
        assertEquals(InputValueType.NUMERIC, dto.getValueType());
    }

    @Test
    public void testSpecMapperString() {
        final DefaultSettingSpecMapper specMapper = new DefaultSettingSpecMapper();
        final SettingSpec stringSpec = SettingSpec.newBuilder(settingSpec1)
                .setStringSettingValueType(StringSettingValueType.newBuilder()
                        .setDefault("BOO!")
                        .setValidationRegex(".*"))
                .build();
        final SettingApiDTO dto = specMapper.settingSpecToApi(stringSpec).get();
        assertEquals("BOO!", dto.getDefaultValue());
        assertEquals(InputValueType.STRING, dto.getValueType());
    }

    @Test
    public void testSpecMapperGlobalScope() {
        final DefaultSettingSpecMapper specMapper = new DefaultSettingSpecMapper();
        final SettingSpec stringSpec = SettingSpec.newBuilder(settingSpec1)
                .setGlobalSettingSpec(GlobalSettingSpec.getDefaultInstance())
                .build();
        final SettingApiDTO dto = specMapper.settingSpecToApi(stringSpec).get();
        assertEquals(SettingScope.GLOBAL, dto.getScope());
    }

    @Test
    public void testSpecMapperMultiSupportedTypes() {
        final DefaultSettingSpecMapper specMapper = new DefaultSettingSpecMapper();
        final SettingSpec stringSpec = SettingSpec.newBuilder(settingSpec1)
                .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                    .setEntitySettingScope(EntitySettingScope.newBuilder()
                        .setEntityTypeSet(EntityTypeSet.newBuilder()
                            .addEntityType(10)
                            .addEntityType(20))))
                .build();
        final SettingApiDTO dto = specMapper.settingSpecToApi(stringSpec).get();
        // The first entity type should be the one set in the returned DTO.
        assertEquals(ServiceEntityMapper.toUIEntityType(10), dto.getEntityType());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapInputPolicyDefault() {
        final SettingManagerMapping mapping = mock(SettingManagerMapping.class);
        final SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        final SettingsMapper mapper = new SettingsMapper(mapping,  specMapper);

        final SettingsPolicyApiDTO settingsPolicyApiDTO = new SettingsPolicyApiDTO();
        settingsPolicyApiDTO.setDisplayName("Test");
        settingsPolicyApiDTO.setDefault(true);
        settingsPolicyApiDTO.setEntityType(
                ServiceEntityMapper.toUIEntityType(EntityType.VIRTUAL_MACHINE.getNumber()));

        SettingPolicyInfo info =
                mapper.convertInputPolicy(settingsPolicyApiDTO, Collections.emptyMap());
    }

    @Test
    public void testMapInputPolicy() {
        final SettingManagerMapping mapping = mock(SettingManagerMapping.class);
        final SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        final SettingsMapper mapper = new SettingsMapper(mapping,  specMapper);

        final SettingsPolicyApiDTO settingsPolicyApiDTO = new SettingsPolicyApiDTO();

        final SettingApiDTO boolSetting = new SettingApiDTO();
        boolSetting.setUuid("bool setting");
        boolSetting.setValue("true");
        final Setting boolSettingProto = Setting.newBuilder()
                .setSettingSpecName(boolSetting.getUuid())
                .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(true))
                .build();

        final SettingApiDTO numSetting = new SettingApiDTO();
        numSetting.setUuid("num setting");
        numSetting.setValue("10");
        final Setting numSettingProto = Setting.newBuilder()
                .setSettingSpecName(numSetting.getUuid())
                .setNumericSettingValue(NumericSettingValue.newBuilder().setValue(10))
                .build();

        final SettingApiDTO stringSetting = new SettingApiDTO();
        stringSetting.setUuid("string setting");
        stringSetting.setValue("foo");
        final Setting strSettingProto = Setting.newBuilder()
                .setSettingSpecName(stringSetting.getUuid())
                .setStringSettingValue(StringSettingValue.newBuilder().setValue("foo"))
                .build();

        final SettingApiDTO enumSetting = new SettingApiDTO();
        enumSetting.setUuid("enum setting");
        enumSetting.setValue("VAL");
        final Setting enumSettingProto = Setting.newBuilder()
                .setSettingSpecName(enumSetting.getUuid())
                .setEnumSettingValue(EnumSettingValue.newBuilder().setValue("VAL"))
                .build();

        final Map<String, SettingSpec> specMap = ImmutableMap.<String, SettingSpec>builder()
            .put(boolSetting.getUuid(), SettingSpec.newBuilder()
                .setBooleanSettingValueType(BooleanSettingValueType.getDefaultInstance()).build())
            .put(numSetting.getUuid(), SettingSpec.newBuilder()
                .setNumericSettingValueType(NumericSettingValueType.getDefaultInstance()).build())
            .put(stringSetting.getUuid(), SettingSpec.newBuilder()
                .setStringSettingValueType(StringSettingValueType.getDefaultInstance()).build())
            .put(enumSetting.getUuid(), SettingSpec.newBuilder()
                .setEnumSettingValueType(EnumSettingValueType.getDefaultInstance()).build())
            .build();

        final SettingsManagerApiDTO settingMgr1 = new SettingsManagerApiDTO();
        settingMgr1.setSettings(Arrays.asList(boolSetting, numSetting));
        final SettingsManagerApiDTO settingMgr2 = new SettingsManagerApiDTO();
        settingMgr2.setSettings(Arrays.asList(stringSetting, enumSetting));

        settingsPolicyApiDTO.setSettingsManagers(Arrays.asList(settingMgr1, settingMgr2));
        settingsPolicyApiDTO.setDefault(false);

        final GroupApiDTO group = new GroupApiDTO();
        group.setUuid("7");
        settingsPolicyApiDTO.setScopes(Collections.singletonList(group));

        settingsPolicyApiDTO.setDisplayName("A Setting Policy is neither a Setting, not a Policy." +
                " Like a pineapple.");
        settingsPolicyApiDTO.setEntityType(ServiceEntityMapper.toUIEntityType(
                EntityType.VIRTUAL_MACHINE.getNumber()));
        settingsPolicyApiDTO.setDisabled(false);

        final SettingPolicyInfo info = mapper.convertInputPolicy(settingsPolicyApiDTO, specMap);
        assertEquals(settingsPolicyApiDTO.getDisplayName(), info.getName());
        assertEquals(EntityType.VIRTUAL_MACHINE.getNumber(), info.getEntityType());
        assertEquals(true, info.getEnabled());
        assertTrue(info.hasScope());
        assertThat(info.getScope().getGroupsList(), containsInAnyOrder(7L));
        assertThat(info.getSettingsList(),
                containsInAnyOrder(boolSettingProto, numSettingProto,
                        strSettingProto, enumSettingProto));
    }

    @Test
    public void testMapPolicyInfoToApiDto() {
        final SettingManagerMapping mapping = mock(SettingManagerMapping.class);
        final SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        final SettingsMapper mapper = new SettingsMapper(mapping,  specMapper);

        final long groupId = 7;
        final String groupName = "goat";

        final SettingPolicy settingPolicy = SettingPolicy.newBuilder()
                .setId(1)
                .setSettingPolicyType(Type.USER)
                .setInfo(SettingPolicyInfo.newBuilder()
                    .setName("foo")
                    .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                    .setEnabled(true)
                    .setScope(Scope.newBuilder()
                            .addGroups(7L))
                    .addSettings(Setting.newBuilder()
                            .setSettingSpecName(settingSpec1.getName())
                            .setEnumSettingValue(EnumSettingValue.newBuilder()
                                    .setValue("AUTOMATIC"))))
                .build();

        when(mapping.getManagerUuid(eq(settingSpec1.getName()))).thenReturn(Optional.of(mgrId1));
        when(mapping.getManagerInfo(mgrId1))
                .thenReturn(Optional.of(mgr1Info));

        final SettingsPolicyApiDTO retDto =
                mapper.convertSettingsPolicy(settingPolicy, ImmutableMap.of(groupId, groupName));
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
        assertEquals(1, mgr.getSettings().size());
        final SettingApiDTO setting = mgr.getSettings().get(0);
        assertEquals(settingSpec1.getName(), setting.getUuid());
        assertEquals("AUTOMATIC", setting.getValue());
    }

    @Test
    public void testValMgrDtoEnum() {
        final SettingManagerMapping mapping = mock(SettingManagerMapping.class);
        final SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        final SettingsMapper mapper = new SettingsMapper(mapping,  specMapper);

        final SettingsManagerApiDTO mgr =
            mapper.createValMgrDto(mgrId1, mgr1Info, Collections.singletonList(Setting.newBuilder()
                .setSettingSpecName(settingSpec1.getName())
                .setEnumSettingValue(EnumSettingValue.newBuilder()
                        .setValue("AUTOMATIC"))
                .build()));
        assertEquals(mgrId1, mgr.getUuid());
        assertEquals(mgrName1, mgr.getDisplayName());
        assertEquals(mgrCategory1, mgr.getCategory());
        assertEquals(1, mgr.getSettings().size());
        final SettingApiDTO setting = mgr.getSettings().get(0);
        assertEquals(settingSpec1.getName(), setting.getUuid());
        assertEquals("AUTOMATIC", setting.getValue());
    }

    @Test
    public void testValMgrDtoBool() {
        final SettingManagerMapping mapping = mock(SettingManagerMapping.class);
        final SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        final SettingsMapper mapper = new SettingsMapper(mapping,  specMapper);

        final SettingsManagerApiDTO mgr =
                mapper.createValMgrDto(mgrId1, mgr1Info, Collections.singletonList(Setting.newBuilder()
                        .setSettingSpecName("setting")
                        .setBooleanSettingValue(BooleanSettingValue.newBuilder()
                                .setValue(true))
                        .build()));
        assertEquals(mgrId1, mgr.getUuid());
        assertEquals(mgrName1, mgr.getDisplayName());
        assertEquals(mgrCategory1, mgr.getCategory());
        assertEquals(1, mgr.getSettings().size());
        final SettingApiDTO setting = mgr.getSettings().get(0);
        assertEquals("setting", setting.getUuid());
        assertEquals("true", setting.getValue());
    }

    @Test
    public void testValMgrDtoNum() {
        final SettingManagerMapping mapping = mock(SettingManagerMapping.class);
        final SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        final SettingsMapper mapper = new SettingsMapper(mapping,  specMapper);

        final SettingsManagerApiDTO mgr =
                mapper.createValMgrDto(mgrId1, mgr1Info, Collections.singletonList(Setting.newBuilder()
                        .setSettingSpecName("setting")
                        .setNumericSettingValue(NumericSettingValue.newBuilder()
                                .setValue(2.7f))
                        .build()));
        assertEquals(mgrId1, mgr.getUuid());
        assertEquals(mgrName1, mgr.getDisplayName());
        assertEquals(mgrCategory1, mgr.getCategory());
        assertEquals(1, mgr.getSettings().size());
        final SettingApiDTO setting = mgr.getSettings().get(0);
        assertEquals("setting", setting.getUuid());
        assertEquals("2.7", setting.getValue());
    }

    @Test
    public void testValMgrDtoStr() {
        final SettingManagerMapping mapping = mock(SettingManagerMapping.class);
        final SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        final SettingsMapper mapper = new SettingsMapper(mapping,  specMapper);

        final SettingsManagerApiDTO mgr =
                mapper.createValMgrDto(mgrId1, mgr1Info, Collections.singletonList(Setting.newBuilder()
                        .setSettingSpecName("setting")
                        .setStringSettingValue(StringSettingValue.newBuilder()
                                .setValue("foo"))
                        .build()));
        assertEquals(mgrId1, mgr.getUuid());
        assertEquals(mgrName1, mgr.getDisplayName());
        assertEquals(mgrCategory1, mgr.getCategory());
        assertEquals(1, mgr.getSettings().size());
        final SettingApiDTO setting = mgr.getSettings().get(0);
        assertEquals("setting", setting.getUuid());
        assertEquals("foo", setting.getValue());
    }

    @Test
    public void testInferEntityTypeSuccess() {
        final SettingManagerMapping mapping = mock(SettingManagerMapping.class);
        final SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        final SettingsMapper mapper = new SettingsMapper(mapping,  specMapper);
        final Map<String, SettingSpec> specMap = ImmutableMap.of("setting", SettingSpec.newBuilder()
                .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                        .setEntitySettingScope(EntitySettingScope.newBuilder()
                                .setEntityTypeSet(EntityTypeSet.newBuilder()
                                        .addEntityType(EntityType.VIRTUAL_MACHINE.getNumber()))))
                .build());
        assertEquals(EntityType.VIRTUAL_MACHINE.getNumber(), mapper.inferEntityType(specMap));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInferEntityTypeTwoTypes() {
        final SettingManagerMapping mapping = mock(SettingManagerMapping.class);
        final SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        final SettingsMapper mapper = new SettingsMapper(mapping,  specMapper);
        final Map<String, SettingSpec> specMap = ImmutableMap.of("setting", SettingSpec.newBuilder()
            .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                .setEntitySettingScope(EntitySettingScope.newBuilder()
                    .setEntityTypeSet(EntityTypeSet.newBuilder()
                        .addEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
                        .addEntityType(EntityType.STORAGE.getNumber()))))
            .build());
        assertEquals(EntityType.VIRTUAL_MACHINE.getNumber(), mapper.inferEntityType(specMap));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInferEntityTypeNoType() {
        final SettingManagerMapping mapping = mock(SettingManagerMapping.class);
        final SettingSpecMapper specMapper = mock(SettingSpecMapper.class);
        final SettingsMapper mapper = new SettingsMapper(mapping,  specMapper);
        final Map<String, SettingSpec> specMap = ImmutableMap.of("setting", SettingSpec.newBuilder()
            .setEntitySettingSpec(EntitySettingSpec.newBuilder()
                .setEntitySettingScope(EntitySettingScope.newBuilder()
                    .setAllEntityType(AllEntityType.getDefaultInstance())))
            .build());
        assertEquals(EntityType.VIRTUAL_MACHINE.getNumber(), mapper.inferEntityType(specMap));
    }
}
