package com.vmturbo.api.component.external.api.mapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
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

import com.vmturbo.api.component.external.api.mapper.SettingsMapper.DefaultSettingSpecMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.SettingManagerInfo;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.SettingManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.SettingSpecMapper;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.enums.InputValueType;
import com.vmturbo.api.enums.SettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingScope.EntityTypeSet;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.GlobalSettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.NumericSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingCategoryPath.SettingCategoryPathNode;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingTiebreaker;
import com.vmturbo.common.protobuf.setting.SettingProto.StringSettingValueType;

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
        when(mapping.getManagerUuid(settingSpec1)).thenReturn(Optional.of(mgrId1));

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
        when(mapping.getManagerUuid(settingSpec1)).thenReturn(Optional.empty());
        when(mapping.getManagerUuid(settingSpec2)).thenReturn(Optional.of(mgrId1 + "suffix"));

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

        when(mapping.getManagerUuid(settingSpec1)).thenReturn(Optional.of(mgrId1));
        when(mapping.getManagerUuid(settingSpec2)).thenReturn(Optional.of(mgrId2));
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
        when(mapping.getManagerUuid(settingSpec1)).thenReturn(Optional.empty());
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
}
