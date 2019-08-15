package com.vmturbo.api.component.external.api.util.setting;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerInfo;
import com.vmturbo.api.component.external.api.mapper.SettingsManagerMappingLoader.SettingsManagerMapping;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper.SettingApiDTOPossibilities;
import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.setting.EntitySettingQueryExecutor.EntitySettingGroupMapper;
import com.vmturbo.api.dto.setting.SettingActivePolicyApiDTO;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup.SettingPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.SearchSettingSpecsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;

public class EntitySettingQueryExecutorTest {

    private GroupExpander groupExpander = mock(GroupExpander.class);
    private SettingsManagerMapping settingsManagerMapping = mock(SettingsManagerMapping.class);

    private SettingServiceMole settingServiceBackend = spy(SettingServiceMole.class);
    private SettingPolicyServiceMole settingPolicyServiceBackend = spy(SettingPolicyServiceMole.class);

    @Rule
    public GrpcTestServer grpcTestServer =
        GrpcTestServer.newServer(settingPolicyServiceBackend, settingServiceBackend);

    private EntitySettingGroupMapper entitySettingGroupMapper = mock(EntitySettingGroupMapper.class);

    @Nonnull
    private EntitySettingQueryExecutor newExecutor() {
        return new EntitySettingQueryExecutor(SettingPolicyServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            SettingServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            groupExpander, entitySettingGroupMapper, settingsManagerMapping);
    }

    @Test
    public void testGetSettingsIndividualEntity() {
        // ARRANGE
        final EntitySettingQueryExecutor executor = newExecutor();
        final ApiId scope = mock(ApiId.class);
        final long scopeId = 7;
        final UIEntityType scopeType = UIEntityType.VIRTUAL_MACHINE;
        when(scope.isGroup()).thenReturn(false);
        when(scope.oid()).thenReturn(scopeId);
        when(scope.getScopeType()).thenReturn(Optional.of(scopeType));

        // ACT
        final List<SettingsManagerApiDTO> retMgrs = executor.getEntitySettings(scope, false);

        // ASSERT
        verify(settingPolicyServiceBackend).getEntitySettings(GetEntitySettingsRequest.newBuilder()
            .setSettingFilter(EntitySettingFilter.newBuilder()
                .addEntities(7L))
            .setIncludeSettingPolicies(false)
            .build());
        verify(settingServiceBackend).searchSettingSpecs(SearchSettingSpecsRequest.newBuilder()
            .addSettingSpecName(SPEC_NAME)
            .build());
        verify(settingsManagerMapping).getManagerUuid(SPEC_NAME);
        verify(entitySettingGroupMapper).toSettingApiDto(Collections.singletonList(SETTING_GROUP),
            FOO_SETTING_SPEC, Optional.of(scopeType), false);
        verify(settingsManagerMapping).getManagerInfo(MGR_UUID);
        assertThat(retMgrs, containsInAnyOrder(MAPPED_MGR_DTO));
    }

    private static final String SPEC_NAME = "foo";

    private static final String MGR_UUID = "fooMgr";

    private static final SettingSpec FOO_SETTING_SPEC = SettingSpec.newBuilder()
        .setName(SPEC_NAME)
        .build();

    private static final EntitySettingGroup SETTING_GROUP = EntitySettingGroup.newBuilder()
        .setSetting(Setting.newBuilder()
            .setSettingSpecName(SPEC_NAME))
        // Some random id
        .addEntityOids(123)
        .build();

    private static final SettingsManagerApiDTO MAPPED_MGR_DTO = new SettingsManagerApiDTO();

    @Before
    public void setup() {
        doReturn(Collections.singletonList(FOO_SETTING_SPEC)).when(settingServiceBackend).searchSettingSpecs(any());

        when(settingsManagerMapping.getManagerUuid(SPEC_NAME)).thenReturn(Optional.of(MGR_UUID));

        SettingApiDTO mappedSetting = new SettingApiDTO();
        mappedSetting.setDisplayName(SPEC_NAME);
        when(entitySettingGroupMapper.toSettingApiDto(any(), any(), any(), anyBoolean())).thenReturn(Optional.of(mappedSetting));

        MAPPED_MGR_DTO.setUuid(MGR_UUID);
        final SettingsManagerInfo mgrInfo = mock(SettingsManagerInfo.class);
        when(mgrInfo.newApiDTO(MGR_UUID)).thenReturn(MAPPED_MGR_DTO);

        when(settingsManagerMapping.getManagerInfo(MGR_UUID)).thenReturn(Optional.of(mgrInfo));

        doReturn(Collections.singletonList(GetEntitySettingsResponse.newBuilder()
            .addSettingGroup(SETTING_GROUP)
            .build())).when(settingPolicyServiceBackend).getEntitySettings(any());
    }

    @Test
    public void testGetSettingsGroup() {
        // ARRANGE
        final EntitySettingQueryExecutor executor = newExecutor();
        final ApiId scope = mock(ApiId.class);
        final long scopeId = 7;
        final long groupMember = 77;
        final UIEntityType scopeType = UIEntityType.VIRTUAL_MACHINE;
        when(scope.isGroup()).thenReturn(true);
        when(scope.oid()).thenReturn(scopeId);
        when(scope.getScopeType()).thenReturn(Optional.of(scopeType));

        when(groupExpander.expandOids(any())).thenReturn(Collections.singleton(groupMember));

        // ACT
        final List<SettingsManagerApiDTO> retMgrs = executor.getEntitySettings(scope, false);

        // ASSERT
        verify(settingPolicyServiceBackend).getEntitySettings(GetEntitySettingsRequest.newBuilder()
            .setSettingFilter(EntitySettingFilter.newBuilder()
                // Should put the GROUP MEMBERS in the filter, not the group ID.
                .addEntities(groupMember))
            .setIncludeSettingPolicies(false)
            .build());
        verify(settingServiceBackend).searchSettingSpecs(SearchSettingSpecsRequest.newBuilder()
            .addSettingSpecName(SPEC_NAME)
            .build());
        verify(settingsManagerMapping).getManagerUuid(SPEC_NAME);
        verify(entitySettingGroupMapper).toSettingApiDto(Collections.singletonList(SETTING_GROUP),
            FOO_SETTING_SPEC, Optional.of(scopeType), false);
        verify(settingsManagerMapping).getManagerInfo(MGR_UUID);
        assertThat(retMgrs, containsInAnyOrder(MAPPED_MGR_DTO));
    }

    @Test
    public void testDominantGroupNoPolicyBreakdown() {
        final SettingsMapper settingsMapper = mock(SettingsMapper.class);
        final EntitySettingGroupMapper groupMapper = new EntitySettingGroupMapper(settingsMapper);

        final EntitySettingGroup smallerGroup = EntitySettingGroup.newBuilder()
            .setSetting(Setting.newBuilder()
                .setSettingSpecName(FOO_SETTING_SPEC.getName())
                .setBooleanSettingValue(BooleanSettingValue.newBuilder()
                    .setValue(true)))
            .addEntityOids(1L)
            .build();

        final EntitySettingGroup dominantGroup = EntitySettingGroup.newBuilder()
            .setSetting(Setting.newBuilder()
                .setSettingSpecName(FOO_SETTING_SPEC.getName())
                .setBooleanSettingValue(BooleanSettingValue.newBuilder()
                    // Value is different
                    .setValue(false)))
            // Two OIDs makes this the dominant group
            .addEntityOids(2L)
            .addEntityOids(3L)
            .build();

        final SettingApiDTOPossibilities smallerPossibilities = mock(SettingApiDTOPossibilities.class);
        final SettingApiDTO smallerSetting = new SettingApiDTO();
        when(smallerPossibilities.getSettingForEntityType(UIEntityType.VIRTUAL_MACHINE.apiStr()))
            .thenReturn(Optional.of(smallerSetting));
        when(settingsMapper.toSettingApiDto(smallerGroup.getSetting(), FOO_SETTING_SPEC))
            .thenReturn(smallerPossibilities);

        final SettingApiDTOPossibilities dominantPossibilities = mock(SettingApiDTOPossibilities.class);
        final SettingApiDTO dominantSetting = new SettingApiDTO();
        when(dominantPossibilities.getSettingForEntityType(UIEntityType.VIRTUAL_MACHINE.apiStr()))
            .thenReturn(Optional.of(dominantSetting));
        when(settingsMapper.toSettingApiDto(dominantGroup.getSetting(), FOO_SETTING_SPEC))
            .thenReturn(dominantPossibilities);

        final Optional<SettingApiDTO<String>> result = groupMapper.toSettingApiDto(
            Arrays.asList(smallerGroup, dominantGroup),
            FOO_SETTING_SPEC,
            Optional.of(UIEntityType.VIRTUAL_MACHINE), false);

        // Relying on object equality.
        assertThat(result.get(), is(dominantSetting));
    }

    @Test
    public void testEntityGroupMappingPolicyBreakdown() {
        final SettingsMapper settingsMapper = mock(SettingsMapper.class);
        final EntitySettingGroupMapper groupMapper = new EntitySettingGroupMapper(settingsMapper);

        final EntitySettingGroup smallerGroup = EntitySettingGroup.newBuilder()
            .setSetting(Setting.newBuilder()
                .setSettingSpecName(FOO_SETTING_SPEC.getName())
                .setBooleanSettingValue(BooleanSettingValue.newBuilder()
                    .setValue(true)))
            .setPolicyId(SettingPolicyId.newBuilder()
                .setPolicyId(5L)
                .setType(Type.USER)
                .setDisplayName("smallerPolicy"))
            .addEntityOids(1L)
            .build();

        final EntitySettingGroup dominantGroup = EntitySettingGroup.newBuilder()
            .setSetting(Setting.newBuilder()
                .setSettingSpecName(FOO_SETTING_SPEC.getName())
                .setBooleanSettingValue(BooleanSettingValue.newBuilder()
                    // Value is different
                    .setValue(false)))
            .setPolicyId(SettingPolicyId.newBuilder()
                .setPolicyId(55L)
                .setType(Type.USER)
                .setDisplayName("dominantPolicy"))
            // Two OIDs makes this the dominant group
            .addEntityOids(2L)
            .addEntityOids(3L)
            .build();

        final SettingApiDTOPossibilities smallerPossibilities = mock(SettingApiDTOPossibilities.class);
        final SettingApiDTO smallerSetting = new SettingApiDTO();
        smallerSetting.setValue("true");
        when(smallerPossibilities.getSettingForEntityType(UIEntityType.VIRTUAL_MACHINE.apiStr()))
            .thenReturn(Optional.of(smallerSetting));
        when(settingsMapper.toSettingApiDto(smallerGroup.getSetting(), FOO_SETTING_SPEC))
            .thenReturn(smallerPossibilities);

        final SettingApiDTOPossibilities dominantPossibilities = mock(SettingApiDTOPossibilities.class);
        final SettingApiDTO dominantSetting = new SettingApiDTO();
        dominantSetting.setValue("false");
        when(dominantPossibilities.getSettingForEntityType(UIEntityType.VIRTUAL_MACHINE.apiStr()))
            .thenReturn(Optional.of(dominantSetting));
        when(settingsMapper.toSettingApiDto(dominantGroup.getSetting(), FOO_SETTING_SPEC))
            .thenReturn(dominantPossibilities);

        final Optional<SettingApiDTO<String>> resultOpt = groupMapper.toSettingApiDto(
            Arrays.asList(smallerGroup, dominantGroup),
            FOO_SETTING_SPEC,
            Optional.of(UIEntityType.VIRTUAL_MACHINE), true);

        // Relying on object equality - the "base" should still be the dominant setting.
        final SettingApiDTO<String> result = resultOpt.get();
        assertThat(result, is(dominantSetting));
        // The rest of SettingApiDTO properties should be set by the settings mapper, so we don't
        // check them.

        assertThat(result.getSourceGroupName(), is(dominantGroup.getPolicyId().getDisplayName()));
        assertThat(result.getSourceGroupUuid(),
            is(Long.toString(dominantGroup.getPolicyId().getPolicyId())));
        final Map<String, SettingActivePolicyApiDTO> activePoliciesByName =
            result.getActiveSettingsPolicies().stream()
                .collect(Collectors.toMap(
                    policy -> policy.getSettingsPolicy().getDisplayName(),
                    Function.identity()));
        assertThat(activePoliciesByName.keySet(),
            containsInAnyOrder(dominantGroup.getPolicyId().getDisplayName(),
                smallerGroup.getPolicyId().getDisplayName()));

        final SettingActivePolicyApiDTO smallerActivePolicy =
            activePoliciesByName.get(smallerGroup.getPolicyId().getDisplayName());
        assertThat(smallerActivePolicy.getNumEntities(), is(smallerGroup.getEntityOidsCount()));
        assertThat(smallerActivePolicy.getValue(), is("true"));
        assertThat(smallerActivePolicy.getSettingsPolicy().getUuid(),
            is(Long.toString(smallerGroup.getPolicyId().getPolicyId())));
        assertThat(smallerActivePolicy.getSettingsPolicy().getDisplayName(),
            is(smallerGroup.getPolicyId().getDisplayName()));

        final SettingActivePolicyApiDTO dominantActivePolicy =
            activePoliciesByName.get(dominantGroup.getPolicyId().getDisplayName());
        assertThat(dominantActivePolicy.getNumEntities(), is(dominantGroup.getEntityOidsCount()));
        assertThat(dominantActivePolicy.getValue(), is("false"));
        assertThat(dominantActivePolicy.getSettingsPolicy().getUuid(),
            is(Long.toString(dominantGroup.getPolicyId().getPolicyId())));
        assertThat(dominantActivePolicy.getSettingsPolicy().getDisplayName(),
            is(dominantGroup.getPolicyId().getDisplayName()));
    }

    @Test
    public void testEntityGroupMappingDefaultPolicyBreakdown() {
        // If the only active policy is the default policy, we don't include it in the
        // list of active policies even if a breakdown is requested.
        final SettingsMapper settingsMapper = mock(SettingsMapper.class);
        final EntitySettingGroupMapper groupMapper = new EntitySettingGroupMapper(settingsMapper);

        final EntitySettingGroup defaultGroup = EntitySettingGroup.newBuilder()
            .setSetting(Setting.newBuilder()
                .setSettingSpecName(FOO_SETTING_SPEC.getName())
                .setBooleanSettingValue(BooleanSettingValue.newBuilder()
                    .setValue(true)))
            .setPolicyId(SettingPolicyId.newBuilder()
                .setPolicyId(5L)
                .setType(Type.DEFAULT)
                .setDisplayName("defaultPolicy"))
            .addEntityOids(1L)
            .build();

        final SettingApiDTOPossibilities defaultPossibilities = mock(SettingApiDTOPossibilities.class);
        final SettingApiDTO defaultSetting = new SettingApiDTO();
        defaultSetting.setValue("true");
        when(defaultPossibilities.getSettingForEntityType(UIEntityType.VIRTUAL_MACHINE.apiStr()))
            .thenReturn(Optional.of(defaultSetting));
        when(settingsMapper.toSettingApiDto(defaultGroup.getSetting(), FOO_SETTING_SPEC))
            .thenReturn(defaultPossibilities);

        final Optional<SettingApiDTO<String>> resultOpt = groupMapper.toSettingApiDto(
            Collections.singletonList(defaultGroup),
            FOO_SETTING_SPEC,
            Optional.of(UIEntityType.VIRTUAL_MACHINE), true);

        // Relying on object equality - the "base" should still be the dominant setting.
        final SettingApiDTO result = resultOpt.get();
        assertThat(result.getActiveSettingsPolicies(), is(Collections.emptyList()));
        assertThat(result.getSourceGroupName(), is(defaultGroup.getPolicyId().getDisplayName()));
        assertThat(result.getSourceGroupUuid(),
            is(Long.toString(defaultGroup.getPolicyId().getPolicyId())));
    }
}