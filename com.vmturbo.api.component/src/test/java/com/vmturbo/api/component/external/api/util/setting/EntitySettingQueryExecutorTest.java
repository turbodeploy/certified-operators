package com.vmturbo.api.component.external.api.util.setting;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyCollection;
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

import com.google.common.collect.Maps;

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
import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup.SettingPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SearchSettingSpecsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
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

    private UserSessionContext userSessionContext = mock(UserSessionContext.class);

    @Nonnull
    private EntitySettingQueryExecutor newExecutor() {
        return new EntitySettingQueryExecutor(SettingPolicyServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            SettingServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
            groupExpander, entitySettingGroupMapper, settingsManagerMapping, userSessionContext);
    }

    /**
     * Setup.
     */
    @Before
    public void setup() {
        doReturn(Collections.singletonList(FOO_SETTING_SPEC)).when(settingServiceBackend).searchSettingSpecs(any());

        when(settingsManagerMapping.getManagerUuid(SPEC_NAME)).thenReturn(Optional.of(MGR_UUID));

        SettingApiDTO mappedSetting = new SettingApiDTO();
        mappedSetting.setDisplayName(SPEC_NAME);
        when(entitySettingGroupMapper.toSettingApiDto(any(), any(), any(), anyBoolean(), any())).thenReturn(Optional.of(mappedSetting));

        MAPPED_MGR_DTO.setUuid(MGR_UUID);
        final SettingsManagerInfo mgrInfo = mock(SettingsManagerInfo.class);
        when(mgrInfo.newApiDTO(MGR_UUID)).thenReturn(MAPPED_MGR_DTO);

        when(settingsManagerMapping.getManagerInfo(MGR_UUID)).thenReturn(Optional.of(mgrInfo));

        doReturn(Collections.singletonList(GetEntitySettingsResponse.newBuilder()
            .addSettingGroup(SETTING_GROUP)
            .build())).when(settingPolicyServiceBackend).getEntitySettings(any());

        when(userSessionContext.isUserScoped()).thenReturn(false);
    }

    @Test
    public void testGetSettingsIndividualEntity() {
        // ARRANGE
        final EntitySettingQueryExecutor executor = newExecutor();
        final ApiId scope = mock(ApiId.class);
        final long scopeId = 7;
        final ApiEntityType scopeType = ApiEntityType.VIRTUAL_MACHINE;
        when(scope.isGroup()).thenReturn(false);
        when(scope.oid()).thenReturn(scopeId);
        when(scope.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(scopeType)));

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
            FOO_SETTING_SPEC, Optional.of(scopeType), false, Collections.emptyMap());
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

    @Test
    public void testGetSettingsGroup() {
        // ARRANGE
        final EntitySettingQueryExecutor executor = newExecutor();
        final ApiId scope = mock(ApiId.class);
        final long scopeId = 7;
        final long groupMember = 77;
        final ApiEntityType scopeType = ApiEntityType.VIRTUAL_MACHINE;
        when(scope.isGroup()).thenReturn(true);
        when(scope.oid()).thenReturn(scopeId);
        when(scope.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(scopeType)));

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
            FOO_SETTING_SPEC, Optional.of(scopeType), false, Collections.emptyMap());
        verify(settingsManagerMapping).getManagerInfo(MGR_UUID);
        assertThat(retMgrs, containsInAnyOrder(MAPPED_MGR_DTO));
    }

    /**
     * Test invocation of getEntitySettings by a user with a scoped role. If the entity OID is not
     * in scope a UserAccessScopeException is expected.
     */
    @Test(expected = UserAccessScopeException.class)
    public void testUserScopeValidation() {
        // ARRANGE
        final EntitySettingQueryExecutor executor = newExecutor();
        final ApiId scope = mock(ApiId.class);
        final long scopeId = 7;
        final long groupMember = 77;
        final ApiEntityType scopeType = ApiEntityType.VIRTUAL_MACHINE;
        when(scope.isGroup()).thenReturn(true);
        when(scope.oid()).thenReturn(scopeId);
        when(scope.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(scopeType)));

        when(groupExpander.expandOids(any())).thenReturn(Collections.singleton(groupMember));

        EntityAccessScope userScope = mock(EntityAccessScope.class);
        when(userScope.contains(anyCollection())).thenReturn(false);
        when(userSessionContext.getUserAccessScope()).thenReturn(userScope);
        when(userSessionContext.isUserScoped()).thenReturn(true);

        // ACT
        final List<SettingsManagerApiDTO> retMgrs = executor.getEntitySettings(scope, false);
    }

    /**
     * Test invocation of getEntitySettings by a user with a scoped role. If the entity OID is not
     * in scope a UserAccessScopeException is expected.
     */
    @Test
    public void testScopedUserAccessingEntityInScope() {
        // ARRANGE
        final EntitySettingQueryExecutor executor = newExecutor();
        final ApiId scope = mock(ApiId.class);
        final long scopeId = 7;
        final long groupMember = 77;
        final ApiEntityType scopeType = ApiEntityType.VIRTUAL_MACHINE;
        when(scope.isGroup()).thenReturn(true);
        when(scope.oid()).thenReturn(scopeId);
        when(scope.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(scopeType)));

        when(groupExpander.expandOids(any())).thenReturn(Collections.singleton(groupMember));

        EntityAccessScope userScope = mock(EntityAccessScope.class);
        when(userScope.contains(anyCollection())).thenReturn(true);
        when(userSessionContext.getUserAccessScope()).thenReturn(userScope);
        when(userSessionContext.isUserScoped()).thenReturn(true);

        // ACT
        final List<SettingsManagerApiDTO> retMgrs = executor.getEntitySettings(scope, false);

        // Assert getEntitySettings complete without exception.
        assertThat(retMgrs, containsInAnyOrder(MAPPED_MGR_DTO));
    }

    /**
     * When getEntitySettings is called for a group which has multiple setting groups, with at
     * least one setting group having policies for the same spec FOO, then ensure that we fetch all
     * these raw policies. But we should not fetch the policies listed for some other spec which
     * does not have more than one policy id.
     */
    @Test
    public void testGetSettingsGroupForSettingGroupWithTwoPolicies() {
        // ARRANGE
        final EntitySettingQueryExecutor executor = newExecutor();
        final ApiId scope = mock(ApiId.class);
        final long scopeId = 7;
        final long groupMember = 77;
        final ApiEntityType scopeType = ApiEntityType.VIRTUAL_MACHINE;
        when(scope.isGroup()).thenReturn(true);
        when(scope.oid()).thenReturn(scopeId);
        when(scope.getScopeTypes()).thenReturn(Optional.of(Collections.singleton(scopeType)));

        when(groupExpander.expandOids(any())).thenReturn(Collections.singleton(groupMember));

        EntitySettingGroup settingGroup1 = EntitySettingGroup.newBuilder()
            .setSetting(Setting.newBuilder()
                .setSettingSpecName(SPEC_NAME))
            .addEntityOids(123)
            .addPolicyId(SettingPolicyId.newBuilder()
                .setPolicyId(3L).build())
            .build();
        EntitySettingGroup settingGroup2 = EntitySettingGroup.newBuilder()
            .setSetting(Setting.newBuilder()
                .setSettingSpecName(SPEC_NAME))
            .addEntityOids(456)
            .addPolicyId(SettingPolicyId.newBuilder()
                .setPolicyId(1L).build())
            .addPolicyId(SettingPolicyId.newBuilder()
                .setPolicyId(2L).build())
            .build();
        EntitySettingGroup settingGroup3 = EntitySettingGroup.newBuilder()
            .setSetting(Setting.newBuilder()
                .setSettingSpecName("bar"))
            .addEntityOids(789)
            .addPolicyId(SettingPolicyId.newBuilder()
                .setPolicyId(4L).build())
            .build();

        doReturn(Collections.singletonList(GetEntitySettingsResponse.newBuilder()
            .addSettingGroup(settingGroup1)
            .addSettingGroup(settingGroup2)
            .addSettingGroup(settingGroup3).build()))
            .when(settingPolicyServiceBackend).getEntitySettings(any());

        when(settingsManagerMapping.getManagerUuid("bar")).thenReturn(Optional.of(MGR_UUID));

        SettingApiDTO mappedSetting = new SettingApiDTO();
        mappedSetting.setDisplayName("bar");
        when(entitySettingGroupMapper.toSettingApiDto(any(), any(), any(), anyBoolean(), any())).thenReturn(Optional.of(mappedSetting));

        // ACT
        executor.getEntitySettings(scope, true);

        // ASSERT
        verify(settingPolicyServiceBackend).listSettingPolicies(ListSettingPoliciesRequest.newBuilder()
            .addAllIdFilter(Arrays.asList(1L, 2L, 3L))
            .build());
    }

    /**<p>Group 1 consists of 3 VMs (ids 90, 91, 92).
     * Group 2 consists of 1 VM (id 90).
     * Policy P1 excludes templates 1 and 2, and is applied to Group 1. So it applies to 3 VMs.
     * Policy P2 excludes templates 3 and 4, and is applied to Group 2. So it applies to 1 VM.</p>
     *<p>This gives rise to two EntitySettingGroups - group1WithMultiplePolicyIds and
     * group2WithSinglePolicyId.</p>
     *<p>Group1WithMultiplePolicyIds has 2 policy ids. So in this case, we will just list out the
     *   policies in the activeSettingPolicies of the resultant SettingApiDTO.</p>
     * <p>We ensure that in this test case. We also test that the number of entities each policy is applied to is correct.</p>
     */
    @Test
    public void testToSettingApiDtoForSettingGroupWithTwoPolicies() {
        final SettingsMapper settingsMapper = mock(SettingsMapper.class);
        final EntitySettingGroupMapper groupMapper = new EntitySettingGroupMapper(settingsMapper);

        final EntitySettingGroup group1WithMultiplePolicyIds = EntitySettingGroup.newBuilder()
            .setSetting(Setting.newBuilder()
                .setSettingSpecName(FOO_SETTING_SPEC.getName())
                .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                    .addAllOids(Arrays.asList(1L, 2L, 3L, 4L))))
            .addPolicyId(SettingPolicyId.newBuilder()
                .setPolicyId(11L)
                .setType(Type.USER)
                .setDisplayName("P1"))
            .addPolicyId(SettingPolicyId.newBuilder()
                .setPolicyId(12L)
                .setType(Type.USER)
                .setDisplayName("P2"))
            .addEntityOids(90L)
            .build();

        final EntitySettingGroup group2WithSinglePolicyId = EntitySettingGroup.newBuilder()
            .setSetting(Setting.newBuilder()
                .setSettingSpecName(FOO_SETTING_SPEC.getName())
                .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                    .addAllOids(Arrays.asList(1L, 2L))))
            .addPolicyId(SettingPolicyId.newBuilder()
                .setPolicyId(11L)
                .setType(Type.USER)
                .setDisplayName("P1"))
            .addAllEntityOids(Arrays.asList(91L, 92L))
            .build();

        final SettingApiDTOPossibilities possibilities = mock(SettingApiDTOPossibilities.class);
        final SettingApiDTO settingApi = new SettingApiDTO();
        settingApi.setValue("1,2");
        when(possibilities.getSettingForEntityType(ApiEntityType.VIRTUAL_MACHINE.apiStr()))
            .thenReturn(Optional.of(settingApi));
        when(settingsMapper.toSettingApiDto(group2WithSinglePolicyId.getSetting(), FOO_SETTING_SPEC))
            .thenReturn(possibilities);

        Setting policy1Setting = Setting.newBuilder()
            .setSettingSpecName(FOO_SETTING_SPEC.getName())
            .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                .addAllOids(Arrays.asList(1L, 2L))).build();
        final SettingApiDTOPossibilities possibilities1 = mock(SettingApiDTOPossibilities.class);
        final SettingApiDTO settingApi1 = new SettingApiDTO();
        settingApi1.setValue("1,2");
        when(possibilities1.getSettingForEntityType(ApiEntityType.VIRTUAL_MACHINE.apiStr()))
            .thenReturn(Optional.of(settingApi1));
        when(settingsMapper.toSettingApiDto(policy1Setting, FOO_SETTING_SPEC))
            .thenReturn(possibilities1);

        Setting policy2Setting = Setting.newBuilder()
            .setSettingSpecName(FOO_SETTING_SPEC.getName())
            .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                .addAllOids(Arrays.asList(3L, 4L))).build();
        final SettingApiDTOPossibilities possibilities2 = mock(SettingApiDTOPossibilities.class);
        final SettingApiDTO settingApi2 = new SettingApiDTO();
        settingApi2.setValue("3,4");
        when(possibilities2.getSettingForEntityType(ApiEntityType.VIRTUAL_MACHINE.apiStr()))
            .thenReturn(Optional.of(settingApi2));
        when(settingsMapper.toSettingApiDto(policy2Setting, FOO_SETTING_SPEC))
            .thenReturn(possibilities2);

        final Map<Long, SettingPolicy> rawSettingPoliciesById = Maps.newHashMap();
        rawSettingPoliciesById.put(11L, SettingPolicy.newBuilder().setInfo(
            SettingPolicyInfo.newBuilder().addSettings(policy1Setting).build()).build());
        rawSettingPoliciesById.put(12L, SettingPolicy.newBuilder().setInfo(
            SettingPolicyInfo.newBuilder().addSettings(policy2Setting).build()).build());

        final Optional<SettingApiDTO<String>> resultOpt = groupMapper.toSettingApiDto(
            Arrays.asList(group1WithMultiplePolicyIds, group2WithSinglePolicyId),
            FOO_SETTING_SPEC,
            Optional.of(ApiEntityType.VIRTUAL_MACHINE), true, rawSettingPoliciesById);

        assertEquals(2, resultOpt.get().getActiveSettingsPolicies().size());
        assertEquals(3, (int)resultOpt.get().getActiveSettingsPolicies().get(0).getNumEntities());
        assertEquals("1,2", resultOpt.get().getActiveSettingsPolicies().get(0).getValue());
        assertEquals("11", resultOpt.get().getActiveSettingsPolicies().get(0).getSettingsPolicy().getUuid());
        assertEquals("P1", resultOpt.get().getActiveSettingsPolicies().get(0).getSettingsPolicy().getDisplayName());

        assertEquals(1, (int)resultOpt.get().getActiveSettingsPolicies().get(1).getNumEntities());
        assertEquals("3,4", resultOpt.get().getActiveSettingsPolicies().get(1).getValue());
        assertEquals("12", resultOpt.get().getActiveSettingsPolicies().get(1).getSettingsPolicy().getUuid());
        assertEquals("P2", resultOpt.get().getActiveSettingsPolicies().get(1).getSettingsPolicy().getDisplayName());
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
        when(smallerPossibilities.getSettingForEntityType(ApiEntityType.VIRTUAL_MACHINE.apiStr()))
            .thenReturn(Optional.of(smallerSetting));
        when(settingsMapper.toSettingApiDto(smallerGroup.getSetting(), FOO_SETTING_SPEC))
            .thenReturn(smallerPossibilities);

        final SettingApiDTOPossibilities dominantPossibilities = mock(SettingApiDTOPossibilities.class);
        final SettingApiDTO dominantSetting = new SettingApiDTO();
        when(dominantPossibilities.getSettingForEntityType(ApiEntityType.VIRTUAL_MACHINE.apiStr()))
            .thenReturn(Optional.of(dominantSetting));
        when(settingsMapper.toSettingApiDto(dominantGroup.getSetting(), FOO_SETTING_SPEC))
            .thenReturn(dominantPossibilities);

        final Optional<SettingApiDTO<String>> result = groupMapper.toSettingApiDto(
            Arrays.asList(smallerGroup, dominantGroup),
            FOO_SETTING_SPEC,
            Optional.of(ApiEntityType.VIRTUAL_MACHINE), false, Collections.emptyMap());

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
            .addPolicyId(SettingPolicyId.newBuilder()
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
            .addPolicyId(SettingPolicyId.newBuilder()
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
        when(smallerPossibilities.getSettingForEntityType(ApiEntityType.VIRTUAL_MACHINE.apiStr()))
            .thenReturn(Optional.of(smallerSetting));
        when(settingsMapper.toSettingApiDto(smallerGroup.getSetting(), FOO_SETTING_SPEC))
            .thenReturn(smallerPossibilities);

        final SettingApiDTOPossibilities dominantPossibilities = mock(SettingApiDTOPossibilities.class);
        final SettingApiDTO dominantSetting = new SettingApiDTO();
        dominantSetting.setValue("false");
        when(dominantPossibilities.getSettingForEntityType(ApiEntityType.VIRTUAL_MACHINE.apiStr()))
            .thenReturn(Optional.of(dominantSetting));
        when(settingsMapper.toSettingApiDto(dominantGroup.getSetting(), FOO_SETTING_SPEC))
            .thenReturn(dominantPossibilities);

        final Optional<SettingApiDTO<String>> resultOpt = groupMapper.toSettingApiDto(
            Arrays.asList(smallerGroup, dominantGroup),
            FOO_SETTING_SPEC,
            Optional.of(ApiEntityType.VIRTUAL_MACHINE), true, Collections.emptyMap());

        // Relying on object equality - the "base" should still be the dominant setting.
        final SettingApiDTO<String> result = resultOpt.get();
        assertThat(result, is(dominantSetting));
        // The rest of SettingApiDTO properties should be set by the settings mapper, so we don't
        // check them.

        assertThat(result.getSourceGroupName(), is(dominantGroup.getPolicyId(0).getDisplayName()));
        assertThat(result.getSourceGroupUuid(),
            is(Long.toString(dominantGroup.getPolicyId(0).getPolicyId())));
        final Map<String, SettingActivePolicyApiDTO> activePoliciesByName =
            result.getActiveSettingsPolicies().stream()
                .collect(Collectors.toMap(
                    policy -> policy.getSettingsPolicy().getDisplayName(),
                    Function.identity()));
        assertThat(activePoliciesByName.keySet(),
            containsInAnyOrder(dominantGroup.getPolicyId(0).getDisplayName(),
                smallerGroup.getPolicyId(0).getDisplayName()));

        final SettingActivePolicyApiDTO smallerActivePolicy =
            activePoliciesByName.get(smallerGroup.getPolicyId(0).getDisplayName());
        assertThat(smallerActivePolicy.getNumEntities(), is(smallerGroup.getEntityOidsCount()));
        assertThat(smallerActivePolicy.getValue(), is("true"));
        assertThat(smallerActivePolicy.getSettingsPolicy().getUuid(),
            is(Long.toString(smallerGroup.getPolicyId(0).getPolicyId())));
        assertThat(smallerActivePolicy.getSettingsPolicy().getDisplayName(),
            is(smallerGroup.getPolicyId(0).getDisplayName()));

        final SettingActivePolicyApiDTO dominantActivePolicy =
            activePoliciesByName.get(dominantGroup.getPolicyId(0).getDisplayName());
        assertThat(dominantActivePolicy.getNumEntities(), is(dominantGroup.getEntityOidsCount()));
        assertThat(dominantActivePolicy.getValue(), is("false"));
        assertThat(dominantActivePolicy.getSettingsPolicy().getUuid(),
            is(Long.toString(dominantGroup.getPolicyId(0).getPolicyId())));
        assertThat(dominantActivePolicy.getSettingsPolicy().getDisplayName(),
            is(dominantGroup.getPolicyId(0).getDisplayName()));
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
            .addPolicyId(SettingPolicyId.newBuilder()
                .setPolicyId(5L)
                .setType(Type.DEFAULT)
                .setDisplayName("defaultPolicy"))
            .addEntityOids(1L)
            .build();

        final SettingApiDTOPossibilities defaultPossibilities = mock(SettingApiDTOPossibilities.class);
        final SettingApiDTO defaultSetting = new SettingApiDTO();
        defaultSetting.setValue("true");
        when(defaultPossibilities.getSettingForEntityType(ApiEntityType.VIRTUAL_MACHINE.apiStr()))
            .thenReturn(Optional.of(defaultSetting));
        when(settingsMapper.toSettingApiDto(defaultGroup.getSetting(), FOO_SETTING_SPEC))
            .thenReturn(defaultPossibilities);

        final Optional<SettingApiDTO<String>> resultOpt = groupMapper.toSettingApiDto(
            Collections.singletonList(defaultGroup),
            FOO_SETTING_SPEC,
            Optional.of(ApiEntityType.VIRTUAL_MACHINE), true, Collections.emptyMap());

        // Relying on object equality - the "base" should still be the dominant setting.
        final SettingApiDTO result = resultOpt.get();
        assertThat(result.getActiveSettingsPolicies(), is(Collections.emptyList()));
        assertThat(result.getSourceGroupName(), is(defaultGroup.getPolicyId(0).getDisplayName()));
        assertThat(result.getSourceGroupUuid(),
            is(Long.toString(defaultGroup.getPolicyId(0).getPolicyId())));
    }
}
