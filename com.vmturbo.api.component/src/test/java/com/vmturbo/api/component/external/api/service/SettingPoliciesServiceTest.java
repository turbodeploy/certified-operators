package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableList;

import io.grpc.Status;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.dto.settingspolicy.SettingsPolicyApiDTO;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.DeleteSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.ResetSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.ResetSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.Scope;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingPolicyServiceMole;
import com.vmturbo.common.protobuf.setting.SettingProtoMoles.SettingServiceMole;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

/**
 * Unit tests for {@link SettingsPoliciesService}.
 */
public class SettingPoliciesServiceTest {

    private static final long GROUP_ID = 1L;
    private static final long SETTING_POLICY_ID = 7L;
    private static final String SETTING_NAME = "be cool";
    private static final String GROUP_NAME = "the krew";

    private static final SettingPolicyInfo DEFAULT_POLICY_INFO = SettingPolicyInfo.newBuilder()
            .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
            .build();

    private static final SettingPolicy DEFAULT_POLICY = SettingPolicy.newBuilder()
            .setId(SETTING_POLICY_ID)
            .setInfo(DEFAULT_POLICY_INFO)
            .build();

    private static final SettingPolicyInfo SCOPE_POLICY_INFO = SettingPolicyInfo.newBuilder()
            .setScope(Scope.newBuilder()
                    .addGroups(GROUP_ID))
            .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
            .build();


    private static final SettingPolicy SCOPE_POLICY = SettingPolicy.newBuilder()
            .setId(SETTING_POLICY_ID)
            .setInfo(SCOPE_POLICY_INFO)
            .build();


    private static final SettingSpec SETTING_SPEC = SettingSpec.newBuilder()
            .setName(SETTING_NAME)
            .build();

    private static final Grouping GROUP = Grouping.newBuilder()
            .setId(GROUP_ID)
            .setDefinition(GroupDefinition.newBuilder()
                    .setType(GroupType.REGULAR)
                    .setDisplayName(GROUP_NAME))
            .build();

    private static final SettingsPolicyApiDTO RET_SP_DTO = new SettingsPolicyApiDTO();

    private final SettingsPolicyApiDTO inputPolicy = new SettingsPolicyApiDTO();

    private SettingsPoliciesService settingsPoliciesService;

    private SettingPolicyServiceMole settingPolicyBackend = spy(new SettingPolicyServiceMole());

    private SettingsMapper settingsMapper = mock(SettingsMapper.class);

    private final SettingServiceMole settingBackend = spy(new SettingServiceMole());

    private SettingServiceBlockingStub settingServiceStub;


    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(settingPolicyBackend,settingBackend);

    @Before
    public void setup() throws IOException {
        settingServiceStub = SettingServiceGrpc.newBlockingStub(grpcTestServer.getChannel());

        settingsPoliciesService = new SettingsPoliciesService(settingServiceStub, settingsMapper,
                SettingPolicyServiceGrpc.newBlockingStub(grpcTestServer.getChannel()));

        final SettingsManagerApiDTO mgr = new SettingsManagerApiDTO();
        final SettingApiDTO<?> setting = new SettingApiDTO<>();
        setting.setUuid(SETTING_NAME);
        mgr.setSettings(Collections.singletonList(setting));
        inputPolicy.setSettingsManagers(Collections.singletonList(mgr));
    }

    @Test
    public void testGetPoliciesEntityTypesNull() throws Exception {
        when(settingPolicyBackend.listSettingPolicies(any()))
                .thenReturn(Collections.singletonList(DEFAULT_POLICY));
        // Map should be empty, since the policy is a default type.
        when(settingsMapper.convertSettingPolicies(eq(ImmutableList.of(DEFAULT_POLICY,
                settingsPoliciesService.createGlobalSettingPolicy())),
                eq(Collections.emptySet())))
            .thenReturn(Collections.singletonList(RET_SP_DTO));
        List<SettingsPolicyApiDTO> ret =
                settingsPoliciesService.getSettingsPolicies(false, null);
        assertThat(ret, containsInAnyOrder(RET_SP_DTO));
    }

    @Test
    public void testGetPoliciesWithGlobalPolicyInjection() throws Exception {
        when(settingPolicyBackend.listSettingPolicies(any()))
                .thenReturn(Collections.singletonList(DEFAULT_POLICY));

        // Since default policy has no entity type we should inject Global Action Policy
        // and statsMapper should be called with two policies : Default and Global.
        final List<SettingPolicy> settingPolicies = new LinkedList<>();
        settingPolicies.add(DEFAULT_POLICY);
        settingPolicies.add(settingsPoliciesService.createGlobalSettingPolicy());

        List<SettingsPolicyApiDTO> ret =
                settingsPoliciesService.getSettingsPolicies(false, Collections.emptyList());

        // Verify stats mapper is called with two policies mentioned above.
        verify(settingsMapper, Mockito.times(1)).convertSettingPolicies(settingPolicies,
                Collections.emptySet());
    }

    @Test
    public void testGetPolicies() throws Exception {
        when(settingPolicyBackend.listSettingPolicies(any()))
                .thenReturn(Collections.singletonList(SCOPE_POLICY));

        when(settingsMapper.convertSettingPolicies(any(), eq(Collections.emptySet())))
                .thenReturn(Collections.singletonList(RET_SP_DTO));

        List<SettingsPolicyApiDTO> ret =
                settingsPoliciesService.getSettingsPolicies(false, Collections.emptyList());
        assertThat(ret, containsInAnyOrder(RET_SP_DTO));
    }

    @Test
    public void testGetPoliciesEntityTypes() throws Exception {

        // This one should get filtered out.
        final SettingPolicy wrongEntityTypePolicy = SettingPolicy.newBuilder()
                .setId(GROUP_ID + 1)
                .setSettingPolicyType(Type.DEFAULT)
                .setInfo(SettingPolicyInfo.newBuilder()
                        .setEntityType(EntityType.STORAGE.getNumber()))
                .build();

        when(settingPolicyBackend.listSettingPolicies(any()))
                .thenReturn(Arrays.asList(DEFAULT_POLICY, wrongEntityTypePolicy));
        // Map should be empty, since the policy is a default type.
        when(settingsMapper.convertSettingPolicies(eq(Collections.singletonList(DEFAULT_POLICY)),
                eq(Collections.emptySet()))).thenReturn(Collections.singletonList(RET_SP_DTO));

        List<SettingsPolicyApiDTO> ret =
                settingsPoliciesService.getSettingsPolicies(false,
                    Collections.singletonList(ApiEntityType.VIRTUAL_MACHINE.apiStr()));
        assertThat(ret, containsInAnyOrder(RET_SP_DTO));
    }

    @Test
    public void testCreatePolicy() throws Exception {
        when(settingsMapper.convertNewInputPolicy(eq(inputPolicy)))
            .thenReturn(SCOPE_POLICY_INFO);

        when(settingPolicyBackend.createSettingPolicy(CreateSettingPolicyRequest.newBuilder()
                .setSettingPolicyInfo(SCOPE_POLICY_INFO)
                .build()))
            .thenReturn(CreateSettingPolicyResponse.newBuilder()
                .setSettingPolicy(SCOPE_POLICY)
                .build());

        when(settingsMapper.convertSettingPolicy(eq(SCOPE_POLICY)))
            .thenReturn(RET_SP_DTO);

        SettingsPolicyApiDTO retDto = settingsPoliciesService.createSettingsPolicy(inputPolicy);
        assertEquals(retDto, RET_SP_DTO);
    }

    @Test
    public void testUpdatePolicy() throws Exception {
        final long id = 7;
        when(settingsMapper.convertEditedInputPolicy(eq(id), eq(inputPolicy)))
                .thenReturn(SCOPE_POLICY_INFO);
        when(settingPolicyBackend.updateSettingPolicy(UpdateSettingPolicyRequest.newBuilder()
                .setId(id)
                .setNewInfo(SCOPE_POLICY_INFO)
                .build()))
            .thenReturn(UpdateSettingPolicyResponse.newBuilder()
                .setSettingPolicy(SCOPE_POLICY)
                .build());

        when(settingsMapper.convertSettingPolicy(eq(SCOPE_POLICY)))
            .thenReturn(RET_SP_DTO);

        final SettingsPolicyApiDTO retDto =
                settingsPoliciesService.editSettingsPolicy(Long.toString(id), false, inputPolicy);
        assertEquals(retDto, RET_SP_DTO);
    }

    @Test
    public void testUpdatePolicySetDefault() throws Exception {
        final long id = 7;
        String rateOfResizeSettingName = GlobalSettingSpecs.RateOfResize.getSettingName();
        float defaultValue = GlobalSettingSpecs.RateOfResize.createSettingSpec().getNumericSettingValueType().getDefault();
        when(settingsMapper.convertEditedInputPolicy(eq(id), eq(inputPolicy)))
                .thenReturn(SCOPE_POLICY_INFO);
        when(settingPolicyBackend.resetSettingPolicy(ResetSettingPolicyRequest.newBuilder()
                .setSettingPolicyId(id)
                .build()))
                .thenReturn(ResetSettingPolicyResponse.newBuilder()
                        .setSettingPolicy(SCOPE_POLICY)
                        .build());

        when(settingsMapper.convertSettingPolicy(eq(SCOPE_POLICY)))
                .thenReturn(RET_SP_DTO);
        when(settingBackend.updateGlobalSetting(
            UpdateGlobalSettingRequest.newBuilder().addSetting(Setting.newBuilder()
                .setSettingSpecName(rateOfResizeSettingName)
                .setNumericSettingValue(
                    SettingDTOUtil.createNumericSettingValue(
                        defaultValue)))
                .build())).thenReturn(UpdateGlobalSettingResponse.newBuilder().build());
        final SettingsPolicyApiDTO retDto =
                settingsPoliciesService.editSettingsPolicy(Long.toString(id), true, inputPolicy);
        verify(settingPolicyBackend).resetSettingPolicy(any());
        verify(settingBackend).updateGlobalSetting(any());
        assertEquals(retDto, RET_SP_DTO);
    }

    @Test(expected = OperationFailedException.class)
    public void testUpdatePolicyExistingName() throws Exception {
        final long id = 7;
        when(settingsMapper.convertEditedInputPolicy(eq(id), eq(inputPolicy)))
                .thenReturn(DEFAULT_POLICY_INFO);

        when(settingPolicyBackend.updateSettingPolicyError(any()))
                .thenReturn(Optional.of(Status.ALREADY_EXISTS.asException()));

        settingsPoliciesService.editSettingsPolicy(Long.toString(id), false, inputPolicy);
    }

    @Test(expected = UnknownObjectException.class)
    public void testUpdatePolicyNotFound() throws Exception {
        final long id = 7;
        when(settingsMapper.convertEditedInputPolicy(eq(id), eq(inputPolicy)))
                .thenReturn(DEFAULT_POLICY_INFO);

        when(settingPolicyBackend.updateSettingPolicyError(any()))
                .thenReturn(Optional.of(Status.NOT_FOUND.asException()));

        settingsPoliciesService.editSettingsPolicy(Long.toString(id), false, inputPolicy);
    }

    @Test(expected = InvalidOperationException.class)
    public void testUpdatePolicyNotValid() throws Exception {
        final long id = 7;
        when(settingsMapper.convertEditedInputPolicy(eq(id), eq(inputPolicy)))
                .thenReturn(DEFAULT_POLICY_INFO);

        when(settingPolicyBackend.updateSettingPolicyError(any()))
                .thenReturn(Optional.of(Status.INVALID_ARGUMENT.asException()));

        settingsPoliciesService.editSettingsPolicy(Long.toString(id), false, inputPolicy);
    }

    @Test(expected = NumberFormatException.class)
    public void testUpdatePolicyStringId() throws Exception {
        settingsPoliciesService.editSettingsPolicy("blah", false, inputPolicy);
    }

    @Test
    public void testDeletePolicy() throws Exception {
        final long id = 7;
        assertTrue(settingsPoliciesService.deleteSettingsPolicy(Long.toString(id)));
        verify(settingPolicyBackend).deleteSettingPolicy(
                eq(DeleteSettingPolicyRequest.newBuilder()
                    .setId(id)
                    .build()), any());
    }

    @Test(expected = UnknownObjectException.class)
    public void testDeletePolicyNotFound() throws Exception {
        final long id = 7;
        when(settingPolicyBackend.deleteSettingPolicyError(eq(DeleteSettingPolicyRequest.newBuilder()
                .setId(id)
                .build()))).thenReturn(Optional.of(Status.NOT_FOUND.asException()));
        settingsPoliciesService.deleteSettingsPolicy(Long.toString(id));
    }

    @Test(expected = InvalidOperationException.class)
    public void testDeletePolicyInvalidDelete() throws Exception {
        final long id = 7;
        when(settingPolicyBackend.deleteSettingPolicyError(eq(DeleteSettingPolicyRequest.newBuilder()
                .setId(id)
                .build()))).thenReturn(Optional.of(Status.INVALID_ARGUMENT.asException()));
        settingsPoliciesService.deleteSettingsPolicy(Long.toString(id));
    }

    @Test(expected = NumberFormatException.class)
    public void testDeletePolicyInvalidId() throws Exception {
        settingsPoliciesService.deleteSettingsPolicy("blah");
    }

    /**
     * Technically the UI should prevent duplicate names from being created (it does right now),
     * but we still want to test that case in the API.
     */
    @Test(expected = OperationFailedException.class)
    public void testCreateExistingPolicy() throws Exception {

        when(settingsMapper.convertNewInputPolicy(eq(inputPolicy)))
            .thenReturn(DEFAULT_POLICY_INFO);

        when(settingPolicyBackend.createSettingPolicyError(any()))
            .thenReturn(Optional.of(Status.ALREADY_EXISTS.asException()));

        settingsPoliciesService.createSettingsPolicy(inputPolicy);
    }

    @Test(expected = InvalidOperationException.class)
    public void testCreateInvalidPolicy() throws Exception {
        when(settingsMapper.convertNewInputPolicy(any())).thenThrow(InvalidOperationException.class);

        settingsPoliciesService.createSettingsPolicy(inputPolicy);
    }
}
