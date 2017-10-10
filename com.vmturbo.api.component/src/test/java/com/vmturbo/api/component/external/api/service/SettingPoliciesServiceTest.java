package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.api.component.external.api.mapper.ServiceEntityMapper;
import com.vmturbo.api.component.external.api.mapper.SettingsMapper;
import com.vmturbo.api.dto.setting.SettingApiDTO;
import com.vmturbo.api.dto.setting.SettingsManagerApiDTO;
import com.vmturbo.api.dto.setting.SettingsPolicyApiDTO;
import com.vmturbo.api.exceptions.InvalidOperationException;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceImplBase;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceImplBase;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.DefaultType;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.ScopeType;
import com.vmturbo.common.protobuf.setting.SettingProto.SearchSettingSpecsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceImplBase;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Unit tests for {@link SettingsPoliciesService}.
 */
public class SettingPoliciesServiceTest {

    private static final long GROUP_ID = 1L;
    private static final long SETTING_POLICY_ID = 7L;
    private static final String SETTING_NAME = "be cool";
    private static final String GROUP_NAME = "the krew";

    private static final SettingPolicyInfo DEFAULT_POLICY_INFO = SettingPolicyInfo.newBuilder()
            .setDefault(DefaultType.getDefaultInstance())
            .setEntityType(EntityType.VIRTUAL_MACHINE.getNumber())
            .build();

    private static final SettingPolicy DEFAULT_POLICY = SettingPolicy.newBuilder()
            .setId(SETTING_POLICY_ID)
            .setInfo(DEFAULT_POLICY_INFO)
            .build();

    private static final SettingPolicyInfo SCOPE_POLICY_INFO = SettingPolicyInfo.newBuilder()
            .setScope(ScopeType.newBuilder()
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

    private static final Group GROUP = Group.newBuilder()
            .setId(GROUP_ID)
            .setInfo(GroupInfo.newBuilder()
                    .setName(GROUP_NAME))
            .build();

    private static final SettingsPolicyApiDTO RET_SP_DTO = new SettingsPolicyApiDTO();

    private final SettingsPolicyApiDTO inputPolicy = new SettingsPolicyApiDTO();

    private SettingsPoliciesService settingsPoliciesService;

    private GrpcTestServer grpcTestServer;

    private TestSettingPolicyService settingPolicyBackend = spy(new TestSettingPolicyService());

    private TestSettingService settingBackend = spy(new TestSettingService());

    private TestGroupService groupBackend = spy(new TestGroupService());

    private SettingsMapper settingsMapper = mock(SettingsMapper.class);

    @Before
    public void setup() throws IOException {
        grpcTestServer = GrpcTestServer.withServices(settingBackend,
                groupBackend, settingPolicyBackend);
        settingsPoliciesService = new SettingsPoliciesService(settingsMapper,
                grpcTestServer.getChannel());

        final SettingsManagerApiDTO mgr = new SettingsManagerApiDTO();
        final SettingApiDTO setting = new SettingApiDTO();
        setting.setUuid(SETTING_NAME);
        mgr.setSettings(Collections.singletonList(setting));
        inputPolicy.setSettingsManagers(Collections.singletonList(mgr));
    }

    @After
    public void teardown() {
        grpcTestServer.close();
    }

    @Test
    public void testGetPoliciesNoInvolvedGroups() throws Exception {
        when(settingPolicyBackend.listSettingPolicies(any()))
            .thenReturn(Collections.singletonList(DEFAULT_POLICY));
        // Map should be empty, since the policy is a default type.
        when(settingsMapper.convertSettingsPolicy(DEFAULT_POLICY,
                Collections.emptyMap())).thenReturn(RET_SP_DTO);
        List<SettingsPolicyApiDTO> ret =
                settingsPoliciesService.getSettingsPolicies(false, Collections.emptyList());
        assertThat(ret, containsInAnyOrder(RET_SP_DTO));

        verify(groupBackend, never()).getGroups(any());
    }

    /**
     * This is basically {@link SettingPoliciesServiceTest#testGetPoliciesNoInvolvedGroups()},
     * but passing in null instead of an empty list to
     * {@link SettingsPoliciesService#getSettingsPolicies(boolean, List)}.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testGetPoliciesEntityTypesNull() throws Exception {
        when(settingPolicyBackend.listSettingPolicies(any()))
                .thenReturn(Collections.singletonList(DEFAULT_POLICY));
        // Map should be empty, since the policy is a default type.
        when(settingsMapper.convertSettingsPolicy(DEFAULT_POLICY,
                Collections.emptyMap())).thenReturn(RET_SP_DTO);
        List<SettingsPolicyApiDTO> ret =
                settingsPoliciesService.getSettingsPolicies(false, null);
        assertThat(ret, containsInAnyOrder(RET_SP_DTO));

        verify(groupBackend, never()).getGroups(any());
    }

    @Test
    public void testGetPoliciesWithGroups() throws Exception {
        when(settingPolicyBackend.listSettingPolicies(any()))
                .thenReturn(Collections.singletonList(SCOPE_POLICY));
        when(groupBackend.getGroups(GetGroupsRequest.newBuilder()
                .addId(GROUP_ID)
                .build()))
            .thenReturn(Collections.singletonList(Group.newBuilder()
                .setId(GROUP_ID)
                .setInfo(GroupInfo.newBuilder()
                        .setName(GROUP_NAME))
                .build()));

        // Expect a group map of one input.
        when(settingsMapper.convertSettingsPolicy(SCOPE_POLICY,
                ImmutableMap.of(GROUP_ID, GROUP_NAME))).thenReturn(RET_SP_DTO);
        List<SettingsPolicyApiDTO> ret =
                settingsPoliciesService.getSettingsPolicies(false, Collections.emptyList());
        assertThat(ret, containsInAnyOrder(RET_SP_DTO));
    }

    @Test
    public void testGetPoliciesEntityTypes() throws Exception {

        // This one should get filtered out.
        final SettingPolicy wrongEntityTypePolicy = SettingPolicy.newBuilder()
                .setId(GROUP_ID + 1)
                .setInfo(SettingPolicyInfo.newBuilder()
                        .setEntityType(EntityType.STORAGE.getNumber())
                        .setDefault(DefaultType.getDefaultInstance()))
                .build();

        when(settingPolicyBackend.listSettingPolicies(any()))
                .thenReturn(Arrays.asList(DEFAULT_POLICY, wrongEntityTypePolicy));
        // Map should be empty, since the policy is a default type.
        when(settingsMapper.convertSettingsPolicy(DEFAULT_POLICY,
                Collections.emptyMap())).thenReturn(RET_SP_DTO);

        List<SettingsPolicyApiDTO> ret =
                settingsPoliciesService.getSettingsPolicies(false,
                    Collections.singletonList(
                        ServiceEntityMapper.toUIEntityType(EntityType.VIRTUAL_MACHINE.getNumber())));
        assertThat(ret, containsInAnyOrder(RET_SP_DTO));

        verify(groupBackend, never()).getGroups(any());
    }

    @Test
    public void testCreatePolicy() throws Exception {

        when(settingBackend.searchSettingSpecs(SearchSettingSpecsRequest.newBuilder()
                .addSettingSpecName(SETTING_NAME)
                .build()))
                .thenReturn(Collections.singletonList(SETTING_SPEC));

        when(settingsMapper.convertInputPolicy(eq(inputPolicy), eq(ImmutableMap.of(SETTING_NAME,
                SETTING_SPEC))))
            .thenReturn(SCOPE_POLICY_INFO);

        when(settingPolicyBackend.createSettingPolicy(CreateSettingPolicyRequest.newBuilder()
                .setSettingPolicyInfo(SCOPE_POLICY_INFO)
                .build()))
            .thenReturn(CreateSettingPolicyResponse.newBuilder()
                .setSettingPolicy(SCOPE_POLICY)
                .build());

        when(groupBackend.getGroups(GetGroupsRequest.newBuilder()
                .addId(GROUP_ID)
                .build()))
            .thenReturn(Collections.singletonList(GROUP));

        when(settingsMapper.convertSettingsPolicy(eq(SCOPE_POLICY),
                    eq(ImmutableMap.of(GROUP_ID, GROUP_NAME))))
            .thenReturn(RET_SP_DTO);

        SettingsPolicyApiDTO retDto = settingsPoliciesService.createSettingsPolicy(inputPolicy);
        assertEquals(retDto, RET_SP_DTO);
    }

    /**
     * Technically the UI should prevent duplicate names from being created (it does right now),
     * but we still want to test that case in the API.
     */
    @Test(expected = OperationFailedException.class)
    public void testCreateExistingPolicy() throws Exception {

        when(settingBackend.searchSettingSpecs(SearchSettingSpecsRequest.newBuilder()
                .addSettingSpecName(SETTING_NAME)
                .build()))
                .thenReturn(Collections.singletonList(SETTING_SPEC));

        when(settingsMapper.convertInputPolicy(eq(inputPolicy), eq(ImmutableMap.of(SETTING_NAME,
                SETTING_SPEC))))
            .thenReturn(DEFAULT_POLICY_INFO);

        when(settingPolicyBackend.createSettingPolicyError(any()))
            .thenReturn(Optional.of(Status.ALREADY_EXISTS.asException()));

        settingsPoliciesService.createSettingsPolicy(inputPolicy);
    }

    @Test(expected = InvalidOperationException.class)
    public void testCreateInvalidPolicy() throws Exception {
        when(settingBackend.searchSettingSpecs(SearchSettingSpecsRequest.newBuilder()
                .addSettingSpecName(SETTING_NAME)
                .build()))
                .thenReturn(Collections.singletonList(SETTING_SPEC));

        when(settingsMapper.convertInputPolicy(eq(inputPolicy), eq(ImmutableMap.of(SETTING_NAME,
                SETTING_SPEC))))
                .thenReturn(DEFAULT_POLICY_INFO);

        when(settingPolicyBackend.createSettingPolicyError(any()))
                .thenReturn(Optional.of(Status.INVALID_ARGUMENT.asException()));

        settingsPoliciesService.createSettingsPolicy(inputPolicy);
    }

    @Test(expected = InvalidOperationException.class)
    public void testCreatePolicySpecsNotFound() throws Exception {
        when(settingBackend.searchSettingSpecs(any())).thenReturn(Collections.emptyList());

        settingsPoliciesService.createSettingsPolicy(inputPolicy);
    }

    private static class TestSettingService extends SettingServiceImplBase {
        public List<SettingSpec> searchSettingSpecs(SearchSettingSpecsRequest request) {
            return Collections.emptyList();
        }

        public void searchSettingSpecs(SearchSettingSpecsRequest request,
                                       StreamObserver<SettingSpec> responseObserver) {
            searchSettingSpecs(request).forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        }
    }

    private static class TestSettingPolicyService extends SettingPolicyServiceImplBase {
        public List<SettingPolicy> listSettingPolicies(ListSettingPoliciesRequest request) {
            return Collections.emptyList();
        }

        public CreateSettingPolicyResponse createSettingPolicy(CreateSettingPolicyRequest request) {
            return CreateSettingPolicyResponse.getDefaultInstance();
        }

        public Optional<Throwable> createSettingPolicyError(CreateSettingPolicyRequest request) {
            return Optional.empty();
        }

        public void listSettingPolicies(ListSettingPoliciesRequest request,
                                        StreamObserver<SettingPolicy> responseObserver) {
            listSettingPolicies(request).forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        }

        public void createSettingPolicy(CreateSettingPolicyRequest request,
                                        StreamObserver<CreateSettingPolicyResponse> responseObserver) {
            Optional<Throwable> error = createSettingPolicyError(request);
            if (error.isPresent()) {
                responseObserver.onError(error.get());
            } else {
                responseObserver.onNext(createSettingPolicy(request));
                responseObserver.onCompleted();
            }
        }
    }

    private static class TestGroupService extends GroupServiceImplBase {
        public List<Group> getGroups(GetGroupsRequest request) {
            return Collections.emptyList();
        }

        public void getGroups(GetGroupsRequest request,
                              StreamObserver<Group> responseObserver) {
            getGroups(request).forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        }
    }
}
