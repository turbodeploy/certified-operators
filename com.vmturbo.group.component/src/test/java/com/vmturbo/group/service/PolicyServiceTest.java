package com.vmturbo.group.service;

import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collection;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.persistent.GroupStore;
import com.vmturbo.group.persistent.PolicyStore;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class PolicyServiceTest {

    @Mock
    private PolicyStore policyStore;

    @Mock
    private GroupStore groupStore;

    @Mock
    private IdentityProvider identityProvider;

    private PolicyService policyService;

    @Before
    public void setUp() throws Exception {
        policyService = new PolicyService(policyStore, groupStore, identityProvider);
    }

    @Test
    public void testCreateEmptyRequest() {
        final PolicyDTO.PolicyCreateRequest emptyCreateReq = PolicyDTO.PolicyCreateRequest.getDefaultInstance();

        final StreamObserver<PolicyDTO.PolicyCreateResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyCreateResponse>)Mockito.mock(StreamObserver.class);

        policyService.createPolicy(emptyCreateReq, mockObserver);

        Mockito.verify(mockObserver).onError(Matchers.any(IllegalArgumentException.class));
    }

    @Test
    public void testCreate() {
        final long id = 1234L;

        final PolicyDTO.InputPolicy inputPolicy = PolicyDTO.InputPolicy.newBuilder()
                .setId(id)
                .setName("Test Input Policy")
                .build();
        final PolicyDTO.PolicyCreateRequest createReq = PolicyDTO.PolicyCreateRequest.newBuilder()
                .setInputPolicy(inputPolicy)
                .build();
        final StreamObserver<PolicyDTO.PolicyCreateResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyCreateResponse>)Mockito.mock(StreamObserver.class);

        given(identityProvider.next()).willReturn(id);
        given(policyStore.save(id, inputPolicy)).willReturn(true);

        policyService.createPolicy(createReq, mockObserver);

        verify(policyStore).save(id, inputPolicy);
        verify(mockObserver).onNext(PolicyDTO.PolicyCreateResponse.newBuilder()
                .setPolicyId(id).build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testCreateStoreFail() {
        final long id = 1234L;
        final PolicyDTO.InputPolicy inputPolicy = PolicyDTO.InputPolicy.newBuilder()
                .setId(id)
                .setName("Test Input Policy")
                .build();
        final PolicyDTO.PolicyCreateRequest createReq = PolicyDTO.PolicyCreateRequest.newBuilder()
                .setInputPolicy(inputPolicy)
                .build();
        final StreamObserver<PolicyDTO.PolicyCreateResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyCreateResponse>)Mockito.mock(StreamObserver.class);

        given(identityProvider.next()).willReturn(id);
        given(policyStore.save(id, inputPolicy)).willReturn(false);

        policyService.createPolicy(createReq, mockObserver);

        verify(policyStore).save(id, inputPolicy);
        verify(mockObserver).onError(any(IllegalStateException.class));
    }

    @Test
    public void testCreateWithException() {
        final long id = 1234L;
        final PolicyDTO.InputPolicy inputPolicy = PolicyDTO.InputPolicy.newBuilder()
                .setId(id)
                .setName("Test Input Policy")
                .build();
        final PolicyDTO.PolicyCreateRequest createReq = PolicyDTO.PolicyCreateRequest.newBuilder()
                .setInputPolicy(inputPolicy)
                .build();
        final StreamObserver<PolicyDTO.PolicyCreateResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyCreateResponse>)Mockito.mock(StreamObserver.class);
        final RuntimeException excToThrow = new RuntimeException("Mock exception in PolicyService");

        given(identityProvider.next()).willReturn(id);
        given(policyStore.save(id, inputPolicy)).willThrow(excToThrow);

        policyService.createPolicy(createReq, mockObserver);

        verify(mockObserver).onError(any(StatusRuntimeException.class));
    }

    @Test
    public void testEditMissingId() {
        final PolicyDTO.PolicyEditRequest editRequest = PolicyDTO.PolicyEditRequest.newBuilder()
                .setInputPolicy(PolicyDTO.InputPolicy.getDefaultInstance())
                .build();

        final StreamObserver<PolicyDTO.PolicyEditResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyEditResponse>)Mockito.mock(StreamObserver.class);

        policyService.editPolicy(editRequest, mockObserver);

        verify(mockObserver, never()).onCompleted();
        verify(mockObserver).onError(any(IllegalArgumentException.class));
    }

    @Test
    public void testEditMissingInputPolicy() {
        final PolicyDTO.PolicyEditRequest editRequest = PolicyDTO.PolicyEditRequest.newBuilder()
                .setPolicyId(1234L)
                .build();

        final StreamObserver<PolicyDTO.PolicyEditResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyEditResponse>)Mockito.mock(StreamObserver.class);

        policyService.editPolicy(editRequest, mockObserver);

        verify(mockObserver, never()).onCompleted();
        verify(mockObserver).onError(any(IllegalArgumentException.class));
    }

    @Test
    public void testEditPolicy() {
        final long id = 1234L;
        final PolicyDTO.InputPolicy testInputPolicy = PolicyDTO.InputPolicy.newBuilder()
                .setId(id)
                .setName("Test Policy")
                .build();
        final PolicyDTO.PolicyEditRequest editRequest = PolicyDTO.PolicyEditRequest.newBuilder()
                .setPolicyId(id)
                .setInputPolicy(testInputPolicy)
                .build();

        final StreamObserver<PolicyDTO.PolicyEditResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyEditResponse>)Mockito.mock(StreamObserver.class);

        given(policyStore.get(id)).willReturn(Optional.empty());
        given(policyStore.save(id, testInputPolicy)).willReturn(true);
        policyService.editPolicy(editRequest, mockObserver);

        verify(policyStore).get(id);
        verify(mockObserver).onNext(PolicyDTO.PolicyEditResponse.getDefaultInstance());
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }

    @Test
    public void testEditPolicyWithCommodityType() {
        final long id = 1234L;
        final PolicyDTO.InputPolicy oldInputPolicy = PolicyDTO.InputPolicy.newBuilder()
                .setId(id)
                .setName("Test Policy")
                .setCommodityType("DrsSegmentationCommodity")
                .setEnabled(true)
                .build();
        final PolicyDTO.InputPolicy newInputPolicy = PolicyDTO.InputPolicy.newBuilder()
                .setId(id)
                .setName("Test Policy")
                .setEnabled(false)
                .build();
        final PolicyDTO.InputPolicy finalInputPolicy = PolicyDTO.InputPolicy.newBuilder()
                .setId(id)
                .setName("Test Policy")
                .setCommodityType("DrsSegmentationCommodity")
                .setEnabled(false)
                .build();
        final PolicyDTO.PolicyEditRequest editRequest = PolicyDTO.PolicyEditRequest.newBuilder()
                .setPolicyId(id)
                .setInputPolicy(newInputPolicy)
                .build();

        final StreamObserver<PolicyDTO.PolicyEditResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyEditResponse>)Mockito.mock(StreamObserver.class);

        given(policyStore.get(id)).willReturn(Optional.of(oldInputPolicy));
        given(policyStore.save(id, finalInputPolicy)).willReturn(true);
        policyService.editPolicy(editRequest, mockObserver);

        verify(policyStore, Mockito.times(1)).save(id, finalInputPolicy);
        verify(policyStore).get(id);
        verify(mockObserver).onNext(PolicyDTO.PolicyEditResponse.getDefaultInstance());
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }

    @Test
    public void testEditPolicyStoreFailed() {
        final long id = 1234L;
        final PolicyDTO.InputPolicy testInputPolicy = PolicyDTO.InputPolicy.newBuilder()
                .setId(id)
                .setName("Test Policy")
                .build();
        final PolicyDTO.PolicyEditRequest editRequest = PolicyDTO.PolicyEditRequest.newBuilder()
                .setPolicyId(id)
                .setInputPolicy(testInputPolicy)
                .build();

        final StreamObserver<PolicyDTO.PolicyEditResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyEditResponse>)Mockito.mock(StreamObserver.class);

        given(policyStore.save(id, testInputPolicy)).willReturn(false);
        policyService.editPolicy(editRequest, mockObserver);

        verify(mockObserver, never()).onCompleted();
        verify(mockObserver, never()).onNext(any(PolicyDTO.PolicyEditResponse.class));
        verify(mockObserver).onError(any(IllegalStateException.class));
    }

    @Test
    public void testEditPolicyException() {
        final long id = 1234L;
        final PolicyDTO.InputPolicy testInputPolicy = PolicyDTO.InputPolicy.newBuilder()
                .setId(id)
                .setName("Test Policy")
                .build();
        final PolicyDTO.PolicyEditRequest editRequest = PolicyDTO.PolicyEditRequest.newBuilder()
                .setPolicyId(id)
                .setInputPolicy(testInputPolicy)
                .build();

        final StreamObserver<PolicyDTO.PolicyEditResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyEditResponse>)Mockito.mock(StreamObserver.class);

        final RuntimeException runtimeException = new RuntimeException("Mock exception for testEdit");
        given(policyStore.save(id, testInputPolicy)).willThrow(runtimeException);
        policyService.editPolicy(editRequest, mockObserver);

        verify(mockObserver, never()).onNext(any(PolicyDTO.PolicyEditResponse.class));
        verify(mockObserver, never()).onCompleted();
        verify(mockObserver).onError(any(StatusRuntimeException.class));
    }

    @Test
    public void testDeleteEmptyReq() {
        final PolicyDTO.PolicyDeleteRequest emptyReq = PolicyDTO.PolicyDeleteRequest.getDefaultInstance();

        final StreamObserver<PolicyDTO.PolicyDeleteResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyDeleteResponse>)Mockito.mock(StreamObserver.class);

        policyService.deletePolicy(emptyReq, mockObserver);

        verify(mockObserver).onError(any(IllegalArgumentException.class));
        verify(mockObserver, never()).onCompleted();
    }

    @Test
    public void testDelete() {
        final long idToDelete = 1234L;
        final PolicyDTO.PolicyDeleteRequest emptyReq = PolicyDTO.PolicyDeleteRequest.newBuilder()
                .setPolicyId(idToDelete)
                .build();

        final StreamObserver<PolicyDTO.PolicyDeleteResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyDeleteResponse>)Mockito.mock(StreamObserver.class);

        given(policyStore.delete(idToDelete)).willReturn(true);

        policyService.deletePolicy(emptyReq, mockObserver);

        verify(mockObserver).onNext(PolicyDTO.PolicyDeleteResponse.getDefaultInstance());
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }

    @Test
    public void testDeleteFail() {
        final long idToDelete = 1234L;
        final PolicyDTO.PolicyDeleteRequest emptyReq = PolicyDTO.PolicyDeleteRequest.newBuilder()
                .setPolicyId(idToDelete)
                .build();

        final StreamObserver<PolicyDTO.PolicyDeleteResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyDeleteResponse>)Mockito.mock(StreamObserver.class);

        given(policyStore.delete(idToDelete)).willReturn(false);

        policyService.deletePolicy(emptyReq, mockObserver);

        verify(mockObserver).onError(any(IllegalStateException.class));
        verify(mockObserver, never()).onNext(PolicyDTO.PolicyDeleteResponse.getDefaultInstance());
        verify(mockObserver, never()).onCompleted();
    }

    @Test
    public void testDeleteException() {
        final long idToDelete = 1234L;
        final PolicyDTO.PolicyDeleteRequest emptyReq = PolicyDTO.PolicyDeleteRequest.newBuilder()
                .setPolicyId(idToDelete)
                .build();

        final StreamObserver<PolicyDTO.PolicyDeleteResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyDeleteResponse>)Mockito.mock(StreamObserver.class);

        final RuntimeException exc = new RuntimeException("Mock exception in delete");
        given(policyStore.delete(idToDelete)).willThrow(exc);

        policyService.deletePolicy(emptyReq, mockObserver);

        verify(mockObserver).onError(any(StatusRuntimeException.class));
        verify(mockObserver, never()).onCompleted();
    }

    @Test
    public void testGetEmptyRequest() {
        final PolicyDTO.PolicyRequest emptyReq = PolicyDTO.PolicyRequest.getDefaultInstance();

        final StreamObserver<PolicyDTO.PolicyResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyResponse>)Mockito.mock(StreamObserver.class);

        policyService.getPolicy(emptyReq, mockObserver);

        verify(mockObserver).onError(any(IllegalArgumentException.class));
    }

    @Test
    public void testGetPolicy() throws Exception {
        final long policyIdToGet = 1234L;
        final long consumerGroupId = 1L;
        final long providerGroupId = 2L;
        final PolicyDTO.PolicyRequest policyRequest = PolicyDTO.PolicyRequest.newBuilder()
                .setPolicyId(policyIdToGet).build();

        final StreamObserver<PolicyDTO.PolicyResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyResponse>)Mockito.mock(StreamObserver.class);

        final PolicyDTO.InputPolicy mockInputPolicy = PolicyDTO.InputPolicy.newBuilder()
                .setBindToGroup(PolicyDTO.InputPolicy.BindToGroupPolicy.newBuilder()
                        .setConsumerGroup(consumerGroupId)
                        .setProviderGroup(providerGroupId)
                        .build())
                .build();

        final GroupDTO.Group mockConsumerGroup = GroupDTO.Group.newBuilder().setId(consumerGroupId).build();
        final GroupDTO.Group mockProviderGroup = GroupDTO.Group.newBuilder().setId(providerGroupId).build();

        given(policyStore.get(policyIdToGet)).willReturn(Optional.of(mockInputPolicy));
        given(groupStore.get(providerGroupId)).willReturn(Optional.of(mockProviderGroup));
        given(groupStore.get(consumerGroupId)).willReturn(Optional.of(mockConsumerGroup));

        policyService.getPolicy(policyRequest, mockObserver);

        verify(mockObserver).onNext(any(PolicyDTO.PolicyResponse.class));
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }

    @Test
    public void testGetMissingPolicy() {
        final long policyIdToGet = 1234L;
        final PolicyDTO.PolicyRequest policyRequest = PolicyDTO.PolicyRequest.newBuilder()
                .setPolicyId(policyIdToGet).build();

        final StreamObserver<PolicyDTO.PolicyResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyResponse>)Mockito.mock(StreamObserver.class);

        given(policyStore.get(policyIdToGet)).willReturn(Optional.empty());

        policyService.getPolicy(policyRequest, mockObserver);

        verify(mockObserver).onError(any(IllegalArgumentException.class));
        verify(mockObserver, never()).onNext(any(PolicyDTO.PolicyResponse.class));
        verify(mockObserver, never()).onCompleted();
    }

    @Test
    public void testGetPolicyException() {
        final long policyIdToGet = 1234L;
        final PolicyDTO.PolicyRequest policyRequest = PolicyDTO.PolicyRequest.newBuilder()
                .setPolicyId(policyIdToGet).build();

        final StreamObserver<PolicyDTO.PolicyResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyResponse>)Mockito.mock(StreamObserver.class);

        final RuntimeException runtimeException = new RuntimeException("Mock exception in get policy");
        given(policyStore.get(policyIdToGet)).willThrow(runtimeException);

        policyService.getPolicy(policyRequest, mockObserver);

        verify(mockObserver).onError(any(StatusRuntimeException.class));
        verify(mockObserver, never()).onNext(any(PolicyDTO.PolicyResponse.class));
        verify(mockObserver, never()).onCompleted();
    }

    @Test
    public void testGetAllPolicies() {
        final PolicyDTO.PolicyRequest request = PolicyDTO.PolicyRequest.getDefaultInstance();
        final Collection<PolicyDTO.InputPolicy> testPolicies = ImmutableList.of(
                PolicyDTO.InputPolicy.newBuilder().setId(1L).build(),
                PolicyDTO.InputPolicy.newBuilder().setId(2L).build(),
                PolicyDTO.InputPolicy.newBuilder().setId(3L).build(),
                PolicyDTO.InputPolicy.newBuilder().setId(4L).build(),
                PolicyDTO.InputPolicy.newBuilder().setId(5L).build()
        );

        final StreamObserver<PolicyDTO.PolicyResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyResponse>)Mockito.mock(StreamObserver.class);

        given(policyStore.getAll()).willReturn(testPolicies);

        policyService.getAllPolicies(request, mockObserver);

        verify(mockObserver, times(testPolicies.size())).onNext(any(PolicyDTO.PolicyResponse.class));
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }

    @Test
    public void testGetAllPoliciesEmpty() {
        final PolicyDTO.PolicyRequest request = PolicyDTO.PolicyRequest.getDefaultInstance();
        final Collection<PolicyDTO.InputPolicy> testPolicies = ImmutableList.of();

        final StreamObserver<PolicyDTO.PolicyResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyResponse>)Mockito.mock(StreamObserver.class);

        given(policyStore.getAll()).willReturn(testPolicies);

        policyService.getAllPolicies(request, mockObserver);

        verify(mockObserver, never()).onNext(any(PolicyDTO.PolicyResponse.class));
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }

    @Test
    public void testGetAllPoliciesWithException() {
        final PolicyDTO.PolicyRequest request = PolicyDTO.PolicyRequest.getDefaultInstance();

        final StreamObserver<PolicyDTO.PolicyResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyResponse>)Mockito.mock(StreamObserver.class);

        final RuntimeException runtimeException = new RuntimeException("Mock exception for GetAllPolicies");
        given(policyStore.getAll()).willThrow(runtimeException);

        policyService.getAllPolicies(request, mockObserver);

        verify(mockObserver, never()).onCompleted();
        verify(mockObserver, never()).onNext(any(PolicyDTO.PolicyResponse.class));
        verify(mockObserver).onError(any(StatusRuntimeException.class));
    }
}
