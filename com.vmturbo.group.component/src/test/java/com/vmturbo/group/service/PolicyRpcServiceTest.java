package com.vmturbo.group.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;

import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyDeleteRequest;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyEditResponse;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.ImmutableUpdateException.ImmutablePolicyUpdateException;
import com.vmturbo.group.common.ItemNotFoundException.PolicyNotFoundException;
import com.vmturbo.group.policy.PolicyStore;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class PolicyRpcServiceTest {

    @Mock
    private PolicyStore policyStore;

    private PolicyRpcService policyRpcService;

    @Before
    public void setUp() throws Exception {
        policyRpcService = new PolicyRpcService(policyStore);
    }

    @Test
    public void testCreateEmptyRequest() {
        final PolicyDTO.PolicyCreateRequest emptyCreateReq = PolicyDTO.PolicyCreateRequest.getDefaultInstance();

        final StreamObserver<PolicyDTO.PolicyCreateResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyCreateResponse>)Mockito.mock(StreamObserver.class);

        policyRpcService.createPolicy(emptyCreateReq, mockObserver);

        Mockito.verify(mockObserver).onError(Matchers.any(IllegalArgumentException.class));
    }

    @Test
    public void testCreate() throws DuplicateNameException {
        final long id = 1234L;

        final PolicyDTO.PolicyInfo inputPolicy = PolicyDTO.PolicyInfo.newBuilder()
                .setName("Test Input Policy")
                .build();
        final PolicyDTO.PolicyCreateRequest createReq = PolicyDTO.PolicyCreateRequest.newBuilder()
                .setPolicyInfo(inputPolicy)
                .build();
        final StreamObserver<PolicyDTO.PolicyCreateResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyCreateResponse>)Mockito.mock(StreamObserver.class);

        final Policy createdPolicy = Policy.newBuilder()
                .setId(id)
                .setPolicyInfo(inputPolicy)
                .build();

        when(policyStore.newUserPolicy(inputPolicy)).thenReturn(createdPolicy);

        policyRpcService.createPolicy(createReq, mockObserver);

        verify(policyStore).newUserPolicy(inputPolicy);
        verify(mockObserver).onNext(PolicyDTO.PolicyCreateResponse.newBuilder()
                .setPolicy(createdPolicy)
                .build());
        verify(mockObserver).onCompleted();
    }

    @Test
    public void testCreatePolicyInvalidArgument() {
        final PolicyDTO.PolicyCreateRequest createReq = PolicyDTO.PolicyCreateRequest.newBuilder()
                .build();
        final StreamObserver<PolicyDTO.PolicyCreateResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyCreateResponse>)Mockito.mock(StreamObserver.class);

        policyRpcService.createPolicy(createReq, mockObserver);

        final ArgumentCaptor<StatusException> errorCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(errorCaptor.capture());
        assertThat(errorCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .anyDescription());
    }

    @Test
    public void testCreatePolicyDuplicateName() throws DuplicateNameException {
        final PolicyDTO.PolicyInfo inputPolicy = PolicyDTO.PolicyInfo.newBuilder()
                .setName("Test Input Policy")
                .build();
        final PolicyDTO.PolicyCreateRequest createReq = PolicyDTO.PolicyCreateRequest.newBuilder()
                .setPolicyInfo(inputPolicy)
                .build();
        final StreamObserver<PolicyDTO.PolicyCreateResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyCreateResponse>)Mockito.mock(StreamObserver.class);


        when(policyStore.newUserPolicy(inputPolicy)).thenThrow(new DuplicateNameException(1L, inputPolicy.getName()));

        policyRpcService.createPolicy(createReq, mockObserver);


        final ArgumentCaptor<StatusException> errorCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(errorCaptor.capture());
        assertThat(errorCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains(inputPolicy.getName()));
    }

    @Test
    public void testEditPolicy() throws PolicyNotFoundException, DuplicateNameException {
        final long id = 1234L;
        final PolicyDTO.PolicyInfo newPolicyInfo = PolicyDTO.PolicyInfo.newBuilder()
                .setName("Test Policy")
                .build();
        final PolicyDTO.PolicyEditRequest editRequest = PolicyDTO.PolicyEditRequest.newBuilder()
                .setPolicyId(id)
                .setNewPolicyInfo(newPolicyInfo)
                .build();

        final StreamObserver<PolicyDTO.PolicyEditResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyEditResponse>)Mockito.mock(StreamObserver.class);

        final Policy updatedPolicy = Policy.newBuilder()
                .setId(id)
                .setPolicyInfo(newPolicyInfo)
                .build();

        when(policyStore.editPolicy(id, newPolicyInfo)).thenReturn(updatedPolicy);

        policyRpcService.editPolicy(editRequest, mockObserver);

        verify(mockObserver).onNext(PolicyEditResponse.newBuilder()
                .setPolicy(updatedPolicy)
                .build());
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }

    @Test
    public void testEditNonExistingPolicy() throws PolicyNotFoundException, DuplicateNameException {
        final long id = 1234L;
        final PolicyDTO.PolicyInfo inputPolicy = PolicyDTO.PolicyInfo.newBuilder()
                .setName("Test Input Policy")
                .build();
        final PolicyDTO.PolicyEditRequest editRequest = PolicyDTO.PolicyEditRequest.newBuilder()
                .setPolicyId(id)
                .setNewPolicyInfo(inputPolicy)
                .build();

        final StreamObserver<PolicyDTO.PolicyEditResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyEditResponse>)Mockito.mock(StreamObserver.class);

        when(policyStore.editPolicy(id, inputPolicy)).thenThrow(new PolicyNotFoundException(id));

        policyRpcService.editPolicy(editRequest, mockObserver);


        final ArgumentCaptor<StatusException> errorCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(errorCaptor.capture());
        assertThat(errorCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains(Long.toString(id)));
    }

    @Test
    public void testEditDuplicateNamePolicy() throws PolicyNotFoundException, DuplicateNameException {
        final long id = 1234L;
        final PolicyDTO.PolicyInfo inputPolicy = PolicyDTO.PolicyInfo.newBuilder()
                .setName("Test Input Policy")
                .build();
        final PolicyDTO.PolicyEditRequest editRequest = PolicyDTO.PolicyEditRequest.newBuilder()
                .setPolicyId(id)
                .setNewPolicyInfo(inputPolicy)
                .build();

        final StreamObserver<PolicyDTO.PolicyEditResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyEditResponse>)Mockito.mock(StreamObserver.class);

        when(policyStore.editPolicy(id, inputPolicy)).thenThrow(new DuplicateNameException(1L, inputPolicy.getName()));

        policyRpcService.editPolicy(editRequest, mockObserver);


        final ArgumentCaptor<StatusException> errorCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(errorCaptor.capture());
        assertThat(errorCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains(inputPolicy.getName()));
    }

    @Test
    public void testEditPolicyNoId() {
        final PolicyDTO.PolicyInfo inputPolicy = PolicyDTO.PolicyInfo.newBuilder()
                .setName("Test Input Policy")
                .build();
        final PolicyDTO.PolicyEditRequest editRequest = PolicyDTO.PolicyEditRequest.newBuilder()
                .setNewPolicyInfo(inputPolicy)
                .build();

        final StreamObserver<PolicyDTO.PolicyEditResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyEditResponse>)Mockito.mock(StreamObserver.class);

        policyRpcService.editPolicy(editRequest, mockObserver);


        final ArgumentCaptor<StatusException> errorCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(errorCaptor.capture());
        assertThat(errorCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .anyDescription());
    }

    @Test
    public void testEditPolicyNoNewInfo() {
        final long id = 1234L;
        final PolicyDTO.PolicyEditRequest editRequest = PolicyDTO.PolicyEditRequest.newBuilder()
                .setPolicyId(id)
                .build();

        final StreamObserver<PolicyDTO.PolicyEditResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyEditResponse>)Mockito.mock(StreamObserver.class);

        policyRpcService.editPolicy(editRequest, mockObserver);


        final ArgumentCaptor<StatusException> errorCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(errorCaptor.capture());
        assertThat(errorCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .anyDescription());
    }

    @Test
    public void testDeleteEmptyReq() {
        final PolicyDeleteRequest deleteRequest = PolicyDeleteRequest.newBuilder()
                .build();

        final StreamObserver<PolicyDTO.PolicyDeleteResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyDeleteResponse>)Mockito.mock(StreamObserver.class);

        policyRpcService.deletePolicy(deleteRequest, mockObserver);


        final ArgumentCaptor<StatusException> errorCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(errorCaptor.capture());
        verify(mockObserver, never()).onCompleted();
        assertThat(errorCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .anyDescription());
    }

    @Test
    public void testDelete() throws ImmutablePolicyUpdateException, PolicyNotFoundException {
        final long idToDelete = 1234L;
        final PolicyDTO.PolicyDeleteRequest emptyReq = PolicyDTO.PolicyDeleteRequest.newBuilder()
                .setPolicyId(idToDelete)
                .build();

        final StreamObserver<PolicyDTO.PolicyDeleteResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyDeleteResponse>)Mockito.mock(StreamObserver.class);

        final Policy deletedPolicy = Policy.newBuilder()
                .setId(idToDelete)
                .build();

        when(policyStore.deleteUserPolicy(idToDelete)).thenReturn(deletedPolicy);

        policyRpcService.deletePolicy(emptyReq, mockObserver);

        verify(mockObserver).onNext(PolicyDTO.PolicyDeleteResponse.newBuilder()
                .setPolicy(deletedPolicy)
                .build());
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }

    @Test
    public void testDeleteImmutablePolicy() throws ImmutablePolicyUpdateException, PolicyNotFoundException {
        final long idToDelete = 1234L;
        final PolicyDTO.PolicyDeleteRequest deleteReq = PolicyDTO.PolicyDeleteRequest.newBuilder()
                .setPolicyId(idToDelete)
                .build();


        final StreamObserver<PolicyDTO.PolicyDeleteResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyDeleteResponse>)Mockito.mock(StreamObserver.class);

        final String policyName = "FooPolicy";
        when(policyStore.deleteUserPolicy(idToDelete))
            .thenThrow(new ImmutablePolicyUpdateException(policyName));

        policyRpcService.deletePolicy(deleteReq, mockObserver);

        final ArgumentCaptor<StatusException> errorCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(errorCaptor.capture());
        verify(mockObserver, never()).onCompleted();
        assertThat(errorCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains(policyName));
    }

    @Test
    public void testDeletePolicyNotFound() throws ImmutablePolicyUpdateException, PolicyNotFoundException {
        final long idToDelete = 1234L;
        final PolicyDTO.PolicyDeleteRequest deleteReq = PolicyDTO.PolicyDeleteRequest.newBuilder()
                .setPolicyId(idToDelete)
                .build();


        final StreamObserver<PolicyDTO.PolicyDeleteResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyDeleteResponse>)Mockito.mock(StreamObserver.class);

        when(policyStore.deleteUserPolicy(idToDelete)).thenThrow(new PolicyNotFoundException(idToDelete));

        policyRpcService.deletePolicy(deleteReq, mockObserver);

        final ArgumentCaptor<StatusException> errorCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(errorCaptor.capture());
        verify(mockObserver, never()).onCompleted();
        assertThat(errorCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains(Long.toString(idToDelete)));
    }

    @Test
    public void testGetEmptyRequest() {
        final PolicyDTO.PolicyRequest emptyReq = PolicyDTO.PolicyRequest.getDefaultInstance();

        final StreamObserver<PolicyDTO.PolicyResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyResponse>)Mockito.mock(StreamObserver.class);

        policyRpcService.getPolicy(emptyReq, mockObserver);

        final ArgumentCaptor<StatusException> errorCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(errorCaptor.capture());
        verify(mockObserver, never()).onCompleted();
        assertThat(errorCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .anyDescription());
    }

    @Test
    public void testGetPolicy() throws Exception {
        final long policyIdToGet = 1234L;
        final PolicyDTO.PolicyRequest policyRequest = PolicyDTO.PolicyRequest.newBuilder()
                .setPolicyId(policyIdToGet)
                .build();

        final StreamObserver<PolicyDTO.PolicyResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyResponse>)Mockito.mock(StreamObserver.class);

        final PolicyDTO.Policy policy = PolicyDTO.Policy.newBuilder()
                .setId(policyIdToGet)
                .build();

        given(policyStore.get(policyIdToGet)).willReturn(Optional.of(policy));

        policyRpcService.getPolicy(policyRequest, mockObserver);

        verify(mockObserver).onNext(eq(PolicyResponse.newBuilder()
                .setPolicy(policy)
                .build()));
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

        policyRpcService.getPolicy(policyRequest, mockObserver);

        verify(mockObserver).onNext(PolicyResponse.newBuilder()
                .build());
        verify(mockObserver, never()).onError(any());
    }

    @Test
    public void testGetAllPolicies() {
        final PolicyDTO.PolicyRequest request = PolicyDTO.PolicyRequest.getDefaultInstance();
        final Collection<PolicyDTO.Policy> testPolicies = ImmutableList.of(
                PolicyDTO.Policy.newBuilder().setId(1L).build(),
                PolicyDTO.Policy.newBuilder().setId(2L).build(),
                PolicyDTO.Policy.newBuilder().setId(3L).build(),
                PolicyDTO.Policy.newBuilder().setId(4L).build(),
                PolicyDTO.Policy.newBuilder().setId(5L).build()
        );

        final StreamObserver<PolicyDTO.PolicyResponse> mockObserver =
                (StreamObserver<PolicyDTO.PolicyResponse>)Mockito.mock(StreamObserver.class);

        when(policyStore.getAll()).thenReturn(testPolicies);

        policyRpcService.getAllPolicies(request, mockObserver);

        verify(mockObserver, times(testPolicies.size())).onNext(any(PolicyDTO.PolicyResponse.class));
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }
}
