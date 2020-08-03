package com.vmturbo.group.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;

import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyDeleteRequest;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyEditResponse;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.AtMostNPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MergePolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MergePolicy.MergeType;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.ImmutableUpdateException.ImmutablePolicyUpdateException;
import com.vmturbo.group.common.ItemNotFoundException.PolicyNotFoundException;
import com.vmturbo.group.group.GroupDAO;
import com.vmturbo.group.policy.PolicyStore;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class PolicyRpcServiceTest {

    @Mock
    private PolicyStore policyStore;

    private PolicyRpcService policyRpcService;

    private UserSessionContext userSessionContext = Mockito.mock(UserSessionContext.class);

    private GroupRpcService groupRpcService = Mockito.mock(GroupRpcService.class);

    private GroupDAO groupStore = Mockito.mock(GroupDAO.class);

    @Before
    public void setUp() throws Exception {
        policyRpcService = new PolicyRpcService(
            policyStore, groupRpcService, groupStore, userSessionContext);
    }

    @Test
    public void testCreateEmptyRequest() {
        final PolicyDTO.PolicyCreateRequest emptyCreateReq = PolicyDTO.PolicyCreateRequest.getDefaultInstance();

        final StreamObserver<PolicyDTO.PolicyCreateResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        policyRpcService.createPolicy(emptyCreateReq, mockObserver);

        Mockito.verify(mockObserver).onError(Matchers.any(IllegalArgumentException.class));
    }

    @Test
    public void testCreate() throws Exception {
        final long id = 1234L;

        final PolicyDTO.PolicyInfo inputPolicy = PolicyDTO.PolicyInfo.newBuilder()
                .setName("Test Input Policy")
                .build();
        final PolicyDTO.PolicyCreateRequest createReq = PolicyDTO.PolicyCreateRequest.newBuilder()
                .setPolicyInfo(inputPolicy)
                .build();
        final StreamObserver<PolicyDTO.PolicyCreateResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

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
                Mockito.mock(StreamObserver.class);

        policyRpcService.createPolicy(createReq, mockObserver);

        final ArgumentCaptor<StatusException> errorCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(errorCaptor.capture());
        assertThat(errorCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .anyDescription());
    }

    @Test
    public void testCreatePolicyDuplicateName() throws Exception {
        final PolicyDTO.PolicyInfo inputPolicy = PolicyDTO.PolicyInfo.newBuilder()
                .setName("Test Input Policy")
                .build();
        final PolicyDTO.PolicyCreateRequest createReq = PolicyDTO.PolicyCreateRequest.newBuilder()
                .setPolicyInfo(inputPolicy)
                .build();
        final StreamObserver<PolicyDTO.PolicyCreateResponse> mockObserver =
                Mockito.mock(StreamObserver.class);


        when(policyStore.newUserPolicy(inputPolicy)).thenThrow(new DuplicateNameException(1L, inputPolicy.getName()));

        policyRpcService.createPolicy(createReq, mockObserver);


        final ArgumentCaptor<StatusException> errorCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(errorCaptor.capture());
        assertThat(errorCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains(inputPolicy.getName()));
    }

    @Test
    public void testEditPolicy() throws Exception {
        final long id = 1234L;
        final PolicyDTO.PolicyInfo newPolicyInfo = PolicyDTO.PolicyInfo.newBuilder()
                .setName("Test Policy")
                .build();
        final PolicyDTO.PolicyEditRequest editRequest = PolicyDTO.PolicyEditRequest.newBuilder()
                .setPolicyId(id)
                .setNewPolicyInfo(newPolicyInfo)
                .build();

        final StreamObserver<PolicyDTO.PolicyEditResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

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
    public void testEditNonExistingPolicy() throws Exception {
        final long id = 1234L;
        final PolicyDTO.PolicyInfo inputPolicy = PolicyDTO.PolicyInfo.newBuilder()
                .setName("Test Input Policy")
                .build();
        final PolicyDTO.PolicyEditRequest editRequest = PolicyDTO.PolicyEditRequest.newBuilder()
                .setPolicyId(id)
                .setNewPolicyInfo(inputPolicy)
                .build();

        final StreamObserver<PolicyDTO.PolicyEditResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        when(policyStore.editPolicy(id, inputPolicy)).thenThrow(new PolicyNotFoundException(id));

        policyRpcService.editPolicy(editRequest, mockObserver);


        final ArgumentCaptor<StatusException> errorCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(errorCaptor.capture());
        assertThat(errorCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains(Long.toString(id)));
    }

    @Test
    public void testEditDuplicateNamePolicy() throws Exception {
        final long id = 1234L;
        final PolicyDTO.PolicyInfo inputPolicy = PolicyDTO.PolicyInfo.newBuilder()
                .setName("Test Input Policy")
                .build();
        final PolicyDTO.PolicyEditRequest editRequest = PolicyDTO.PolicyEditRequest.newBuilder()
                .setPolicyId(id)
                .setNewPolicyInfo(inputPolicy)
                .build();

        final StreamObserver<PolicyDTO.PolicyEditResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

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
                Mockito.mock(StreamObserver.class);

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
                Mockito.mock(StreamObserver.class);

        policyRpcService.editPolicy(editRequest, mockObserver);


        final ArgumentCaptor<StatusException> errorCaptor = ArgumentCaptor.forClass(StatusException.class);
        verify(mockObserver).onError(errorCaptor.capture());
        assertThat(errorCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .anyDescription());
    }

    @Test(expected = UserAccessException.class)
    public void testEditPolicyCantAccessExisting() throws Exception {
        // verify that a scoped user trying to edit an existing policy they can't access will fail

        // this user has access to group 1 but not group 2
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(groupRpcService.userHasAccessToGrouping(groupStore, 1L)).thenReturn(true);
        when(groupRpcService.userHasAccessToGrouping(groupStore, 2L)).thenReturn(false);

        final long id = 1234L;
        final Optional<Policy> existingPolicy = Optional.of(Policy.newBuilder()
                .setId(id)
                .setPolicyInfo(PolicyInfo.newBuilder()
                        .setAtMostN(AtMostNPolicy.newBuilder()
                                .setConsumerGroupId(2)
                                .setProviderGroupId(2)))
                .build());

        when(policyStore.get(id)).thenReturn(existingPolicy);

        final PolicyDTO.PolicyInfo updatedPolicyInfo = PolicyDTO.PolicyInfo.newBuilder()
                .setName("Test Policy")
                .setAtMostN(AtMostNPolicy.newBuilder()
                        .setConsumerGroupId(1)
                        .setProviderGroupId(1))
                .build();
        final PolicyDTO.PolicyEditRequest editRequest = PolicyDTO.PolicyEditRequest.newBuilder()
                .setPolicyId(id)
                .setNewPolicyInfo(updatedPolicyInfo)
                .build();

        final StreamObserver<PolicyDTO.PolicyEditResponse> mockObserver =
                Mockito.mock(StreamObserver.class);
        policyRpcService.editPolicy(editRequest, mockObserver);
    }

    @Test(expected = UserAccessException.class)
    public void testEditPolicyChangesOutOfScope() throws Exception {
        // verify that a scoped user can't edit a policy to include groups out of scope

        // this user has access to group 1 but not group 2
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(groupRpcService.userHasAccessToGrouping(groupStore, 1L)).thenReturn(true);
        when(groupRpcService.userHasAccessToGrouping(groupStore, 2L)).thenReturn(false);

        final long id = 1234L;
        final Optional<Policy> existingPolicy = Optional.of(Policy.newBuilder()
                .setId(id)
                .setPolicyInfo(PolicyInfo.newBuilder()
                        .setAtMostN(AtMostNPolicy.newBuilder()
                                .setConsumerGroupId(1)
                                .setProviderGroupId(1)))
                .build());

        when(policyStore.get(id)).thenReturn(existingPolicy);

        final PolicyDTO.PolicyInfo updatedPolicyInfo = PolicyDTO.PolicyInfo.newBuilder()
                .setName("Test Policy")
                .setAtMostN(AtMostNPolicy.newBuilder()
                        .setConsumerGroupId(2)
                        .setProviderGroupId(2))
                .build();
        final PolicyDTO.PolicyEditRequest editRequest = PolicyDTO.PolicyEditRequest.newBuilder()
                .setPolicyId(id)
                .setNewPolicyInfo(updatedPolicyInfo)
                .build();

        final StreamObserver<PolicyDTO.PolicyEditResponse> mockObserver =
                Mockito.mock(StreamObserver.class);
        policyRpcService.editPolicy(editRequest, mockObserver);
    }

    @Test
    public void testDeleteEmptyReq() {
        final PolicyDeleteRequest deleteRequest = PolicyDeleteRequest.newBuilder()
                .build();

        final StreamObserver<PolicyDTO.PolicyDeleteResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

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
                Mockito.mock(StreamObserver.class);

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
                Mockito.mock(StreamObserver.class);

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
                Mockito.mock(StreamObserver.class);

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
        final PolicyDTO.SinglePolicyRequest emptyReq = PolicyDTO.SinglePolicyRequest.getDefaultInstance();

        final StreamObserver<PolicyDTO.PolicyResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

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
        final PolicyDTO.SinglePolicyRequest policyRequest = PolicyDTO.SinglePolicyRequest.newBuilder()
                .setPolicyId(policyIdToGet)
                .build();

        final StreamObserver<PolicyDTO.PolicyResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

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
        final PolicyDTO.SinglePolicyRequest policyRequest = PolicyDTO.SinglePolicyRequest.newBuilder()
                .setPolicyId(policyIdToGet).build();

        final StreamObserver<PolicyDTO.PolicyResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        given(policyStore.get(policyIdToGet)).willReturn(Optional.empty());

        policyRpcService.getPolicy(policyRequest, mockObserver);

        verify(mockObserver).onNext(PolicyResponse.newBuilder()
                .build());
        verify(mockObserver, never()).onError(any());
    }

    @Test
    public void testGetPolicies() {
        final PolicyDTO.PolicyRequest request = PolicyDTO.PolicyRequest.getDefaultInstance();
        final Collection<PolicyDTO.Policy> testPolicies = ImmutableList.of(
                PolicyDTO.Policy.newBuilder().setId(1L).build(),
                PolicyDTO.Policy.newBuilder().setId(2L).build(),
                PolicyDTO.Policy.newBuilder().setId(3L).build(),
                PolicyDTO.Policy.newBuilder().setId(4L).build(),
                PolicyDTO.Policy.newBuilder().setId(5L).build()
        );

        final StreamObserver<PolicyDTO.PolicyResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        when(policyStore.getAll()).thenReturn(testPolicies);

        policyRpcService.getPolicies(request, mockObserver);

        verify(mockObserver, times(testPolicies.size())).onNext(any(PolicyDTO.PolicyResponse.class));
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());
    }

    @Test
    public void testGetPolicyForScopedUser() throws Exception {
        // user has access to group 1 but not group 2
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(groupRpcService.userHasAccessToGrouping(groupStore, 1L)).thenReturn(true);
        when(groupRpcService.userHasAccessToGrouping(groupStore, 2L)).thenReturn(false);

        final long policyIdInScope = 1;
        final PolicyDTO.Policy policyInScope = PolicyDTO.Policy.newBuilder()
                .setId(policyIdInScope)
                .setPolicyInfo(PolicyInfo.newBuilder()
                        .setAtMostN(AtMostNPolicy.newBuilder()
                                .setConsumerGroupId(1)
                                .setProviderGroupId(1)))
                .build();

        given(policyStore.get(policyIdInScope)).willReturn(Optional.of(policyInScope));

        final PolicyDTO.SinglePolicyRequest policyInScopeRequest = PolicyDTO.SinglePolicyRequest.newBuilder()
                .setPolicyId(policyIdInScope)
                .build();

        final StreamObserver<PolicyDTO.PolicyResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        policyRpcService.getPolicy(policyInScopeRequest, mockObserver);
        verify(mockObserver, times(1)).onNext(any());
        verify(mockObserver).onCompleted();
    }

    @Test(expected = UserAccessException.class)
    public void testGetPolicyForScopedUserFail() throws Exception {

        // user has access to group 1 but not group 2
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(groupRpcService.userHasAccessToGrouping(groupStore, 1L)).thenReturn(true);
        when(groupRpcService.userHasAccessToGrouping(groupStore, 2L)).thenReturn(false);

        final long policyIdNotInScope = 2;
        final PolicyDTO.Policy policyNotInScope = PolicyDTO.Policy.newBuilder()
                .setId(policyIdNotInScope)
                .setPolicyInfo(PolicyInfo.newBuilder()
                        .setAtMostN(AtMostNPolicy.newBuilder()
                                .setConsumerGroupId(2)
                                .setProviderGroupId(2)))
                .build();
        given(policyStore.get(policyIdNotInScope)).willReturn(Optional.of(policyNotInScope));

        final PolicyDTO.SinglePolicyRequest policyInScopeRequest = PolicyDTO.SinglePolicyRequest.newBuilder()
                .setPolicyId(policyIdNotInScope)
                .build();

        final StreamObserver<PolicyDTO.PolicyResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        // this should trigger an exception
        policyRpcService.getPolicy(policyInScopeRequest, mockObserver);
        verify(mockObserver).onError(any());
    }

    @Test
    public void testGetPoliciesForScopedUser() throws Exception {
        final PolicyDTO.PolicyRequest request = PolicyDTO.PolicyRequest.getDefaultInstance();
        final Collection<PolicyDTO.Policy> testPolicies = ImmutableList.of(
                Policy.newBuilder().setId(1L)
                        .setPolicyInfo(PolicyInfo.newBuilder()
                                .setAtMostN(AtMostNPolicy.newBuilder()
                                        .setConsumerGroupId(1)
                                        .setProviderGroupId(1)))
                        .build(),
                Policy.newBuilder().setId(2L) // unaccessible
                        .setPolicyInfo(PolicyInfo.newBuilder()
                                .setAtMostN(AtMostNPolicy.newBuilder()
                                        .setConsumerGroupId(1)
                                        .setProviderGroupId(2)))
                        .build(),
                Policy.newBuilder().setId(3L) // unaccessible
                        .setPolicyInfo(PolicyInfo.newBuilder()
                                .setAtMostN(AtMostNPolicy.newBuilder()
                                        .setConsumerGroupId(2)
                                        .setProviderGroupId(2)))
                        .build()
        );

        final StreamObserver<PolicyDTO.PolicyResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        when(policyStore.getAll()).thenReturn(testPolicies);
        // user has access to group 1 but not group 2
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(groupRpcService.userHasAccessToGrouping(groupStore, 1L)).thenReturn(true);
        when(groupRpcService.userHasAccessToGrouping(groupStore, 2L)).thenReturn(false);

        policyRpcService.getPolicies(request, mockObserver);
        // verify we have one result and it's for policy 1
        final ArgumentCaptor<PolicyResponse> policyCaptor = ArgumentCaptor.forClass(PolicyResponse.class);
        verify(mockObserver, times(1)).onNext(policyCaptor.capture());
        assertEquals(1L, policyCaptor.getValue().getPolicy().getId());
        verify(mockObserver).onCompleted();
        verify(mockObserver, never()).onError(any());

        // verify the cache is working by checking that the groupRpcService is only called once per group id
        verify(groupRpcService, times(1)).userHasAccessToGrouping(groupStore, 1L);
        verify(groupRpcService, times(1)).userHasAccessToGrouping(groupStore, 2L);
    }

    @Test
    public void testCreateByScopedUser() throws Exception {
        // this user has access to group 1 but not group 2
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(groupRpcService.userHasAccessToGrouping(groupStore, 1L)).thenReturn(true);
        when(groupRpcService.userHasAccessToGrouping(groupStore, 2L)).thenReturn(false);

        // verify that creating a policy for group 1 succeeds
        final PolicyDTO.PolicyInfo inputPolicy = PolicyDTO.PolicyInfo.newBuilder()
                .setName("Test Input Policy")
                .setAtMostN(AtMostNPolicy.newBuilder()
                        .setConsumerGroupId(1)
                        .setProviderGroupId(1))
                .build();
        final PolicyDTO.PolicyCreateRequest createReq = PolicyDTO.PolicyCreateRequest.newBuilder()
                .setPolicyInfo(inputPolicy)
                .build();
        final StreamObserver<PolicyDTO.PolicyCreateResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        final Policy createdPolicy = Policy.newBuilder()
                .setId(1L)
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

    @Test(expected =  UserAccessException.class)
    public void testCreateByScopedUserFail() throws Exception {
        // this user has access to group 1 but not group 2
        when(userSessionContext.isUserScoped()).thenReturn(true);
        when(groupRpcService.userHasAccessToGrouping(groupStore, 1L)).thenReturn(true);
        when(groupRpcService.userHasAccessToGrouping(groupStore, 2L)).thenReturn(false);

        // verify that creating a policy for group 2 will fail
        final PolicyDTO.PolicyInfo inputPolicy = PolicyDTO.PolicyInfo.newBuilder()
                .setName("Test Input Policy")
                .setAtMostN(AtMostNPolicy.newBuilder()
                        .setConsumerGroupId(2)
                        .setProviderGroupId(2))
                .build();
        final PolicyDTO.PolicyCreateRequest createReq = PolicyDTO.PolicyCreateRequest.newBuilder()
                .setPolicyInfo(inputPolicy)
                .build();
        final StreamObserver<PolicyDTO.PolicyCreateResponse> mockObserver =
                Mockito.mock(StreamObserver.class);

        policyRpcService.createPolicy(createReq, mockObserver);
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    // set create some clusters and policies for cluster merge policy testing
    final Grouping clusterA = clusterGroup("clusterA", 1L);
    final Grouping clusterB = clusterGroup("clusterB", 2L);
    final Grouping clusterC = clusterGroup("clusterC", 3L);
    final Grouping clusterD = clusterGroup("clusterD", 4L);
    final Policy mergeAB = clusterMergePolicy(10L, "mergeAB", clusterA, clusterB);
    final Policy mergeCD = clusterMergePolicy(20L, "mergeCD", clusterC, clusterD);

    private void setupClusterTestMocks() {
        when(groupStore.getGroupsById(Collections.singleton(1L))).thenReturn(
                Collections.singleton(clusterA));
        when(groupStore.getGroupsById(Collections.singleton(2L))).thenReturn(
                Collections.singleton(clusterB));
        when(groupStore.getGroupsById(Collections.singleton(3L))).thenReturn(
                Collections.singleton(clusterC));
        when(groupStore.getGroupsById(Collections.singleton(4L))).thenReturn(
                Collections.singleton(clusterD));
    }

    /**
     * Validate an attempt to create a new cluster merge policy in which one of the clusters to be
     * merged already appears in a cluster merge policy. This should fail with an
     * {@link IllegalArgumentException}
     */
    @Test
    public void testDisallowMergePolicyWithAlreadyUsedClusters() {
        expectedException.expect(IllegalArgumentException.class);
        setupClusterTestMocks();
        when(policyStore.getAll()).thenReturn(Collections.singletonList(mergeAB));

        // attempting to merge A and C
        policyRpcService.checkForInvalidClusterMergePolicy(
            clusterMergePolicy(20L,"mergeAC", clusterA, clusterC).getPolicyInfo(),
            Optional.empty()
        );
    }

    /**
     * Validate an attempt to create a new cluster merge policy in which none of the clusters to
     * be merged appears in an existing cluster merge policy. This should succeed without
     * throwing an exception.
     */
    @Test
    public void allowMergePolicyWithUnusedClusters() {

        setupClusterTestMocks();
        when(policyStore.getAll()).thenReturn(Collections.singletonList(mergeAB));

        // attempting to merge A and C
        policyRpcService.checkForInvalidClusterMergePolicy(
            clusterMergePolicy(20L,"mergeCD", clusterC, clusterD).getPolicyInfo(),
            Optional.empty()
        );
    }

    /**
     * Validate an attempt to update a cluster merge policy to include a cluster that already
     * appears in a different cluster merge policy. This should throw
     * {@link IllegalArgumentException}.
     */
    @Test
    public void disallowMergePolicyEditWithAlreadyUsedClusters() {
        expectedException.expect(IllegalArgumentException.class);
        setupClusterTestMocks();
        when(policyStore.getAll()).thenReturn(Arrays.asList(mergeAB, mergeCD));

        // attempting to add C to AB
        policyRpcService.checkForInvalidClusterMergePolicy(
            clusterMergePolicy(mergeAB.getId(),"mergeABC", clusterA, clusterB, clusterC)
                .getPolicyInfo(),
            Optional.of(mergeAB.getId()));
    }

    /**
     * Validate an attempt to update a cluster merge policy to include additional clusters that
     * do not appear in other cluster merge policies. The existing clusters are retained, so the
     * point here is that those clusters should not disqualify this operation; only overlap with
     * a <em>different</em> policy than the one being edited should cause problems. This test
     * should succeed without raising an exception.
     */
    @Test
    public void allowMergePolicyEditWithOwnUsedClusters() {
        setupClusterTestMocks();
        when(policyStore.getAll()).thenReturn(Collections.singletonList(mergeAB));

        // attempting to add C to AB
        policyRpcService.checkForInvalidClusterMergePolicy(
            clusterMergePolicy(mergeAB.getId(),"mergeABC", clusterA, clusterB, clusterC)
                .getPolicyInfo(),
            Optional.of(mergeAB.getId()));
    }

    /**
     * Helper class to set up a cluster merge policy
     * @param id id for the policy
     * @param name name for the policy
     * @param clusters groups corresponding to clusters to be included
     * @return the policy.
     */
    private Policy clusterMergePolicy(Long id, String name, Grouping... clusters) {
        return Policy.newBuilder()
            .setId(id)
            .setPolicyInfo(clusterMergeInfo(name, clusters))
            .build();
    }

    /**
     * Helper class to create the policy info for a new cluster merge policy
     * @param name  policy name
     * @param clusters groups corresponding to clusters to be included
     * @return the policy.
     */
    private PolicyInfo clusterMergeInfo(String name, Grouping... clusters) {
        return PolicyInfo.newBuilder()
            .setName(name)
            .setMerge(MergePolicy.newBuilder()
                .setMergeType(MergeType.CLUSTER)
                .addAllMergeGroupIds(Stream.of(clusters)
                    .map(c -> c.getDefinition().getStaticGroupMembers().getMembersByType(0).getMembers(0))
                    .collect(Collectors.toList()))
                .build())
            .build();
    }

    /**
     * Helper class to create a group for a cluster
     * @param name name for the cluster
     * @param clusterUuids ids of clusters to be included
     * @return the created group.
     */
    private Grouping clusterGroup(String name, Long... clusterUuids) {
        return Grouping.newBuilder()
            .setDefinition(GroupDefinition.newBuilder()
                .setDisplayName(name)
                .setStaticGroupMembers(StaticMembers.newBuilder().addMembersByType(
                                StaticMembersByType.newBuilder()
                                .setType(MemberType.newBuilder().setGroup(GroupType.COMPUTE_HOST_CLUSTER))
                                .addAllMembers(Arrays.asList(clusterUuids))
                                )
                                )
                )
            .build();
    }
}
