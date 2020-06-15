package com.vmturbo.group.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.CancelQueuedActionsRequest;
import com.vmturbo.common.protobuf.action.ActionDTO.RemoveActionsAcceptancesRequest;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.DeleteSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.DeleteSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingGroup.SettingPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingPoliciesResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPoliciesUsingScheduleRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.ResetSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.ResetSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.SortedSetOfOidSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest.Context;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest.SettingsChunk;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsResponse;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.group.common.ItemNotFoundException.SettingPolicyNotFoundException;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.setting.EntitySettingStore;
import com.vmturbo.group.setting.EntitySettingStore.NoSettingsForTopologyException;
import com.vmturbo.group.setting.SettingPolicyFilter;
import com.vmturbo.group.setting.SettingSpecStore;
import com.vmturbo.group.setting.SettingStore;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Unit test for {@link SettingPolicyRpcService}.
 */
public class SettingPolicyRpcServiceTest {

    private long realtimeTopologyContextId = 777777;

    private SettingStore settingStore;

    private SettingSpecStore settingSpecStore = mock(SettingSpecStore.class);

    private SettingSpec settingSpec = SettingSpec.newBuilder()
            .setName("doStuff")
            .setEntitySettingSpec(EntitySettingSpec.getDefaultInstance())
            .setBooleanSettingValueType(BooleanSettingValueType.getDefaultInstance())
            .build();

    private SettingPolicyInfo settingPolicyInfo = SettingPolicyInfo.newBuilder()
            .setName("name")
            .addSettings(Setting.newBuilder()
                    .setSettingSpecName(settingSpec.getName())
                    .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(true))
                    .build())
            .build();

    private SettingPolicy settingPolicy = SettingPolicy.newBuilder()
            .setId(7L)
            .setInfo(settingPolicyInfo)
            .setSettingPolicyType(Type.USER)
            .build();

    private SettingSpec automationSettingSpec =
            EntitySettingSpecs.Move.getSettingSpec();

    private Setting automationSetting =
            Setting.newBuilder()
                    .setSettingSpecName(automationSettingSpec.getName())
                    .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.MANUAL.toString()).build())
                    .build();

    private SettingPolicyInfo automationSettingPolicyInfo = SettingPolicyInfo.newBuilder()
            .setName("automationSettingPolicy")
            .addSettings(automationSetting)
            .build();

    private SettingPolicy automationSettingPolicy =
            SettingPolicy.newBuilder()
                    .setId(100L)
                    .setInfo(automationSettingPolicyInfo)
                    .setSettingPolicyType(Type.DEFAULT)
                    .build();

    private final EntitySettingStore entitySettingStore = mock(EntitySettingStore.class);

    private ActionsServiceMole actionServiceMole = spy(new ActionsServiceMole());

    @Rule
    public GrpcTestServer grpcTestServer =
            GrpcTestServer.newServer(actionServiceMole);

    private SettingPolicyRpcService settingPolicyService;
    private MockTransactionProvider transactionProvider;

    @Before
    public void setup() {
        final IdentityProvider identityProvider = new IdentityProvider(0);
        transactionProvider = new MockTransactionProvider();
        this.settingStore = Mockito.mock(SettingStore.class);
        settingPolicyService =
                new SettingPolicyRpcService(settingStore, settingSpecStore, entitySettingStore,
                        ActionsServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
                        identityProvider, transactionProvider, realtimeTopologyContextId, 1);
    }

    @Test
    public void testCreatePolicy() throws Exception {

        final StreamObserver<CreateSettingPolicyResponse> responseObserver =
                (StreamObserver<CreateSettingPolicyResponse>)mock(StreamObserver.class);

        settingPolicyService.createSettingPolicy(CreateSettingPolicyRequest.newBuilder()
                .setSettingPolicyInfo(settingPolicyInfo)
                .build(), responseObserver);

        final ArgumentCaptor<CreateSettingPolicyResponse> responseCaptor =
                ArgumentCaptor.forClass(CreateSettingPolicyResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();

        CreateSettingPolicyResponse response = responseCaptor.getValue();
        assertTrue(response.hasSettingPolicy());
        Assert.assertEquals(settingPolicyInfo, response.getSettingPolicy().getInfo());
        Assert.assertEquals(Type.USER, response.getSettingPolicy().getSettingPolicyType());
    }

    @Test
    public void testCreatePolicyNoInfo() throws Exception {
        final StreamObserver<CreateSettingPolicyResponse> responseObserver =
                (StreamObserver<CreateSettingPolicyResponse>)mock(StreamObserver.class);
        settingPolicyService.createSettingPolicy(CreateSettingPolicyRequest.newBuilder()
                .build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());

        StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher
                .hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("Missing"));
    }

    @Test
    public void testCreatePolicyInvalid() throws Exception {
        final String errorMsg = "ERR";
        final StreamObserver<CreateSettingPolicyResponse> responseObserver =
                (StreamObserver<CreateSettingPolicyResponse>)mock(StreamObserver.class);

        Mockito.doThrow(new StoreOperationException(Status.INVALID_ARGUMENT, errorMsg))
                .when(transactionProvider.getSettingPolicyStore())
                .createSettingPolicies(Mockito.any());
        settingPolicyService.createSettingPolicy(CreateSettingPolicyRequest.newBuilder()
                .setSettingPolicyInfo(settingPolicyInfo)
                .build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        Mockito.verify(responseObserver).onError(exceptionCaptor.capture());

        StatusException exception = exceptionCaptor.getValue();
        assertThat(exception,
                GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT).descriptionContains(errorMsg));
    }

    @Test
    public void testCreatePolicyDuplicateName() throws Exception {
        final StreamObserver<CreateSettingPolicyResponse> responseObserver =
                (StreamObserver<CreateSettingPolicyResponse>)mock(StreamObserver.class);
        Mockito.doThrow(new StoreOperationException(Status.ALREADY_EXISTS, "foo"))
                .when(transactionProvider.getSettingPolicyStore())
                        .createSettingPolicies(Mockito.any());

        settingPolicyService.createSettingPolicy(CreateSettingPolicyRequest.newBuilder()
                .setSettingPolicyInfo(settingPolicyInfo)
                .build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());

        StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.ALREADY_EXISTS)
                .descriptionContains("foo"));
    }

    @Test
    public void testResetPolicy() throws Exception {
        final SettingPolicy resetPolicy = SettingPolicy.newBuilder()
                .setId(7L)
                .build();
        final StreamObserver<ResetSettingPolicyResponse> responseObserver =
                (StreamObserver<ResetSettingPolicyResponse>)mock(StreamObserver.class);
        when(settingStore.resetSettingPolicy(7L)).thenReturn(Pair.create(resetPolicy, false));

        settingPolicyService.resetSettingPolicy(ResetSettingPolicyRequest.newBuilder()
                .setSettingPolicyId(7)
                .build(), responseObserver);
        final ArgumentCaptor<ResetSettingPolicyResponse> responseCaptor =
                ArgumentCaptor.forClass(ResetSettingPolicyResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();

        assertThat(resetPolicy, is(responseCaptor.getValue().getSettingPolicy()));
    }

    @Test
    public void testResetPolicyNotFound() throws Exception {
        final StreamObserver<ResetSettingPolicyResponse> responseObserver =
                (StreamObserver<ResetSettingPolicyResponse>)mock(StreamObserver.class);
        when(settingStore.resetSettingPolicy(7L))
            .thenThrow(new SettingPolicyNotFoundException(7L));

        settingPolicyService.resetSettingPolicy(ResetSettingPolicyRequest.newBuilder()
                .setSettingPolicyId(7)
                .build(), responseObserver);
        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.NOT_FOUND)
                .descriptionContains("7"));
    }

    @Test
    public void testResetPolicyInvalid() throws Exception {
        final StreamObserver<ResetSettingPolicyResponse> responseObserver =
                (StreamObserver<ResetSettingPolicyResponse>)mock(StreamObserver.class);
        when(settingStore.resetSettingPolicy(7L))
                .thenThrow(new IllegalArgumentException("gesundheit"));

        settingPolicyService.resetSettingPolicy(ResetSettingPolicyRequest.newBuilder()
                .setSettingPolicyId(7)
                .build(), responseObserver);
        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("gesundheit"));
    }

    @Test
    public void testResetPolicyCheckCancelActionsInvocation() throws Exception {

        final StreamObserver<ResetSettingPolicyResponse> responseObserver =
                (StreamObserver<ResetSettingPolicyResponse>)mock(StreamObserver.class);
        when(settingStore.resetSettingPolicy(eq(100L)))
                .thenReturn(Pair.create(automationSettingPolicy, false));

        settingPolicyService.resetSettingPolicy(ResetSettingPolicyRequest.newBuilder()
                .setSettingPolicyId(100L)
                .build(), responseObserver);

        verify(actionServiceMole).cancelQueuedActions(any());
    }

    @Test
    public void testUpdatePolicy() throws Exception {
        final SettingPolicy updatedPolicy = SettingPolicy.newBuilder()
                .setId(7L)
                .build();
        final StreamObserver<UpdateSettingPolicyResponse> responseObserver =
                (StreamObserver<UpdateSettingPolicyResponse>)mock(StreamObserver.class);
        Mockito.doReturn(Pair.create(updatedPolicy, false))
                .when(transactionProvider.getSettingPolicyStore())
                .updateSettingPolicy(eq(7L), eq(settingPolicyInfo));

        settingPolicyService.updateSettingPolicy(UpdateSettingPolicyRequest.newBuilder()
                .setId(7L)
                .setNewInfo(settingPolicyInfo)
                .build(), responseObserver);

        final ArgumentCaptor<UpdateSettingPolicyResponse> responseCaptor =
                ArgumentCaptor.forClass(UpdateSettingPolicyResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();

        final UpdateSettingPolicyResponse response = responseCaptor.getValue();
        assertEquals(updatedPolicy, response.getSettingPolicy());
    }

    @Test
    public void testUpdatePolicyNoId() throws Exception {
        final StreamObserver<UpdateSettingPolicyResponse> responseObserver =
                (StreamObserver<UpdateSettingPolicyResponse>)mock(StreamObserver.class);

        settingPolicyService.updateSettingPolicy(UpdateSettingPolicyRequest.newBuilder()
                .setNewInfo(settingPolicyInfo)
                .build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("ID"));
    }

    @Test
    public void testUpdatePolicyNoNewInfo() throws Exception {
        final StreamObserver<UpdateSettingPolicyResponse> responseObserver =
                (StreamObserver<UpdateSettingPolicyResponse>)mock(StreamObserver.class);

        settingPolicyService.updateSettingPolicy(UpdateSettingPolicyRequest.newBuilder()
                .setId(7L)
                .build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("new"));
    }

    @Test
    public void testUpdatePolicyInvalid() throws Exception {
        final long policyId = 7L;
        final String msg = "Das Problem";
        final StreamObserver<UpdateSettingPolicyResponse> responseObserver =
                (StreamObserver<UpdateSettingPolicyResponse>)mock(StreamObserver.class);
        Mockito.doThrow(new StoreOperationException(Status.INVALID_ARGUMENT, msg))
                .when(transactionProvider.getSettingPolicyStore())
                .updateSettingPolicy(policyId, settingPolicyInfo);

        settingPolicyService.updateSettingPolicy(UpdateSettingPolicyRequest.newBuilder()
                .setId(7L)
                .setNewInfo(settingPolicyInfo)
                .build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains(msg));
    }

    @Test
    public void testUpdatePolicyNotFound() throws Exception {
        final long id = 7;
        final StreamObserver<UpdateSettingPolicyResponse> responseObserver =
                (StreamObserver<UpdateSettingPolicyResponse>)mock(StreamObserver.class);
        Mockito.doThrow(new StoreOperationException(Status.NOT_FOUND,
                "Setting Policy " + id + " not found."))
                .when(transactionProvider.getSettingPolicyStore())
                .updateSettingPolicy(eq(id), eq(settingPolicyInfo));

        settingPolicyService.updateSettingPolicy(UpdateSettingPolicyRequest.newBuilder()
                .setId(id)
                .setNewInfo(settingPolicyInfo)
                .build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.NOT_FOUND)
                .descriptionContains(Long.toString(id)));
    }

    @Test
    public void testUpdatePolicyDuplicateName() throws Exception {
        final long id = 7;
        final String name = "Das Name";
        final StreamObserver<UpdateSettingPolicyResponse> responseObserver =
                (StreamObserver<UpdateSettingPolicyResponse>)mock(StreamObserver.class);
        Mockito.doThrow(new StoreOperationException(Status.ALREADY_EXISTS, "Duplicated policy names found: " + name))
                .when(transactionProvider.getSettingPolicyStore())
                .updateSettingPolicy(id, settingPolicyInfo);

        settingPolicyService.updateSettingPolicy(UpdateSettingPolicyRequest.newBuilder()
                .setId(id)
                .setNewInfo(settingPolicyInfo)
                .build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.ALREADY_EXISTS)
                .descriptionContains(name));
    }

    @Test
    public void testUpdatePolicyCheckCancelActionsInvocation() throws Exception {
        final StreamObserver<UpdateSettingPolicyResponse> responseObserver =
                (StreamObserver<UpdateSettingPolicyResponse>)mock(StreamObserver.class);
        Mockito.doReturn(Pair.create(automationSettingPolicy, false))
                .when(transactionProvider.getSettingPolicyStore())
                .updateSettingPolicy(eq(100L), eq(automationSettingPolicyInfo));

        settingPolicyService.updateSettingPolicy(UpdateSettingPolicyRequest.newBuilder()
                .setId(100L)
                .setNewInfo(automationSettingPolicyInfo)
                .build(), responseObserver);

        verify(actionServiceMole).cancelQueuedActions(any());
    }

    @Test
    public void testDeletePolicy() throws Exception {
        final long id = 7;
        transactionProvider.getSettingPolicyStore()
                .createSettingPolicies(Collections.singletonList(settingPolicy));
        final StreamObserver<DeleteSettingPolicyResponse> responseObserver =
                (StreamObserver<DeleteSettingPolicyResponse>)mock(StreamObserver.class);
        settingPolicyService.deleteSettingPolicy(DeleteSettingPolicyRequest.newBuilder()
                .setId(id)
                .build(), responseObserver);

        final ArgumentCaptor<DeleteSettingPolicyResponse> responseCaptor =
                ArgumentCaptor.forClass(DeleteSettingPolicyResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
        Mockito.verify(responseObserver, Mockito.never()).onError(Mockito.any());

        assertEquals(DeleteSettingPolicyResponse.getDefaultInstance(), responseCaptor.getValue());
        Mockito.verify(transactionProvider.getSettingPolicyStore()).deletePolicies(
                Collections.singletonList(id), Type.USER);
    }

    @Test
    public void testDeletePolicyNotFound() throws Exception {
        final long id = 7;
        final StreamObserver<DeleteSettingPolicyResponse> responseObserver =
                (StreamObserver<DeleteSettingPolicyResponse>)mock(StreamObserver.class);
        settingPolicyService.deleteSettingPolicy(DeleteSettingPolicyRequest.newBuilder()
                .setId(id)
                .build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.NOT_FOUND)
                .descriptionContains(Long.toString(id)));
    }

    @Test
    public void testDeletePolicyImmutable() throws Exception {
        final long id = 7;
        final String error = "ERRORMSG";
        final StreamObserver<DeleteSettingPolicyResponse> responseObserver =
                (StreamObserver<DeleteSettingPolicyResponse>)mock(StreamObserver.class);
        settingPolicyService.deleteSettingPolicy(DeleteSettingPolicyRequest.newBuilder()
                .setId(id)
                .build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        Assert.assertThat(exception, GrpcExceptionMatcher.hasCode(Code.NOT_FOUND)
                .descriptionContains(Long.toString(id)));
    }

    /**
     * Test that after deleting settingPolicy we cancel queued actions and remove acceptance for
     * actions associated with this policy.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testDeletePolicyCheckCancelActionsInvocationAndRemovingAcceptance()
            throws Exception {
        final StreamObserver<DeleteSettingPolicyResponse> responseObserver =
                (StreamObserver<DeleteSettingPolicyResponse>)mock(StreamObserver.class);
        transactionProvider.getSettingPolicyStore()
                .createSettingPolicies(Collections.singletonList(automationSettingPolicy));

        settingPolicyService.deleteSettingPolicy(DeleteSettingPolicyRequest.newBuilder()
                .setId(automationSettingPolicy.getId())
                .build(), responseObserver);

        verify(actionServiceMole).cancelQueuedActions(
                CancelQueuedActionsRequest.getDefaultInstance());
        verify(actionServiceMole).removeActionsAcceptances(
                RemoveActionsAcceptancesRequest.newBuilder()
                        .setPolicyId(automationSettingPolicy.getId())
                        .build());
    }

    @Test
    public void testGetPolicyNotFound() throws Exception {
        final StreamObserver<GetSettingPolicyResponse> responseObserver =
                (StreamObserver<GetSettingPolicyResponse>)mock(StreamObserver.class);

        settingPolicyService.getSettingPolicy(GetSettingPolicyRequest.newBuilder()
                .setId(7L)
                .build(), responseObserver);

        verify(responseObserver).onNext(eq(GetSettingPolicyResponse.getDefaultInstance()));
    }

    @Test
    public void testGetPolicyByName() throws Exception {
        final StreamObserver<GetSettingPolicyResponse> responseObserver =
                (StreamObserver<GetSettingPolicyResponse>)mock(StreamObserver.class);

        transactionProvider.getSettingPolicyStore()
                .createSettingPolicies(Collections.singletonList(settingPolicy));
        settingPolicyService.getSettingPolicy(GetSettingPolicyRequest.newBuilder()
                .setName("name")
                .build(), responseObserver);

        verify(responseObserver).onNext(eq(GetSettingPolicyResponse.newBuilder()
                .setSettingPolicy(settingPolicy)
                .build()));
    }

    @Test
    public void testGetPolicyById() throws Exception {
        final StreamObserver<GetSettingPolicyResponse> responseObserver =
                (StreamObserver<GetSettingPolicyResponse>)mock(StreamObserver.class);

        transactionProvider.getSettingPolicyStore()
                .createSettingPolicies(Collections.singletonList(settingPolicy));
        settingPolicyService.getSettingPolicy(GetSettingPolicyRequest.newBuilder()
                .setId(7L)
                .build(), responseObserver);

        verify(responseObserver).onNext(eq(GetSettingPolicyResponse.newBuilder()
                .setSettingPolicy(settingPolicy)
                .build()));
    }

    @Test
    public void testGetPolicyWithSpec() throws Exception {
        final StreamObserver<GetSettingPolicyResponse> responseObserver =
                (StreamObserver<GetSettingPolicyResponse>)mock(StreamObserver.class);

        transactionProvider.getSettingPolicyStore()
                .createSettingPolicies(Collections.singletonList(settingPolicy));
        when(settingSpecStore.getSettingSpec(settingSpec.getName()))
            .thenReturn(Optional.of(settingSpec));
        settingPolicyService.getSettingPolicy(GetSettingPolicyRequest.newBuilder()
                .setId(7L)
                .setIncludeSettingSpecs(true)
                .build(), responseObserver);

        verify(responseObserver).onNext(eq(GetSettingPolicyResponse.newBuilder()
                .setSettingPolicy(settingPolicy)
                .addSettingSpecs(settingSpec)
                .build()));
    }

    @Test
    public void testGetPolicyWithMissingSpec() throws Exception {
        final StreamObserver<GetSettingPolicyResponse> responseObserver =
                (StreamObserver<GetSettingPolicyResponse>)mock(StreamObserver.class);
        transactionProvider.getSettingPolicyStore()
                .createSettingPolicies(Collections.singletonList(settingPolicy));
        when(settingSpecStore.getSettingSpec(settingSpec.getName()))
                .thenReturn(Optional.empty());
        settingPolicyService.getSettingPolicy(GetSettingPolicyRequest.newBuilder()
                .setId(7L)
                .setIncludeSettingSpecs(true)
                .build(), responseObserver);

        verify(responseObserver).onNext(eq(GetSettingPolicyResponse.newBuilder()
                .setSettingPolicy(settingPolicy)
                // No setting spec, because it wasn't found.
                .build()));
    }

    @Test
    public void testListPolicies() throws Exception {
        final StreamObserver<SettingPolicy> responseObserver =
                (StreamObserver<SettingPolicy>)mock(StreamObserver.class);
        transactionProvider.getSettingPolicyStore()
                .createSettingPolicies(Arrays.asList(settingPolicy,
                        SettingPolicy.newBuilder(settingPolicy).setId(8L).build()));

        settingPolicyService.listSettingPolicies(ListSettingPoliciesRequest.getDefaultInstance(),
                responseObserver);
        final ArgumentCaptor<SettingPolicy> result = ArgumentCaptor.forClass(SettingPolicy.class);
        verify(responseObserver, times(2)).onNext(result.capture());
        Assert.assertEquals(Sets.newHashSet(7L, 8L), result.getAllValues()
                .stream()
                .map(SettingPolicy::getId)
                .collect(Collectors.toSet()));
        verify(responseObserver).onCompleted();
    }

    @Test
    public void testListPolicyForPlan() throws Exception {
        final StreamObserver<SettingPolicy> responseObserver =
                        (StreamObserver<SettingPolicy>)mock(StreamObserver.class);
        transactionProvider.getSettingPolicyStore()
                .createSettingPolicies(Collections.singletonList(automationSettingPolicy));

       settingPolicyService.listSettingPolicies(ListSettingPoliciesRequest.newBuilder()
                .setTypeFilter(Type.DEFAULT).setContextId(111).build(), responseObserver);

       Setting newSetting = automationSettingPolicy.toBuilder().getInfoBuilder().getSettingsBuilder(0)
                .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.AUTOMATIC.toString())
                        .build()).build();
        final SettingPolicy newPolicy = SettingPolicy.newBuilder()
                .setId(100L)
                .setInfo(SettingPolicyInfo.newBuilder()
                        .setName("automationSettingPolicy")
                        .addSettings(newSetting)
                        .build())
                .setSettingPolicyType(Type.DEFAULT)
                .build();

       verify(responseObserver, times(1)).onNext(eq(newPolicy));
       verify(responseObserver).onCompleted();
    }

    @Test
    public void testListPoliciesTypeFilter() throws Exception {
        final StreamObserver<SettingPolicy> responseObserver =
                (StreamObserver<SettingPolicy>)mock(StreamObserver.class);
        when(transactionProvider.getSettingPolicyStore().getPolicies(eq(SettingPolicyFilter.newBuilder()
                .withType(Type.DEFAULT)
                .build())))
            // Just return the same one twice - that's fine for the purpose of the test.
            .thenReturn(Arrays.asList(automationSettingPolicy, automationSettingPolicy));
        settingPolicyService.listSettingPolicies(ListSettingPoliciesRequest.newBuilder()
                    .setTypeFilter(Type.DEFAULT)
                    .build(),
                responseObserver);

        verify(responseObserver, times(2)).onNext(eq(automationSettingPolicy));
        verify(responseObserver).onCompleted();
    }

    @Test
    public void testUploadEntitySettingsMissingArguments() {
        final StreamObserver<UploadEntitySettingsResponse> responseObserver =
                (StreamObserver<UploadEntitySettingsResponse>)mock(StreamObserver.class);

        StreamObserver<UploadEntitySettingsRequest> requestObserver =
            settingPolicyService.uploadEntitySettings(responseObserver);
        requestObserver.onNext(UploadEntitySettingsRequest.newBuilder()
           .build()
        );
        settingPolicyService.uploadEntitySettings(responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());

        StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher
                .hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("Unexpected request chunk"));
    }

    @Test
    public void testUploadEntitySettings() {
        final StreamObserver<UploadEntitySettingsResponse> responseObserver =
                (StreamObserver<UploadEntitySettingsResponse>)mock(StreamObserver.class);

        EntitySettings es =
            EntitySettings.newBuilder()
                .setEntityOid(1234)
                .addAllUserSettings(
                        settingPolicyInfo.getSettingsList().stream()
                                .map(setting -> SettingToPolicyId.newBuilder()
                                        .setSetting(setting)
                                        .addSettingPolicyId(settingPolicy.getId())
                                        .build())
                                .collect(Collectors.toList()))
                    .build();

        List<EntitySettings> esList = new LinkedList<>();
        esList.add(es);

        long topologyId = 1111;
        long topologyContexId = 7777;

        final ArgumentCaptor<UploadEntitySettingsResponse> responseCaptor =
                ArgumentCaptor.forClass(UploadEntitySettingsResponse.class);
        StreamObserver<UploadEntitySettingsRequest> requestObserver =
            settingPolicyService.uploadEntitySettings(responseObserver);

        requestObserver.onNext(UploadEntitySettingsRequest.newBuilder()
            .setContext(Context.newBuilder()
                .setTopologyId(topologyId)
                .setTopologyContextId(topologyContexId))
            .build());
        requestObserver.onNext(UploadEntitySettingsRequest.newBuilder()
            .setSettingsChunk(SettingsChunk.newBuilder()
                .addAllEntitySettings(esList))
            .build());
        requestObserver.onCompleted();
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();

    }

    /**
     * Test receiving and processing multiple chunks.
     */
    @Test
    public void testUploadEntitySettingsMultipleChunks() {
        final StreamObserver<UploadEntitySettingsResponse> responseObserver =
            (StreamObserver<UploadEntitySettingsResponse>)mock(StreamObserver.class);

        EntitySettings es1 =
            EntitySettings.newBuilder()
                .setEntityOid(1234)
                .build();

        EntitySettings es2 =
            EntitySettings.newBuilder()
                .setEntityOid(4567)
                .build();

        EntitySettings es3 =
            EntitySettings.newBuilder()
                .setEntityOid(8910)
                .build();

        List<EntitySettings> esList = new LinkedList<>();
        esList.add(es1);
        esList.add(es2);
        esList.add(es3);

        long topologyId = 1111;
        long topologyContexId = 7777;

        StreamObserver<UploadEntitySettingsRequest> requestObserver =
            settingPolicyService.uploadEntitySettings(responseObserver);
        requestObserver.onNext(UploadEntitySettingsRequest.newBuilder()
            .setContext(Context.newBuilder()
                .setTopologyId(topologyId)
                .setTopologyContextId(topologyContexId))
            .build());
        for (EntitySettings entity : esList) {
            requestObserver.onNext(UploadEntitySettingsRequest.newBuilder()
                .setSettingsChunk(SettingsChunk.newBuilder()
                    .addAllEntitySettings(Arrays.asList(entity)))
                .build());
        }
        requestObserver.onCompleted();
        verify(responseObserver).onNext(any());
        verify(responseObserver).onCompleted();
    }

    /**
     * Test failure when processing a request with no context.
     */
    @Test
    public void testUploadEntitySettingsFailingWhenNoContext() {
        final StreamObserver<UploadEntitySettingsResponse> responseObserver =
            (StreamObserver<UploadEntitySettingsResponse>)mock(StreamObserver.class);

        EntitySettings es1 =
            EntitySettings.newBuilder()
                .setEntityOid(1234)
                .build();

        EntitySettings es2 =
            EntitySettings.newBuilder()
                .setEntityOid(4567)
                .build();

        List<EntitySettings> esList = new LinkedList<>();
        esList.add(es1);
        esList.add(es2);

        long topologyId = 1111;
        long topologyContexId = 7777;

        StreamObserver<UploadEntitySettingsRequest> requestObserver =
            settingPolicyService.uploadEntitySettings(responseObserver);

        // No context.

        for (EntitySettings entity : esList) {
            requestObserver.onNext(UploadEntitySettingsRequest.newBuilder()
                .setSettingsChunk(SettingsChunk.newBuilder()
                    .addAllEntitySettings(Arrays.asList(entity)))
                .build());
        }

        requestObserver.onCompleted();
        verify(responseObserver).onError(any());
    }

    /**
     * Test GetEntitySettingPolicies Grpc call.
     */
    @Test
    public void testGetEntitySettingPolicies() throws StoreOperationException {
        final StreamObserver<GetEntitySettingPoliciesResponse> responseObserver =
            (StreamObserver<GetEntitySettingPoliciesResponse>)mock(StreamObserver.class);
        long entityOid = 12345L;
        final SettingPolicy policy = SettingPolicy.newBuilder()
            .setInfo(SettingPolicyInfo.getDefaultInstance())
            .build();
        when(entitySettingStore.getEntitySettingPolicies(eq(Sets.newHashSet(entityOid))))
            .thenReturn(Arrays.asList(policy));
        settingPolicyService.getEntitySettingPolicies(GetEntitySettingPoliciesRequest.newBuilder()
            .addEntityOidList(entityOid)
            .build(), responseObserver);
        final ArgumentCaptor<GetEntitySettingPoliciesResponse> respCaptor =
            ArgumentCaptor.forClass(GetEntitySettingPoliciesResponse.class);
        verify(responseObserver).onNext(respCaptor.capture());
        verify(responseObserver).onCompleted();
        assertTrue(respCaptor.getValue().getSettingPoliciesList().size() == 1);
        assertTrue(respCaptor.getValue().getSettingPoliciesList().get(0).equals(policy));
    }

    @Test
    public void testGetEntitySettings() throws Exception {
        final StreamObserver<GetEntitySettingsResponse> responseObserver =
                (StreamObserver<GetEntitySettingsResponse>)mock(StreamObserver.class);

        final TopologySelection topologyFilter = TopologySelection.newBuilder()
                .setTopologyId(7L)
                .setTopologyContextId(8L)
                .build();

        final EntitySettingFilter settingsFilter = EntitySettingFilter.newBuilder()
                .addEntities(7L)
                .build();

        final Setting setting = Setting.newBuilder()
                .setSettingSpecName("name")
                .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
                .build();

        final SettingToPolicyId settingToPolicyId = SettingToPolicyId.newBuilder()
                .setSetting(setting)
                .addSettingPolicyId(1L)
                .build();

        when(entitySettingStore.getEntitySettings(eq(topologyFilter), eq(settingsFilter)))
                .thenReturn(ImmutableMap.of(7L, Collections.singletonList(settingToPolicyId)));

        when(transactionProvider.getSettingPolicyStore().getPolicies(any())).thenReturn(
                Arrays.asList(SettingPolicy.newBuilder()
                        .setId(1L)
                        .setInfo(SettingPolicyInfo.newBuilder()
                                .setName("Test-SP")
                                .build())
                        .build()));

        settingPolicyService.getEntitySettings(GetEntitySettingsRequest.newBuilder()
                .setTopologySelection(topologyFilter)
                .setSettingFilter(settingsFilter)
                .build(), responseObserver);

        final ArgumentCaptor<GetEntitySettingsResponse> respCaptor =
                ArgumentCaptor.forClass(GetEntitySettingsResponse.class);
        verify(responseObserver).onNext(respCaptor.capture());
        verify(responseObserver).onCompleted();

        final GetEntitySettingsResponse response = respCaptor.getValue();
        final List<EntitySettingGroup> settingGroupList = response.getSettingGroupList();
        assertEquals(1, settingGroupList.size());
        final EntitySettingGroup settingGroup = settingGroupList.get(0);
        assertThat(settingGroup.getSetting(), is(settingToPolicyId.getSetting()));
        assertThat(settingGroup.getPolicyIdCount(), is(0));
        assertThat(settingGroup.getEntityOidsList(), containsInAnyOrder(7L));
    }

    @Test
    public void testGetEntitySettingsWithPolicy() throws Exception {
        final StreamObserver<GetEntitySettingsResponse> responseObserver =
            (StreamObserver<GetEntitySettingsResponse>)mock(StreamObserver.class);

        final TopologySelection topologyFilter = TopologySelection.newBuilder()
            .setTopologyId(7L)
            .setTopologyContextId(8L)
            .build();

        final EntitySettingFilter settingsFilter = EntitySettingFilter.newBuilder()
            .addEntities(7L)
            .build();

        final Setting setting = Setting.newBuilder()
            .setSettingSpecName("name")
            .setBooleanSettingValue(BooleanSettingValue.getDefaultInstance())
            .build();

        final SettingToPolicyId settingToPolicyId = SettingToPolicyId.newBuilder()
            .setSetting(setting)
            .addSettingPolicyId(1L)
            .build();

        when(entitySettingStore.getEntitySettings(eq(topologyFilter), eq(settingsFilter)))
            .thenReturn(ImmutableMap.of(7L, Collections.singletonList(settingToPolicyId)));

        Mockito.when(settingStore.getSettingPolicies(any())).thenReturn(
            Arrays.asList(SettingPolicy.newBuilder()
                .setId(1L)
                .setSettingPolicyType(Type.DISCOVERED)
                .setInfo(SettingPolicyInfo.newBuilder()
                    .setName("Test-SP")
                    .build())
                .build()));

        settingPolicyService.getEntitySettings(GetEntitySettingsRequest.newBuilder()
            .setTopologySelection(topologyFilter)
            .setSettingFilter(settingsFilter)
            .setIncludeSettingPolicies(true)
            .build(), responseObserver);

        final ArgumentCaptor<GetEntitySettingsResponse> respCaptor =
            ArgumentCaptor.forClass(GetEntitySettingsResponse.class);
        verify(responseObserver).onNext(respCaptor.capture());
        verify(responseObserver).onCompleted();

        final GetEntitySettingsResponse response = respCaptor.getValue();
        final List<EntitySettingGroup> settingGroupList = response.getSettingGroupList();
        assertEquals(1, settingGroupList.size());
        final EntitySettingGroup settingGroup = settingGroupList.get(0);
        assertThat(settingGroup.getSetting(), is(settingToPolicyId.getSetting()));
        assertThat(settingGroup.getPolicyIdCount(), is(1));
        assertThat(settingGroup.getPolicyId(0), is(SettingPolicyId.newBuilder()
            .setPolicyId(1L)
            .setDisplayName("Test-SP")
            .setType(Type.DISCOVERED)
            .build()));
        assertThat(settingGroup.getEntityOidsList(), containsInAnyOrder(7L));
    }

    /**
     * This method tests the grouping result for SortedSetOfOidSetting. In this test, there are:
     * Two tiers: {tier1, tier2},
     * Three entities: {entityOid1, entityOid2, entityOid3},
     * Three settingPolicyIds: {sp1, sp2, sp3}
     * Three SettingToPolicyIds:
     *     settingToPolicyId1, for entityOid1, excludes {tier1, tier2} and is resolved from {sp1, sp2},
     *     settingToPolicyId2, for entityOid2, excludes {tier1, tier2} and is resolved from {sp1, sp2},
     *     settingToPolicyId3, for entityOid3, excludes {tier1, tier2} and is resolved from {sp2, sp3}
     *
     * After grouping by SettingToPolicyId, we should have:
     *     EntitySettingGroup1, for {entityOid1, entityOid2}, excludes {tier1, tier2} and is resolved from {sp1, sp2},
     *     EntitySettingGroup2, for {entityOid3}, excludes {tier1, tier2} and is resolved from {sp2, sp3}
     *
     * @throws Exception on exceptions occurred
     */
    @Test
    public void testGetEntitySettingsWithPolicySortedSetOfOidSetting()
            throws Exception {
        final StreamObserver<GetEntitySettingsResponse> responseObserver =
            (StreamObserver<GetEntitySettingsResponse>)mock(StreamObserver.class);

        final TopologySelection topologyFilter = TopologySelection.newBuilder()
            .setTopologyId(7L).setTopologyContextId(8L).build();

        long entityOid1 = 10L;
        long entityOid2 = 11L;
        long entityOid3 = 12L;
        final EntitySettingFilter settingsFilter = EntitySettingFilter.newBuilder()
            .addAllEntities(Arrays.asList(entityOid1, entityOid2, entityOid3)).build();

        long tier1 = 100L;
        long tier2 = 101L;
        long sp1 = 1L;
        long sp2 = 2L;
        long sp3 = 3L;

        final Setting setting1 = Setting.newBuilder().setSettingSpecName("name")
            .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                .addAllOids(Arrays.asList(tier1, tier2))).build();
        final SettingToPolicyId settingToPolicyId1 = SettingToPolicyId.newBuilder()
            .setSetting(setting1)
            .addAllSettingPolicyId(Arrays.asList(sp1, sp2))
            .build();

        final Setting setting2 = Setting.newBuilder().setSettingSpecName("name")
            .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                .addAllOids(Arrays.asList(tier1, tier2))).build();
        final SettingToPolicyId settingToPolicyId2 = SettingToPolicyId.newBuilder()
            .setSetting(setting2)
            .addAllSettingPolicyId(Arrays.asList(sp1, sp2))
            .build();

        final Setting setting3 = Setting.newBuilder().setSettingSpecName("name")
            .setSortedSetOfOidSettingValue(SortedSetOfOidSettingValue.newBuilder()
                .addAllOids(Arrays.asList(tier1, tier2))).build();
        final SettingToPolicyId settingToPolicyId3 = SettingToPolicyId.newBuilder()
            .setSetting(setting3)
            .addAllSettingPolicyId(Arrays.asList(sp2, sp3))
            .build();

        when(entitySettingStore.getEntitySettings(eq(topologyFilter), eq(settingsFilter)))
            .thenReturn(ImmutableMap.of(
                entityOid1, Collections.singletonList(settingToPolicyId1),
                entityOid2, Collections.singletonList(settingToPolicyId2),
                entityOid3, Collections.singletonList(settingToPolicyId3)));

        Mockito.when(settingStore.getSettingPolicies(any()))
                .thenReturn(Arrays.asList(
            SettingPolicy.newBuilder().setId(sp1)
                .setSettingPolicyType(Type.USER)
                .setInfo(SettingPolicyInfo.newBuilder().setName("Test-SP1").build())
                .build(),
            SettingPolicy.newBuilder().setId(sp2)
                .setSettingPolicyType(Type.DISCOVERED)
                .setInfo(SettingPolicyInfo.newBuilder().setName("Test-SP2").build())
                .build(),
            SettingPolicy.newBuilder().setId(sp3)
                .setSettingPolicyType(Type.USER)
                .setInfo(SettingPolicyInfo.newBuilder().setName("Test-SP3").build())
                .build()));

        settingPolicyService.getEntitySettings(GetEntitySettingsRequest.newBuilder()
            .setTopologySelection(topologyFilter).setSettingFilter(settingsFilter)
            .setIncludeSettingPolicies(true).build(), responseObserver);

        final ArgumentCaptor<GetEntitySettingsResponse> respCaptor =
            ArgumentCaptor.forClass(GetEntitySettingsResponse.class);
        verify(responseObserver, times(2)).onNext(respCaptor.capture());
        verify(responseObserver).onCompleted();

        final List<GetEntitySettingsResponse> responses = respCaptor.getAllValues();
        assertThat(responses.size(), is(2));
        for (GetEntitySettingsResponse response : responses) {
            final List<EntitySettingGroup> settingGroupList = response.getSettingGroupList();
            assertThat(settingGroupList.size(), is(1));
            final EntitySettingGroup settingGroup = settingGroupList.get(0);
            if (settingGroup.getEntityOidsList().size() == 2) {
                assertThat(settingGroup.getEntityOidsList().size(), is(2));
                assertThat(settingGroup.getEntityOidsList(), containsInAnyOrder(entityOid1, entityOid2));
                assertThat(settingGroup.getSetting(), is(settingToPolicyId1.getSetting()));
                assertThat(settingGroup.getPolicyIdCount(), is(2));
                assertThat(settingGroup.getPolicyIdList(), containsInAnyOrder(
                    SettingPolicyId.newBuilder().setPolicyId(sp1)
                        .setDisplayName("Test-SP1").setType(Type.USER).build(),
                    SettingPolicyId.newBuilder().setPolicyId(sp2)
                        .setDisplayName("Test-SP2").setType(Type.DISCOVERED).build()));
            } else {
                assertThat(settingGroup.getEntityOidsList().size(), is(1));
                assertThat(settingGroup.getEntityOidsList(), containsInAnyOrder(entityOid3));
                assertThat(settingGroup.getSetting(), is(settingToPolicyId3.getSetting()));
                assertThat(settingGroup.getPolicyIdCount(), is(2));
                assertThat(settingGroup.getPolicyIdList(), containsInAnyOrder(
                    SettingPolicyId.newBuilder().setPolicyId(sp2)
                        .setDisplayName("Test-SP2").setType(Type.DISCOVERED).build(),
                    SettingPolicyId.newBuilder().setPolicyId(sp3)
                        .setDisplayName("Test-SP3").setType(Type.USER).build()));
            }
        }
    }

    @Test
    public void testGetEntitySettingsEntityNotFound() throws NoSettingsForTopologyException, InvalidProtocolBufferException {
        final StreamObserver<GetEntitySettingsResponse> responseObserver =
                (StreamObserver<GetEntitySettingsResponse>)mock(StreamObserver.class);
        final EntitySettingFilter settingsFilter = EntitySettingFilter.newBuilder()
                .addEntities(7L) // SUppose this entity is not found.
                .build();
        when(entitySettingStore.getEntitySettings(eq(TopologySelection.getDefaultInstance()),
                    eq(settingsFilter)))
                .thenReturn(ImmutableMap.of(7L, Collections.emptyList()));

        settingPolicyService.getEntitySettings(GetEntitySettingsRequest.newBuilder()
                .setSettingFilter(settingsFilter)
                .build(), responseObserver);

        verify(responseObserver, never()).onNext(any());
        verify(responseObserver).onCompleted();
    }

    @Test
    public void testGetEntitySettingsTopologyNotFound() throws NoSettingsForTopologyException, InvalidProtocolBufferException {
        final StreamObserver<GetEntitySettingsResponse> responseObserver =
                (StreamObserver<GetEntitySettingsResponse>)mock(StreamObserver.class);
        when(entitySettingStore.getEntitySettings(any(), any()))
            .thenThrow(new NoSettingsForTopologyException(1, 2));
        settingPolicyService.getEntitySettings(GetEntitySettingsRequest.getDefaultInstance(),
                responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue(),
            GrpcExceptionMatcher.hasCode(Code.NOT_FOUND).anyDescription());
    }

    /**
     * Test get setting policies using schedule rpc.
     */
    @Test
    public void testGetPoliciesUsingSchedule() {
        final StreamObserver<SettingPolicy> responseObserver =
            (StreamObserver<SettingPolicy>)mock(StreamObserver.class);
        final long scheduleId = 11L;
        Mockito.when(transactionProvider.getSettingPolicyStore()
                .getPolicies(SettingPolicyFilter.newBuilder()
                        .withActivationScheduleId(scheduleId)
                        .build())).thenReturn(Arrays.asList(settingPolicy, settingPolicy));
        Mockito.when(transactionProvider.getSettingPolicyStore()
                .getPolicies(SettingPolicyFilter.newBuilder()
                        .withExecutionScheduleId(scheduleId)
                        .build())).thenReturn(Collections.singletonList(settingPolicy));
        settingPolicyService.getSettingPoliciesUsingSchedule(
                GetSettingPoliciesUsingScheduleRequest.newBuilder()
                        .setScheduleId(scheduleId)
                        .build(), responseObserver);

        verify(responseObserver, times(3)).onNext(eq(settingPolicy));
        verify(responseObserver).onCompleted();
    }

    /**
     * Test get setting policies using schedule rpc with missing schedule id in request
     * generates error.
     */
    @Test
    public void testGetPoliciesUsingScheduleMissingScheduleId() {
        final StreamObserver<SettingPolicy> responseObserver =
            (StreamObserver<SettingPolicy>)mock(StreamObserver.class);
        settingPolicyService.getSettingPoliciesUsingSchedule(
            GetSettingPoliciesUsingScheduleRequest.getDefaultInstance(),
            responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
            ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());
        assertThat(exceptionCaptor.getValue(),
            GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT).anyDescription());
    }
}
