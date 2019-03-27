package com.vmturbo.group.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.ImmutableMap;

import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTOMoles.ActionsServiceMole;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.DeleteSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.DeleteSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettings.SettingToPolicyId;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse.SettingToPolicyName;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse.SettingsForEntity;
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
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UpdateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.UploadEntitySettingsResponse;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.ImmutableUpdateException.ImmutableSettingPolicyUpdateException;
import com.vmturbo.group.common.InvalidItemException;
import com.vmturbo.group.common.ItemNotFoundException.SettingPolicyNotFoundException;
import com.vmturbo.group.setting.EntitySettingStore;
import com.vmturbo.group.setting.EntitySettingStore.NoSettingsForTopologyException;
import com.vmturbo.group.setting.SettingPolicyFilter;
import com.vmturbo.group.setting.SettingSpecStore;
import com.vmturbo.group.setting.SettingStore;

public class SettingPolicyRpcServiceTest {

    private long realtimeTopologyContextId = 777777;

    private SettingStore settingStore = mock(SettingStore.class);

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
                    .build();

    private final EntitySettingStore entitySettingStore = mock(EntitySettingStore.class);

    private ActionsServiceMole actionServiceMole = spy(new ActionsServiceMole());

    @Rule
    public GrpcTestServer grpcTestServer =
            GrpcTestServer.newServer(actionServiceMole);

    private SettingPolicyRpcService settingPolicyService;

    @Before
    public void setup() {
        settingPolicyService =
                new SettingPolicyRpcService(settingStore, settingSpecStore, entitySettingStore,
                        ActionsServiceGrpc.newBlockingStub(grpcTestServer.getChannel()),
                        realtimeTopologyContextId);
    }

    @Test
    public void testCreatePolicy() throws Exception {

        final StreamObserver<CreateSettingPolicyResponse> responseObserver =
                (StreamObserver<CreateSettingPolicyResponse>)mock(StreamObserver.class);
        when(settingStore.createUserSettingPolicy(eq(settingPolicyInfo)))
                .thenReturn(settingPolicy);

        settingPolicyService.createSettingPolicy(CreateSettingPolicyRequest.newBuilder()
                .setSettingPolicyInfo(settingPolicyInfo)
                .build(), responseObserver);

        final ArgumentCaptor<CreateSettingPolicyResponse> responseCaptor =
                ArgumentCaptor.forClass(CreateSettingPolicyResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();

        CreateSettingPolicyResponse response = responseCaptor.getValue();
        assertTrue(response.hasSettingPolicy());
        assertEquals(settingPolicy, response.getSettingPolicy());
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
        when(settingStore.createUserSettingPolicy(eq(settingPolicyInfo)))
                .thenThrow(new InvalidItemException(errorMsg));

        settingPolicyService.createSettingPolicy(CreateSettingPolicyRequest.newBuilder()
                .setSettingPolicyInfo(settingPolicyInfo)
                .build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());

        StatusException exception = exceptionCaptor.getValue();
        assertThat(exception,
                GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT).descriptionContains(errorMsg));
    }

    @Test
    public void testCreatePolicyDuplicateName() throws Exception {
        final StreamObserver<CreateSettingPolicyResponse> responseObserver =
                (StreamObserver<CreateSettingPolicyResponse>)mock(StreamObserver.class);
        when(settingStore.createUserSettingPolicy(eq(settingPolicyInfo)))
                .thenThrow(new DuplicateNameException(1, "foo"));

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
        when(settingStore.resetSettingPolicy(7L)).thenReturn(resetPolicy);

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
                .thenReturn(automationSettingPolicy);

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
        when(settingStore.updateSettingPolicy(eq(7L), eq(settingPolicyInfo)))
            .thenReturn(updatedPolicy);

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
        final String msg = "Das Problem";
        final StreamObserver<UpdateSettingPolicyResponse> responseObserver =
                (StreamObserver<UpdateSettingPolicyResponse>)mock(StreamObserver.class);
        when(settingStore.updateSettingPolicy(eq(7L), eq(settingPolicyInfo)))
                .thenThrow(new InvalidItemException(msg));

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
        when(settingStore.updateSettingPolicy(eq(id), eq(settingPolicyInfo)))
                .thenThrow(new SettingPolicyNotFoundException(id));

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
        when(settingStore.updateSettingPolicy(eq(id), eq(settingPolicyInfo)))
                .thenThrow(new DuplicateNameException(id, name));

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
        when(settingStore.updateSettingPolicy(eq(100L), eq(automationSettingPolicyInfo)))
                .thenReturn(automationSettingPolicy);

        settingPolicyService.updateSettingPolicy(UpdateSettingPolicyRequest.newBuilder()
                .setId(100L)
                .setNewInfo(automationSettingPolicyInfo)
                .build(), responseObserver);

        verify(actionServiceMole).cancelQueuedActions(any());
    }

    @Test
    public void testDeletePolicy() throws Exception {
        final long id = 7;
        final StreamObserver<DeleteSettingPolicyResponse> responseObserver =
                (StreamObserver<DeleteSettingPolicyResponse>)mock(StreamObserver.class);
        settingPolicyService.deleteSettingPolicy(DeleteSettingPolicyRequest.newBuilder()
                .setId(id)
                .build(), responseObserver);

        final ArgumentCaptor<DeleteSettingPolicyResponse> responseCaptor =
                ArgumentCaptor.forClass(DeleteSettingPolicyResponse.class);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();

        assertEquals(DeleteSettingPolicyResponse.getDefaultInstance(), responseCaptor.getValue());
        verify(settingStore).deleteUserSettingPolicy(eq(id));
    }

    @Test
    public void testDeletePolicyNotFound() throws Exception {
        final long id = 7;
        final StreamObserver<DeleteSettingPolicyResponse> responseObserver =
                (StreamObserver<DeleteSettingPolicyResponse>)mock(StreamObserver.class);
        when(settingStore.deleteUserSettingPolicy(eq(id)))
            .thenThrow(new SettingPolicyNotFoundException(id));
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
        when(settingStore.deleteUserSettingPolicy(eq(id)))
                .thenThrow(new ImmutableSettingPolicyUpdateException(error));
        settingPolicyService.deleteSettingPolicy(DeleteSettingPolicyRequest.newBuilder()
                .setId(id)
                .build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());

        final StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains(error));
    }

    @Test
    public void testDeletePolicyCheckCancelActionsInvocation() throws Exception {
        final StreamObserver<DeleteSettingPolicyResponse> responseObserver =
                (StreamObserver<DeleteSettingPolicyResponse>)mock(StreamObserver.class);
        when(settingStore.deleteUserSettingPolicy(eq(100L)))
                .thenReturn(automationSettingPolicy);

        settingPolicyService.deleteSettingPolicy(DeleteSettingPolicyRequest.newBuilder()
                .setId(100L)
                .build(), responseObserver);

        verify(actionServiceMole).cancelQueuedActions(any());
    }

    @Test
    public void testGetPolicyNotFound() throws Exception {
        final StreamObserver<GetSettingPolicyResponse> responseObserver =
                (StreamObserver<GetSettingPolicyResponse>)mock(StreamObserver.class);

        when(settingStore.getSettingPolicy(anyLong())).thenReturn(Optional.empty());
        settingPolicyService.getSettingPolicy(GetSettingPolicyRequest.newBuilder()
                .setId(7L)
                .build(), responseObserver);

        verify(responseObserver).onNext(eq(GetSettingPolicyResponse.getDefaultInstance()));
    }

    @Test
    public void testGetPolicyByName() throws Exception {
        final StreamObserver<GetSettingPolicyResponse> responseObserver =
                (StreamObserver<GetSettingPolicyResponse>)mock(StreamObserver.class);

        when(settingStore.getSettingPolicy(eq("name"))).thenReturn(Optional.of(settingPolicy));
        settingPolicyService.getSettingPolicy(GetSettingPolicyRequest.newBuilder()
                .setName("name")
                .build(), responseObserver);

        verify(responseObserver).onNext(eq(GetSettingPolicyResponse.newBuilder()
                .setSettingPolicy(settingPolicy)
                .build()));
    }

    @Test
    public void testGetPolicyById() {
        final StreamObserver<GetSettingPolicyResponse> responseObserver =
                (StreamObserver<GetSettingPolicyResponse>)mock(StreamObserver.class);

        when(settingStore.getSettingPolicy(eq(7L))).thenReturn(Optional.of(settingPolicy));
        settingPolicyService.getSettingPolicy(GetSettingPolicyRequest.newBuilder()
                .setId(7L)
                .build(), responseObserver);

        verify(responseObserver).onNext(eq(GetSettingPolicyResponse.newBuilder()
                .setSettingPolicy(settingPolicy)
                .build()));
    }

    @Test
    public void testGetPolicyWithSpec() {
        final StreamObserver<GetSettingPolicyResponse> responseObserver =
                (StreamObserver<GetSettingPolicyResponse>)mock(StreamObserver.class);

        when(settingStore.getSettingPolicy(eq(7L))).thenReturn(Optional.of(settingPolicy));
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
    public void testGetPolicyWithMissingSpec() {
        final StreamObserver<GetSettingPolicyResponse> responseObserver =
                (StreamObserver<GetSettingPolicyResponse>)mock(StreamObserver.class);

        when(settingStore.getSettingPolicy(eq(7L))).thenReturn(Optional.of(settingPolicy));
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
    public void testListPolicies() {
        final StreamObserver<SettingPolicy> responseObserver =
                (StreamObserver<SettingPolicy>)mock(StreamObserver.class);
        when(settingStore.getSettingPolicies(eq(SettingPolicyFilter.newBuilder().build())))
            // Just return the same one twice - that's fine for the purpose of the test.
            .thenReturn(Stream.of(settingPolicy, settingPolicy));
        settingPolicyService.listSettingPolicies(ListSettingPoliciesRequest.getDefaultInstance(),
                responseObserver);

        verify(responseObserver, times(2)).onNext(eq(settingPolicy));
        verify(responseObserver).onCompleted();
    }

    @Test
    public void testListPolicyForPlan() throws Exception {
        final StreamObserver<SettingPolicy> responseObserver =
                        (StreamObserver<SettingPolicy>)mock(StreamObserver.class);
        when(settingStore.getSettingPolicies(eq(SettingPolicyFilter.newBuilder()
                .withType(Type.DEFAULT).build())))
        // Just return the same one twice - that's fine for the purpose of the test.
                .thenReturn(Stream.of(automationSettingPolicy));
       settingPolicyService.listSettingPolicies(ListSettingPoliciesRequest.newBuilder()
                .setTypeFilter(Type.DEFAULT).setContextId(111).build(), responseObserver);

       Setting newSetting = automationSettingPolicy.toBuilder().getInfoBuilder().getSettingsBuilder(0)
                .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(ActionMode.AUTOMATIC.toString())
                        .build()).build();
       SettingPolicy newPolicy = SettingPolicy.newBuilder().setId(100L).setInfo(SettingPolicyInfo
                .newBuilder().setName("automationSettingPolicy").addSettings(newSetting).build()).build();

       verify(responseObserver, times(1)).onNext(eq(newPolicy));
       verify(responseObserver).onCompleted();
    }

    @Test
    public void testListPoliciesTypeFilter() {
        final StreamObserver<SettingPolicy> responseObserver =
                (StreamObserver<SettingPolicy>)mock(StreamObserver.class);
        when(settingStore.getSettingPolicies(eq(SettingPolicyFilter.newBuilder()
                .withType(Type.DEFAULT)
                .build())))
            // Just return the same one twice - that's fine for the purpose of the test.
            .thenReturn(Stream.of(settingPolicy, settingPolicy));
        settingPolicyService.listSettingPolicies(ListSettingPoliciesRequest.newBuilder()
                    .setTypeFilter(Type.DEFAULT)
                    .build(),
                responseObserver);

        verify(responseObserver, times(2)).onNext(eq(settingPolicy));
        verify(responseObserver).onCompleted();
    }

    @Test
    public void testUploadEntitySettingsMissingArguments() {
        final StreamObserver<UploadEntitySettingsResponse> responseObserver =
                (StreamObserver<UploadEntitySettingsResponse>)mock(StreamObserver.class);

        settingPolicyService.uploadEntitySettings(UploadEntitySettingsRequest.newBuilder()
                .build(), responseObserver);

        final ArgumentCaptor<StatusException> exceptionCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(exceptionCaptor.capture());

        StatusException exception = exceptionCaptor.getValue();
        assertThat(exception, GrpcExceptionMatcher
                .hasCode(Code.INVALID_ARGUMENT)
                .descriptionContains("Missing topologyId and/or topologyContexId argument!"));
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
                                        .setSettingPolicyId(settingPolicy.getId())
                                        .build())
                                .collect(Collectors.toList()))
                    .build();

        List<EntitySettings> esList = new LinkedList<>();
        esList.add(es);

        long topologyId = 1111;
        long topologyContexId = 7777;
        UploadEntitySettingsRequest.Builder request =
            UploadEntitySettingsRequest.newBuilder()
                .setTopologyId(topologyId)
                .setTopologyContextId(topologyContexId)
                .addAllEntitySettings(esList);

        final ArgumentCaptor<UploadEntitySettingsResponse> responseCaptor =
                ArgumentCaptor.forClass(UploadEntitySettingsResponse.class);
        settingPolicyService.uploadEntitySettings(request.build(), responseObserver);
        verify(responseObserver).onNext(responseCaptor.capture());
        verify(responseObserver).onCompleted();
    }

    @Test
    public void testGetEntitySettings() throws NoSettingsForTopologyException {
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
                .setSettingPolicyId(1L)
                .build();

        when(entitySettingStore.getEntitySettings(eq(topologyFilter), eq(settingsFilter)))
                .thenReturn(ImmutableMap.of(7L, Collections.singletonList(settingToPolicyId)));

        when(settingStore.getSettingPolicies(any())).thenReturn(
                Stream.of(SettingPolicy.newBuilder()
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
        final List<SettingsForEntity> settingList = response.getSettingsList();
        assertEquals(1, settingList.size());
        final SettingsForEntity settingsForEntity = settingList.get(0);
        assertEquals(7L, settingsForEntity.getEntityId());
        assertThat(settingsForEntity.getSettingsList().stream()
                        .map(SettingToPolicyName::getSetting)
                        .collect(Collectors.toList()),
                containsInAnyOrder(settingToPolicyId.getSetting()));
    }

    @Test
    public void testGetEntitySettingsEntityNotFound() throws NoSettingsForTopologyException {
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

        final ArgumentCaptor<GetEntitySettingsResponse> respCaptor =
                ArgumentCaptor.forClass(GetEntitySettingsResponse.class);
        verify(responseObserver).onNext(respCaptor.capture());
        verify(responseObserver).onCompleted();

        final GetEntitySettingsResponse response = respCaptor.getValue();
        final List<SettingsForEntity> settingList = response.getSettingsList();
        assertEquals(1, settingList.size());
        final SettingsForEntity settingsForEntity = settingList.get(0);
        assertEquals(7L, settingsForEntity.getEntityId());
        assertTrue(settingsForEntity.getSettingsList().isEmpty());
    }

    @Test
    public void testGetEntitySettingsTopologyNotFound() throws NoSettingsForTopologyException {
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

}
