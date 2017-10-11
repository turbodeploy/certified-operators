package com.vmturbo.group.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.BooleanSettingValueType;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.CreateSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingSpec;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPolicyRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSettingPolicyResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.ListSettingPoliciesRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicy.Type;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingPolicyInfo;
import com.vmturbo.common.protobuf.setting.SettingProto.SettingSpec;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.group.persistent.DuplicateNameException;
import com.vmturbo.group.persistent.InvalidSettingPolicyException;
import com.vmturbo.group.persistent.SettingPolicyFilter;
import com.vmturbo.group.persistent.SettingStore;

public class SettingPolicyRpcServiceTest {

    private SettingStore settingStore = mock(SettingStore.class);

    private SettingSpec settingSpec = SettingSpec.newBuilder()
            .setName("doStuff")
            .setEntitySettingSpec(EntitySettingSpec.getDefaultInstance())
            .setBooleanSettingValueType(BooleanSettingValueType.getDefaultInstance())
            .build();

    private SettingPolicyInfo settingPolicyInfo = SettingPolicyInfo.newBuilder()
            .setName("name")
            .addSettings(Setting.newBuilder()
                    .setSettingSpecName(settingSpec.getName())
                    .setBooleanSettingValue(BooleanSettingValue.newBuilder().setValue(true)))
            .build();

    private SettingPolicy settingPolicy = SettingPolicy.newBuilder()
            .setId(7L)
            .setInfo(settingPolicyInfo)
            .build();

    private final SettingPolicyRpcService service = new SettingPolicyRpcService(settingStore);

    @Test
    public void testCreatePolicy() throws Exception {
        final StreamObserver<CreateSettingPolicyResponse> responseObserver =
                (StreamObserver<CreateSettingPolicyResponse>)mock(StreamObserver.class);
        when(settingStore.createSettingPolicy(eq(settingPolicyInfo)))
                .thenReturn(settingPolicy);

        service.createSettingPolicy(CreateSettingPolicyRequest.newBuilder()
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
        service.createSettingPolicy(CreateSettingPolicyRequest.newBuilder()
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
        when(settingStore.createSettingPolicy(eq(settingPolicyInfo)))
                .thenThrow(new InvalidSettingPolicyException(errorMsg));

        service.createSettingPolicy(CreateSettingPolicyRequest.newBuilder()
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
        when(settingStore.createSettingPolicy(eq(settingPolicyInfo)))
                .thenThrow(new DuplicateNameException(1, "foo"));

        service.createSettingPolicy(CreateSettingPolicyRequest.newBuilder()
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
    public void testGetPolicyNotFound() throws Exception {
        final StreamObserver<GetSettingPolicyResponse> responseObserver =
                (StreamObserver<GetSettingPolicyResponse>)mock(StreamObserver.class);

        when(settingStore.getSettingPolicy(anyLong())).thenReturn(Optional.empty());
        service.getSettingPolicy(GetSettingPolicyRequest.newBuilder()
                .setId(7L)
                .build(), responseObserver);

        verify(responseObserver).onNext(eq(GetSettingPolicyResponse.getDefaultInstance()));
    }

    @Test
    public void testGetPolicyByName() throws Exception {
        final StreamObserver<GetSettingPolicyResponse> responseObserver =
                (StreamObserver<GetSettingPolicyResponse>)mock(StreamObserver.class);

        when(settingStore.getSettingPolicy(eq("name"))).thenReturn(Optional.of(settingPolicy));
        service.getSettingPolicy(GetSettingPolicyRequest.newBuilder()
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
        service.getSettingPolicy(GetSettingPolicyRequest.newBuilder()
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
        when(settingStore.getSettingSpec(settingSpec.getName()))
            .thenReturn(Optional.of(settingSpec));
        service.getSettingPolicy(GetSettingPolicyRequest.newBuilder()
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
        when(settingStore.getSettingSpec(settingSpec.getName()))
                .thenReturn(Optional.empty());
        service.getSettingPolicy(GetSettingPolicyRequest.newBuilder()
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
        service.listSettingPolicies(ListSettingPoliciesRequest.getDefaultInstance(),
                responseObserver);

        verify(responseObserver, times(2)).onNext(eq(settingPolicy));
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
        service.listSettingPolicies(ListSettingPoliciesRequest.newBuilder()
                    .setTypeFilter(Type.DEFAULT)
                    .build(),
                responseObserver);

        verify(responseObserver, times(2)).onNext(eq(settingPolicy));
        verify(responseObserver).onCompleted();
    }

}
