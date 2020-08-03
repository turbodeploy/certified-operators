package com.vmturbo.action.orchestrator.rpc;

import static com.vmturbo.action.orchestrator.ActionOrchestratorTestUtils.buildCoreQuotaActionConstraintInfo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;

import com.vmturbo.action.orchestrator.action.constraint.ActionConstraintStoreFactory;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintType;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.UploadActionConstraintInfoRequest;

/**
 * Tests for rpc methods in {@link ActionConstraintsRpcService}.
 */
public class ActionConstraintsRpcServiceTest {

    private ActionConstraintsRpcService actionConstraintsRpcService;

    private final ActionConstraintStoreFactory actionConstraintStoreFactory =
        mock(ActionConstraintStoreFactory.class);

    /**
     * Initialization of each test.
     */
    @Before
    public void setup() {
        actionConstraintsRpcService = new ActionConstraintsRpcService(actionConstraintStoreFactory);
    }

    /**
     * Test {@link ActionConstraintsRpcService#uploadActionConstraintInfo(StreamObserver)}.
     */
    @Test
    public void testUploadActionConstraintInfo() {
        final StreamObserver<Empty> responseObserver =
            (StreamObserver<Empty>)mock(StreamObserver.class);

        StreamObserver<UploadActionConstraintInfoRequest> requestObserver =
            actionConstraintsRpcService.uploadActionConstraintInfo(responseObserver);

        final long businessAccountId1 = 1;
        final long businessAccountId2 = 2;
        final long regionId1 = 11;
        final long regionId2 = 12;
        final String family1 = "family1";
        final String family2 = "family2";
        final int value = 100;

        buildCoreQuotaActionConstraintInfo(
            Collections.singletonList(businessAccountId1), Arrays.asList(regionId1, regionId2),
            Arrays.asList(family1, family2), value);

        requestObserver.onNext(UploadActionConstraintInfoRequest.newBuilder()
            .addActionConstraintInfo(buildCoreQuotaActionConstraintInfo(
                Collections.singletonList(businessAccountId1), Arrays.asList(regionId1, regionId2),
                Arrays.asList(family1, family2), value)).build());
        requestObserver.onNext(UploadActionConstraintInfoRequest.newBuilder()
            .addActionConstraintInfo(buildCoreQuotaActionConstraintInfo(
                Collections.singletonList(businessAccountId2), Arrays.asList(regionId1, regionId2),
                Arrays.asList(family1, family2), value)).build());
        requestObserver.onCompleted();

        verify(responseObserver, times(1)).onCompleted();
        verify(responseObserver, times(0)).onNext(any());
        verify(responseObserver, times(0)).onError(any());
        verify(actionConstraintStoreFactory).updateActionConstraintInfo(Collections.singletonMap(
            ActionConstraintType.CORE_QUOTA, buildCoreQuotaActionConstraintInfo(
                Arrays.asList(businessAccountId1, businessAccountId2),
                Arrays.asList(regionId1, regionId2),
                Arrays.asList(family1, family2), value)));
    }
}
