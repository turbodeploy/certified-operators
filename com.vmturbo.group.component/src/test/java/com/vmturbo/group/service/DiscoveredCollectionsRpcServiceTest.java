package com.vmturbo.group.service;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredCollectionsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredCollectionsResponse;
import com.vmturbo.components.common.health.HealthStatus;
import com.vmturbo.components.common.health.HealthStatusProvider;
import com.vmturbo.group.persistent.ClusterStore;
import com.vmturbo.group.persistent.GroupStore;
import com.vmturbo.group.persistent.PolicyStore;

public class DiscoveredCollectionsRpcServiceTest {

    private GroupStore groupStore = mock(GroupStore.class);

    private PolicyStore policyStore = mock(PolicyStore.class);

    private ClusterStore clusterStore = mock(ClusterStore.class);

    private HealthStatusProvider compositeHealthMonitor = mock(HealthStatusProvider.class);

    private DiscoveredCollectionsRpcService service =
            new DiscoveredCollectionsRpcService(groupStore, policyStore, clusterStore, compositeHealthMonitor);

    @Mock
    private StreamObserver<StoreDiscoveredCollectionsResponse> responseObserver;

    @Captor
    private ArgumentCaptor<StatusException> exceptionCaptor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testNoTargetId() {
        service.storeDiscoveredCollections(StoreDiscoveredCollectionsRequest.getDefaultInstance(),
                responseObserver);
        verify(responseObserver).onError(exceptionCaptor.capture());
        final StatusException e = exceptionCaptor.getValue();
        assertEquals(Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
    }

    @Test
    public void testComponentUnhealthy() {
        final HealthStatus status = mock(HealthStatus.class);
        when(status.isHealthy()).thenReturn(false);
        when(compositeHealthMonitor.getHealthStatus()).thenReturn(status);

        service.storeDiscoveredCollections(StoreDiscoveredCollectionsRequest.newBuilder()
                .setTargetId(10L)
                .build(), responseObserver);
        verify(responseObserver).onError(exceptionCaptor.capture());
        final StatusException e = exceptionCaptor.getValue();
        assertEquals(Status.UNAVAILABLE.getCode(), e.getStatus().getCode());
    }

    @Test
    public void testUpdateClusters() throws Exception {
        final HealthStatus status = mock(HealthStatus.class);
        when(status.isHealthy()).thenReturn(true);
        when(compositeHealthMonitor.getHealthStatus()).thenReturn(status);

        service.storeDiscoveredCollections(StoreDiscoveredCollectionsRequest.newBuilder()
                .setTargetId(10L)
                .addDiscoveredGroup(GroupInfo.getDefaultInstance())
                .addDiscoveredCluster(ClusterInfo.getDefaultInstance())
                .build(), responseObserver);

        verify(responseObserver).onCompleted();
        verify(groupStore).updateTargetGroups(eq(10L),
                eq(Collections.singletonList(GroupInfo.getDefaultInstance())));
        verify(clusterStore).updateTargetClusters(eq(10L),
                eq(Collections.singletonList(ClusterInfo.getDefaultInstance())));
    }
}
