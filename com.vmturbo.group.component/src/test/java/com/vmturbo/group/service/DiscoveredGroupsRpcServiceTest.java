package com.vmturbo.group.service;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.GroupDTO.ClusterInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupInfo;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredGroupsResponse;
import com.vmturbo.components.common.health.HealthStatus;
import com.vmturbo.group.group.GroupStore;
import com.vmturbo.group.policy.PolicyStore;
import com.vmturbo.group.setting.SettingStore;
import com.vmturbo.sql.utils.TestSQLDatabaseConfig;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(
        loader = AnnotationConfigContextLoader.class,
        classes = {TestSQLDatabaseConfig.class}
)
@TestPropertySource(properties = {"originalSchemaName=group_component"})
public class DiscoveredGroupsRpcServiceTest {

    @Autowired
    private TestSQLDatabaseConfig dbConfig;

    private GroupStore groupStore = mock(GroupStore.class);

    private PolicyStore policyStore = mock(PolicyStore.class);

    private SettingStore settingStore = mock(SettingStore.class);

    private DiscoveredGroupsRpcService service;

    @Mock
    private StreamObserver<StoreDiscoveredGroupsResponse> responseObserver;

    @Captor
    private ArgumentCaptor<StatusException> exceptionCaptor;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        service = new DiscoveredGroupsRpcService(dbConfig.dsl(), groupStore, policyStore, settingStore);
    }

    @Test
    public void testNoTargetId() {
        service.storeDiscoveredGroups(StoreDiscoveredGroupsRequest.getDefaultInstance(),
                responseObserver);
        verify(responseObserver).onError(exceptionCaptor.capture());
        final StatusException e = exceptionCaptor.getValue();
        assertEquals(Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
    }

    @Test
    public void testUpdateClusters() throws Exception {
        final HealthStatus status = mock(HealthStatus.class);
        when(status.isHealthy()).thenReturn(true);

        service.storeDiscoveredGroups(StoreDiscoveredGroupsRequest.newBuilder()
                .setTargetId(10L)
                .addDiscoveredGroup(GroupInfo.getDefaultInstance())
                .addDiscoveredCluster(ClusterInfo.getDefaultInstance())
                .build(), responseObserver);

        verify(responseObserver).onCompleted();
        verify(groupStore).updateTargetGroups(isA(DSLContext.class), eq(10L),
                eq(Collections.singletonList(GroupInfo.getDefaultInstance())),
                eq(Collections.singletonList(ClusterInfo.getDefaultInstance())));

        verify(policyStore).updateTargetPolicies(isA(DSLContext.class), eq(10L), eq(Collections.emptyList()), anyMap());
        verify(settingStore).updateTargetSettingPolicies(isA(DSLContext.class), eq(10L), eq(Collections.emptyList()), anyMap());
    }
}
