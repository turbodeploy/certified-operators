package com.vmturbo.cost.component.scope;

import static com.vmturbo.cost.component.scope.ScopeIdReplacementTestUtils.ALIAS_OID;
import static com.vmturbo.cost.component.scope.ScopeIdReplacementTestUtils.BROADCAST_TIME_UTC_MS;
import static com.vmturbo.cost.component.scope.ScopeIdReplacementTestUtils.REAL_OID;
import static com.vmturbo.cost.component.scope.ScopeIdReplacementTestUtils.createOidMapping;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import java.util.Collection;
import java.util.Collections;

import io.grpc.stub.StreamObserver;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cost.AliasedOidsServiceGrpc;
import com.vmturbo.common.protobuf.cost.AliasedOidsServiceGrpc.AliasedOidsServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.AliasedOidsServices.UploadAliasedOidsRequest;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Unit tests for {@link UploadAliasedOidsRpcService}.
 */
public class UploadAliasedOidsRpcServiceTest {

    private AliasedOidsServiceBlockingStub client;
    private final SqlOidMappingStore sqlOidMappingStore = mock(SqlOidMappingStore.class);
    private final SqlCloudCostScopeIdReplacementLog cloudCostScopeIdReplacementLog
        = mock(SqlCloudCostScopeIdReplacementLog.class);
    private final UploadAliasedOidsRpcService uploadAliasedOidsRpcService = new UploadAliasedOidsRpcService(
        sqlOidMappingStore, Collections.singletonList(cloudCostScopeIdReplacementLog));

    /**
     * Grpc service stub initialization.
     */
    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(uploadAliasedOidsRpcService);

    /**
     * Initialize test resources.
     */
    @Before
    public void setup() {
        client = AliasedOidsServiceGrpc.newBlockingStub(testServer.getChannel());
    }

    /**
     * Test that when {@link UploadAliasedOidsRpcService#uploadAliasedOids(UploadAliasedOidsRequest, StreamObserver)}
     * is invoked with unset parameters in {@link UploadAliasedOidsRequest},
     * {@link SqlOidMappingStore#registerOidMappings(Collection)} is not invoked.
     */
    @Test
    public void testUploadAliasOidsUnsetBroadcastTimeUtcMs() {
        client.uploadAliasedOids(UploadAliasedOidsRequest.newBuilder().build());
        Mockito.verify(sqlOidMappingStore, never()).registerOidMappings(anyCollectionOf(OidMapping.class));
    }

    /**
     * Test that {@link SqlOidMappingStore#registerOidMappings(Collection)} is not invoked if
     * {@link UploadAliasedOidsRequest#getAliasIdToRealIdMap()} is empty.
     */
    @Test
    public void testUploadAliasOidsEmptyMap() {
        client.uploadAliasedOids(UploadAliasedOidsRequest.newBuilder()
                .setBroadcastTimeUtcMs(BROADCAST_TIME_UTC_MS)
            .build());
        Mockito.verify(sqlOidMappingStore, never()).registerOidMappings(anyCollectionOf(OidMapping.class));
    }

    /**
     * Test that {@link UploadAliasedOidsRpcService#uploadAliasedOids(UploadAliasedOidsRequest, StreamObserver)} invoked
     * with a populated {@link UploadAliasedOidsRequest#getAliasIdToRealIdMap()} results in
     * {@link SqlOidMappingStore#registerOidMappings(Collection)} being invoked with the correct argument.
     */
    @Test
    public void testUploadAliasOidsValidEntries() {
        client.uploadAliasedOids(sampleRequest());
        Mockito.verify(sqlOidMappingStore).registerOidMappings(Collections.singleton(
            createOidMapping(ALIAS_OID, REAL_OID, BROADCAST_TIME_UTC_MS)));
    }

    /**
     * Test that even when {@link SqlOidMappingStore#registerOidMappings(Collection)} throws an {@link Exception},
     * {@link ScopeIdReplacementLog#persistScopeIdReplacements()} is invoked.
     */
    @Test
    public void testUploadAliasOidsExceptionFromSqlOidMappingStore() throws Exception {
        Mockito.doThrow(Exception.class).when(sqlOidMappingStore)
            .registerOidMappings(anyCollectionOf(OidMapping.class));
        client.uploadAliasedOids(sampleRequest());
        Mockito.verify(cloudCostScopeIdReplacementLog, times(1)).persistScopeIdReplacements();
    }

    /**
     * Test that when {@link ScopeIdReplacementLog#persistScopeIdReplacements()} throws an {@link Exception}, the client
     * receives a valid response.
     *
     * @throws Exception if {@link ScopeIdReplacementLog#persistScopeIdReplacements()} throws an exception.
     */
    @Test
    public void testUploadAliasOidsExceptionFromPersistScopeIdReplacement() throws Exception {
        Mockito.when(cloudCostScopeIdReplacementLog.persistScopeIdReplacements()).thenThrow(new Exception());
        Assert.assertNotNull(client.uploadAliasedOids(sampleRequest()));
    }

    private UploadAliasedOidsRequest sampleRequest() {
        return UploadAliasedOidsRequest.newBuilder()
            .putAliasIdToRealId(ALIAS_OID, REAL_OID)
            .setBroadcastTimeUtcMs(BROADCAST_TIME_UTC_MS)
            .build();
    }
}