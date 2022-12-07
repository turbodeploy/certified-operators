package com.vmturbo.cost.component.scope;

import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import java.time.Instant;
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

    private static final long ALIAS_OID = 111111L;
    private static final long REAL_OID = 7777777L;
    private static final long BROADCAST_TIME_UTC_MS = 1668046796000L;

    private AliasedOidsServiceBlockingStub client;
    private final SqlOidMappingStore sqlOidMappingStore = mock(SqlOidMappingStore.class);
    private final UploadAliasedOidsRpcService uploadAliasedOidsRpcService
        = new UploadAliasedOidsRpcService(sqlOidMappingStore);

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
     * is invoked with unset parameters in {@link UploadAliasedOidsRequest} {@link Exception} is thrown.
     */
    @Test
    public void testUploadAliasOidsUnsetBroadcastTimeUtcMs() {
        Assert.assertThrows(RuntimeException.class,
            () -> client.uploadAliasedOids(UploadAliasedOidsRequest.newBuilder().build()));
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
        Mockito.verify(sqlOidMappingStore, times(0)).registerOidMappings(anyCollectionOf(OidMapping.class));
    }

    /**
     * Test that {@link UploadAliasedOidsRpcService#uploadAliasedOids(UploadAliasedOidsRequest, StreamObserver)} invoked
     * with a populated {@link UploadAliasedOidsRequest#getAliasIdToRealIdMap()} results in
     * {@link SqlOidMappingStore#registerOidMappings(Collection)} being invoked with the correct argument.
     */
    @Test
    public void testUploadAliasOidsValidEntries() {
        client.uploadAliasedOids(UploadAliasedOidsRequest.newBuilder()
            .setBroadcastTimeUtcMs(BROADCAST_TIME_UTC_MS)
                .putAliasIdToRealId(ALIAS_OID, REAL_OID)
            .build());
        Mockito.verify(sqlOidMappingStore).registerOidMappings(Collections.singleton(ImmutableOidMapping.builder()
            .oidMappingKey(ImmutableOidMappingKey.builder()
                .aliasOid(ALIAS_OID)
                .realOid(REAL_OID)
                .build())
                .firstDiscoveredTimeMsUtc(Instant.ofEpochMilli(BROADCAST_TIME_UTC_MS))
            .build()));
    }

    /**
     * Test that when {@link SqlOidMappingStore#registerOidMappings(Collection)} throws an {@link Exception}, then
     * {@link Exception} is thrown on
     * {@link UploadAliasedOidsRpcService#uploadAliasedOids(UploadAliasedOidsRequest, StreamObserver)}.
     */
    @Test
    public void testUploadAliasOidsExceptionFromSqlOidMappingStore() {
        Mockito.doThrow(Exception.class).when(sqlOidMappingStore)
            .registerOidMappings(anyCollectionOf(OidMapping.class));
        Assert.assertThrows(RuntimeException.class,
            () -> client.uploadAliasedOids(UploadAliasedOidsRequest.newBuilder()
                    .putAliasIdToRealId(ALIAS_OID, REAL_OID)
                    .setBroadcastTimeUtcMs(BROADCAST_TIME_UTC_MS)
                .build()));
    }
}