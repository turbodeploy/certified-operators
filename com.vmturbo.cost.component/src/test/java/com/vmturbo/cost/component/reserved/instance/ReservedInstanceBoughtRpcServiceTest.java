package com.vmturbo.cost.component.reserved.instance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.hamcrest.Matchers.is;

import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageResponse;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;

public class ReservedInstanceBoughtRpcServiceTest {

    private ReservedInstanceBoughtStore reservedInstanceBoughtStore =
            mock(ReservedInstanceBoughtStore.class);

    private EntityReservedInstanceMappingStore reservedInstanceMappingStore =
            mock(EntityReservedInstanceMappingStore.class);

    private ReservedInstanceBoughtRpcService service = new ReservedInstanceBoughtRpcService(
            reservedInstanceBoughtStore,
            reservedInstanceMappingStore);
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(service);

    private ReservedInstanceBoughtServiceBlockingStub client;

    @Before
    public void setup() {
        client = ReservedInstanceBoughtServiceGrpc.newBlockingStub(grpcServer.getChannel());
    }

    @Test
    public void testGetRiCoverage() {
        final Map<Long, EntityReservedInstanceCoverage> coverageMap =
                ImmutableMap.of(7L, EntityReservedInstanceCoverage.getDefaultInstance());
        when(reservedInstanceMappingStore.getEntityRiCoverage())
                .thenReturn(coverageMap);
        final GetEntityReservedInstanceCoverageResponse response =
            client.getEntityReservedInstanceCoverage(GetEntityReservedInstanceCoverageRequest.getDefaultInstance());

        assertThat(response.getCoverageByEntityIdMap(), is(coverageMap));
    }
}
