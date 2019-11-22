package com.vmturbo.cost.component.reserved.instance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageResponse;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.utils.TimeFrameCalculator;

/**
 * Test the ReservedInstanceUtilizationCoverageRpcService public methods which get coverage
 * statistics.
 */
public class ReservedInstanceUtilizationCoverageRpcServiceTest {

    private ReservedInstanceUtilizationStore reservedInstanceUtilizationStore
        = mock(ReservedInstanceUtilizationStore.class);

    private ReservedInstanceCoverageStore reservedInstanceCoverageStore
        = mock(ReservedInstanceCoverageStore.class);

    private ProjectedRICoverageAndUtilStore projectedRICoverageStore
        = mock(ProjectedRICoverageAndUtilStore.class);

    private EntityReservedInstanceMappingStore entityReservedInstanceMappingStore
        = mock(EntityReservedInstanceMappingStore.class);

    private TimeFrameCalculator timeFrameCalculator = mock(TimeFrameCalculator.class);

    private Clock clock = mock(Clock.class);

    private ReservedInstanceUtilizationCoverageRpcService service =
        new ReservedInstanceUtilizationCoverageRpcService(
               reservedInstanceUtilizationStore,
               reservedInstanceCoverageStore,
               projectedRICoverageStore,
               entityReservedInstanceMappingStore,
               timeFrameCalculator,
               clock);

    /**
     * Set up a test GRPC server.
     */
    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(service);

    private ReservedInstanceUtilizationCoverageServiceBlockingStub client;

    /**
     * Set up the ReservedInstanceUtilizationCoverageRpcService stub for use in the tests.
     */
    @Before
    public void setup() {
        client = ReservedInstanceUtilizationCoverageServiceGrpc
                        .newBlockingStub(grpcServer.getChannel());
    }

    /**
     * Verify that the expected coverage Map is returned when we invoke the
     * getEntityReservedInstanceCoverage. We mock the underlying store's return value and expect
     * that return value to be passed back in the response.
     */
    @Test
    public void testGetEntityReservedInstanceCoverage() {
        final Map<Long, EntityReservedInstanceCoverage> coverageMap =
                ImmutableMap.of(7L, EntityReservedInstanceCoverage.getDefaultInstance());
        when(entityReservedInstanceMappingStore.getEntityRiCoverage())
                .thenReturn(coverageMap);
        final GetEntityReservedInstanceCoverageResponse response =
            client.getEntityReservedInstanceCoverage(
                GetEntityReservedInstanceCoverageRequest.getDefaultInstance());

        assertThat(response.getCoverageByEntityIdMap(), is(coverageMap));
    }
}
