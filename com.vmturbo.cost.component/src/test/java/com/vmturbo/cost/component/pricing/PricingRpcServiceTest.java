package com.vmturbo.cost.component.pricing;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableResponse;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;

public class PricingRpcServiceTest {

    private static final String PROBE_TYPE = "foo";

    private static final PriceTable PRICE_TABLE = PriceTable.newBuilder()
            .putOnDemandPriceByRegionId(7L, OnDemandPriceTable.getDefaultInstance())
            .build();

    private PriceTableStore priceTableStore = mock(PriceTableStore.class);

    private ReservedInstanceSpecStore riSpecStore = mock(ReservedInstanceSpecStore.class);

    private PricingRpcService backend = new PricingRpcService(priceTableStore, riSpecStore);

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(backend);

    private PricingServiceBlockingStub clientStub;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        clientStub = PricingServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
    }

    // TODO: Patrick: I need to create a unit test for testUpdatePriceTables

    @Test
    public void testGet() {
        final PriceTable priceTable = PriceTable.newBuilder()
                .putOnDemandPriceByRegionId(7L, OnDemandPriceTable.getDefaultInstance())
                .build();
        when(priceTableStore.getMergedPriceTable()).thenReturn(priceTable);
        final GetPriceTableResponse response = clientStub.getPriceTable(
                GetPriceTableRequest.newBuilder()
                        .build());
        assertTrue(response.hasGlobalPriceTable());
        assertThat(response.getGlobalPriceTable(), is(priceTable));
    }
}
