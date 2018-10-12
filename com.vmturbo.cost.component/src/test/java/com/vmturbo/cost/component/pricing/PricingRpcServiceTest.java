package com.vmturbo.cost.component.pricing;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.hamcrest.CoreMatchers.is;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableResponse;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.ProbePriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.UploadPriceTableRequest;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;

public class PricingRpcServiceTest {

    private static final String PROBE_TYPE = "foo";

    private static final PriceTable PRICE_TABLE = PriceTable.newBuilder()
            .putOnDemandPriceByRegionId(7L, OnDemandPriceTable.getDefaultInstance())
            .build();

    private PriceTableStore priceTableStore = mock(PriceTableStore.class);

    private PricingRpcService backend = new PricingRpcService(priceTableStore);

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(backend);

    private PricingServiceBlockingStub clientStub;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        clientStub = PricingServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
    }

    @Test
    public void testUpload() {
        final ProbePriceTable probePriceTable = ProbePriceTable.newBuilder()
                .setPriceTable(PRICE_TABLE)
                .build();
        clientStub.uploadPriceTable(UploadPriceTableRequest.newBuilder()
                .putProbePriceTables(PROBE_TYPE, probePriceTable)
                .build());

        Mockito.verify(priceTableStore).putProbePriceTables(ImmutableMap.of(PROBE_TYPE, probePriceTable));
    }

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
