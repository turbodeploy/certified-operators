package com.vmturbo.cost.component.pricing;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Pricing.BusinessAccountPriceTableKey;
import com.vmturbo.common.protobuf.cost.Pricing.GetAccountPriceTableRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetAccountPriceTableResponse;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableChecksumRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableChecksumResponse;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableRequest;
import com.vmturbo.common.protobuf.cost.Pricing.GetPriceTableResponse;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTableKey;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstanceSpecPrice;
import com.vmturbo.common.protobuf.cost.Pricing.UploadAccountPriceTableKeyRequest;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CommonCost.PaymentOption;
import com.vmturbo.platform.sdk.common.PricingDTO;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.ReservedInstancePrice;

/**
 * Test class for the PricingRpc Service.
 */
public class PricingRpcServiceTest {

    private static final String PROBE_TYPE = "foo";

    private static final PriceTable PRICE_TABLE = PriceTable.newBuilder()
            .putOnDemandPriceByRegionId(7L, OnDemandPriceTable.getDefaultInstance())
            .build();

    private static final PriceTableKey PRICE_TABLE_KEY = PriceTableKey.newBuilder()
            .putProbeKeyMaterial("enrollmentId", "123")
            .putProbeKeyMaterial("offerId", "234")
            .setServiceProviderId(12345L).build();

    private PriceTableStore priceTableStore = mock(PriceTableStore.class);

    private ReservedInstanceSpecStore riSpecStore = mock(ReservedInstanceSpecStore.class);

    private BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore = mock(BusinessAccountPriceTableKeyStore.class);

    private ReservedInstanceBoughtStore reservedInstanceBoughtStore = mock(ReservedInstanceBoughtStore.class);

    private PricingRpcService backend = new PricingRpcService(priceTableStore, riSpecStore, reservedInstanceBoughtStore, businessAccountPriceTableKeyStore);

    /**
     * Create a grpc instance of the pricing rpc service.
     */
    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(backend);

    private PricingServiceBlockingStub pricingServiceBlockingStub;

    private PricingServiceStub pricingServiceStub;

    /**
     * An instance of the expected expection.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Setup for the test class.
     */
    @Before
    public void setup() {
        pricingServiceBlockingStub = PricingServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        pricingServiceStub = PricingServiceGrpc.newStub(grpcTestServer.getChannel());

    }

    /**
     * Test the getter for the price table.
     */
    @Test
    public void testGet() {
        final PriceTable priceTable = PriceTable.newBuilder()
                .putOnDemandPriceByRegionId(7L, OnDemandPriceTable.getDefaultInstance())
                .build();
        when(priceTableStore.getMergedPriceTable()).thenReturn(priceTable);
        final GetPriceTableResponse response = pricingServiceBlockingStub.getPriceTable(
                GetPriceTableRequest.newBuilder()
                        .build());
        assertTrue(response.hasGlobalPriceTable());
        assertThat(response.getGlobalPriceTable(), is(priceTable));
    }

    /**
     * Test the price table checksum.
     */
    @Test
    public void testGetPriceTableChecksum() {
        final PriceTableKey priceTableKey = PRICE_TABLE_KEY;
        Map<PriceTableKey, Long> test = new HashMap<>();
        test.put(priceTableKey, 99999L);
        when(priceTableStore.getChecksumByPriceTableKeys(anyList())).thenReturn(test);
        GetPriceTableChecksumResponse getPriceTableChecksumResponse =
                pricingServiceBlockingStub.getPriceTableChecksum(
                        GetPriceTableChecksumRequest.newBuilder().addPriceTableKey(PRICE_TABLE_KEY)
                                .build());
        assertEquals(getPriceTableChecksumResponse
                .getPricetableToChecksum(0).getPriceTableKey(), priceTableKey);
    }

    /**
     * Test the updateRISpecsAndBuildRIPriceTable method which takes a list of {@Link ReservedInstanceSpecPrice}
     * and generates a {@link ReservedInstancePriceTable}.
     * The list contains ReservedInstanceSpecPrice that have different ReservedInstanceSpecInfo with
     * the same ReservedInstancePrice.
     * A ReservedInstanceSpecPrice consists ReservedInstanceSpecInfo and common_dto.ReservedInstancePrice.
     * A ReservedInstancePriceTable contains a map from ReservedInstanceSpec OID to ReservedInstanceSpecPrice.
     */
    @Test
    public void updateRISpecsAndBuildRIPriceTableTest() {

        // build ReservedInstanceSpecInfo
        final boolean SIZE_FLEXIBLE_FALSE = false;
        final boolean SIZE_FLEXIBLE_TRUE = true;

        final long REGION_AWS_OHIO_OID = 500L;
        final long REGION_AWS_OREGON_OID = 5100L;

        final long COMPUTE_TIER_M5_LARGE_OID = 4204;
        final String COMPUTE_TIER_M5_FAMILY = "m5";
        final long COMPUTE_TIER_T2_NANO_OID = 4110;
        final long COMPUTE_TIER_T2_MICRO_OID = 4111;
        final String COMPUTE_TIER_T2_FAMILY = "t2";

        final ReservedInstanceType RESERVED_INSTANCE_TYPE_1 = ReservedInstanceType.newBuilder()
            .setOfferingClass(OfferingClass.STANDARD)
            .setPaymentOption(PaymentOption.NO_UPFRONT)
            .setTermYears(1).build();
        final ReservedInstanceType RESERVED_INSTANCE_TYPE_2 = ReservedInstanceType.newBuilder()
            .setOfferingClass(OfferingClass.CONVERTIBLE)
            .setPaymentOption(PaymentOption.ALL_UPFRONT)
            .setTermYears(3).build();

        final ReservedInstanceSpecInfo RI_SPEC_INFO_1 = ReservedInstanceSpecInfo.newBuilder()
            .setOs(OSType.LINUX)
            .setTenancy(Tenancy.DEFAULT)
            .setTierId(COMPUTE_TIER_M5_LARGE_OID)
            .setRegionId(REGION_AWS_OHIO_OID)
            .setType(RESERVED_INSTANCE_TYPE_1)
            .setSizeFlexible(SIZE_FLEXIBLE_TRUE)
            .build();
        final ReservedInstanceSpecInfo RI_SPEC_INFO_2 = ReservedInstanceSpecInfo.newBuilder()
            .setOs(OSType.LINUX)
            .setTenancy(Tenancy.DEFAULT)
            .setTierId(COMPUTE_TIER_M5_LARGE_OID)
            .setRegionId(REGION_AWS_OREGON_OID)
            .setType(RESERVED_INSTANCE_TYPE_1)
            .setSizeFlexible(SIZE_FLEXIBLE_TRUE)
            .build();
        final ReservedInstanceSpecInfo RI_SPEC_INFO_3 = ReservedInstanceSpecInfo.newBuilder()
            .setOs(OSType.WINDOWS)
            .setTenancy(Tenancy.DEDICATED)
            .setTierId(COMPUTE_TIER_T2_MICRO_OID)
            .setRegionId(REGION_AWS_OREGON_OID)
            .setType(RESERVED_INSTANCE_TYPE_2)
            .setSizeFlexible(SIZE_FLEXIBLE_FALSE)
            .build();

        // build ReservedInstancePrice
        final Unit UNIT_HOURS = Unit.HOURS;
        final Unit UNIT_TOTAL = Unit.TOTAL;

        final CurrencyAmount HOURLY_CURRENCY_AMOUNT_1 = CurrencyAmount.newBuilder().setAmount(1D).build();
        final CurrencyAmount TOTAL_CURRENCY_AMOUNT_1 = CurrencyAmount.newBuilder().setAmount(100d).build();

        final Price PRICE_HOURLY_1 = Price.newBuilder()
            .setUnit(UNIT_HOURS)
            .setPriceAmount(HOURLY_CURRENCY_AMOUNT_1).build();
        final Price PRICE_UPFRONT_1 = Price.newBuilder()
            .setUnit(UNIT_TOTAL)
            .setPriceAmount(TOTAL_CURRENCY_AMOUNT_1).build();

        ReservedInstancePrice riPrice1 = ReservedInstancePrice.newBuilder()
            .setRecurringPrice(PRICE_HOURLY_1).build();

        ReservedInstancePrice riPrice2 = ReservedInstancePrice.newBuilder()
            .setUpfrontPrice(PRICE_UPFRONT_1).build();

        // build the ReservedInstanceSpecStore mapping from temp spec ID to real ID.
        final long RESERVED_INSTANCE_SPEC_ID_1 = 100001L;
        final long RESERVED_INSTANCE_SPEC_ID_2 = 100002L;
        final long RESERVED_INSTANCE_SPEC_ID_3 = 100003L;
        Map<Long, Long> tempSpecIdToRealId = new HashMap<>();
        tempSpecIdToRealId.put(0L, RESERVED_INSTANCE_SPEC_ID_1);
        tempSpecIdToRealId.put(1L, RESERVED_INSTANCE_SPEC_ID_2);
        tempSpecIdToRealId.put(2L, RESERVED_INSTANCE_SPEC_ID_3);

        // build list of ReservedInstanceSpecPrice for the input
        ReservedInstanceSpecPrice specPrice1 = ReservedInstanceSpecPrice.newBuilder()
            .setRiSpecInfo(RI_SPEC_INFO_1)
            .setPrice(riPrice1).build();
        ReservedInstanceSpecPrice specPrice2 = ReservedInstanceSpecPrice.newBuilder()
            .setRiSpecInfo(RI_SPEC_INFO_2)
            .setPrice(riPrice1).build();
        ReservedInstanceSpecPrice specPrice3 = ReservedInstanceSpecPrice.newBuilder()
            .setRiSpecInfo(RI_SPEC_INFO_3)
            .setPrice(riPrice2).build();
        List<ReservedInstanceSpecPrice> specPriceList = Arrays.asList(specPrice1, specPrice2, specPrice3);

        ReservedInstanceSpecStore riSpecstore = Mockito.spy(ReservedInstanceSpecStore.class);
        ReservedInstanceBoughtStore reservedInstanceBoughtStore = mock(ReservedInstanceBoughtStore.class);
        Mockito.doReturn(tempSpecIdToRealId).when(riSpecstore).updateReservedInstanceSpec(Mockito.anyList());

        // generate the PriceTableStore
        PriceTableStore priceTableStore = Mockito.spy(PriceTableStore.class);
        PricingRpcService service = new PricingRpcService(priceTableStore, riSpecstore, reservedInstanceBoughtStore,
                businessAccountPriceTableKeyStore);
        ReservedInstancePriceTable riPriceTable = service.updateRISpecsAndBuildRIPriceTable(specPriceList);

        // get the map from ReservedInstanceSpec OID to ReservedInstancePrice
        Map<Long, PricingDTO.ReservedInstancePrice> riPrices = riPriceTable.getRiPricesBySpecIdMap();

        Assert.assertTrue(riPrices.get(RESERVED_INSTANCE_SPEC_ID_1) != null);
        Assert.assertTrue(riPrices.get(RESERVED_INSTANCE_SPEC_ID_1).equals(riPrice1));
        Assert.assertTrue(riPrices.get(RESERVED_INSTANCE_SPEC_ID_2) != null);
        Assert.assertTrue(riPrices.get(RESERVED_INSTANCE_SPEC_ID_2).equals(riPrice1));
        Assert.assertTrue(riPrices.get(RESERVED_INSTANCE_SPEC_ID_3) != null);
        Assert.assertTrue(riPrices.get(RESERVED_INSTANCE_SPEC_ID_3).equals(riPrice2));
    }

    /**
     * Test upload of BA oid to {@link PriceTableKey} mappings to DB.
     */
    @Test
    public void testUploadBusinessAccountPriceTableKey() {
        BusinessAccountPriceTableKey businessAccountPriceTableKey = BusinessAccountPriceTableKey
                .newBuilder().putBusinessAccountPriceTableKey(123, PRICE_TABLE_KEY).build();
        UploadAccountPriceTableKeyRequest uploadAccountPriceTableKeyRequest = UploadAccountPriceTableKeyRequest.newBuilder()
                .setBusinessAccountPriceTableKey(businessAccountPriceTableKey).build();
        pricingServiceBlockingStub.uploadAccountPriceTableKeys(uploadAccountPriceTableKeyRequest);
        verify(businessAccountPriceTableKeyStore).uploadBusinessAccount(any());
    }

    /**
     * Test fetch of BA oid to {@link PriceTableKey} mapping indexed by BA OIDs.
     * See {@link BusinessAccountPriceTableKey }.
     */
    @Test
    public void fetchBusinessAccountPriceTableKey() {
        Map<Long, Long> expected = Maps.newHashMap(ImmutableMap.of(123L, 456L));
        when(businessAccountPriceTableKeyStore.fetchPriceTableKeyOidsByBusinessAccount(Collections.emptySet()))
                .thenReturn(expected);
        GetAccountPriceTableResponse getAccountPriceTableKeyResponse = pricingServiceBlockingStub
                .getAccountPriceTable(GetAccountPriceTableRequest.newBuilder().build());
        verify(businessAccountPriceTableKeyStore, times(1)).fetchPriceTableKeyOidsByBusinessAccount(any());
        assertEquals("BA oid to priceTableKey fetch mismatch",
                getAccountPriceTableKeyResponse.getBusinessAccountPriceTableKeyMap(),
                expected);
    }
}
