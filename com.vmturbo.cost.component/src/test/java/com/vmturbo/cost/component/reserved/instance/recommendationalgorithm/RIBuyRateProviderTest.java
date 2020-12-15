package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.ReservedInstancePriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.component.pricing.BusinessAccountPriceTableKeyStore;
import com.vmturbo.cost.component.pricing.PriceTableStore;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.RIBuyRateProvider.PricingProviderResult;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.RIBuyRegionalContext;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList.ComputeTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.ReservedInstancePrice;

/**
 * Test the methods of the {@link RIBuyRateProvider} class.
 */
public class RIBuyRateProviderTest {

    private static final long BA1_OID = 1111L;
    private static final long PT1_OID = 11111L;

    private static final long PRICE_SPEC_ID1 = 11L;
    private static final long PRICE_SPEC_ID2 = 22L;

    private static final long TIER1_OID = 1L;
    private static final long TIER2_OID = 2L;

    private static final float ON_DEMAND_RATE = 100f;
    private static final float ON_DEMAND_RATE_ADJ = 50f;

    private static final float UPFRONT_PRICE = 8760f;
    private static final float RECURRING_PRICE = 2f;
    private static final double COMP_DELTA = 0.001f;

    private static final ReservedInstanceSpecInfo RI_SPEC_INFO =
            ReservedInstanceSpecInfo.newBuilder()
                    .setTenancy(Tenancy.DEFAULT)
                    .setOs(OSType.WINDOWS)
                    .build();

    private RIBuyRateProvider riBuyRateProvider;

    /**
     * Init {@link RIBuyRateProvider}.
     */
    @Before
    public void init() {
        BusinessAccountPriceTableKeyStore baPriceTableStore =
                mock(BusinessAccountPriceTableKeyStore.class);

        // create BA to PriceTable mapping
        Map<Long, Long> priceTableKeyOidByBusinessAccountOid =
                ImmutableMap.<Long, Long>builder().put(BA1_OID, PT1_OID).build();
        when(baPriceTableStore.fetchPriceTableKeyOidsByBusinessAccount(any())).thenReturn(
                priceTableKeyOidByBusinessAccountOid);

        final PriceTableStore priceTableStore = mock(PriceTableStore.class);
        mockOnDemandPrices(priceTableStore);
        mockRIPrices(priceTableStore);

        // create RIBuyRateProvider
        Set<Long> primaryAccounts = ImmutableSet.of(BA1_OID);
        riBuyRateProvider =
                new RIBuyRateProvider(priceTableStore, baPriceTableStore, primaryAccounts);
    }

    private void mockOnDemandPrices(final PriceTableStore priceTableStore) {
        //mock on-demand prices
        ComputeTierPriceList computeTierPriceList = ComputeTierPriceList.newBuilder()
                .setBasePrice(ComputeTierConfigPrice.newBuilder()
                        .addPrices(Price.newBuilder()
                                .setUnit(Unit.HOURS)
                                .setPriceAmount(
                                        CurrencyAmount.newBuilder().setAmount(ON_DEMAND_RATE))))
                .build();

        ComputeTierPriceList computeTierPriceList2 = ComputeTierPriceList.newBuilder()
                .setBasePrice(ComputeTierConfigPrice.newBuilder()
                        .addPrices(Price.newBuilder()
                                .setUnit(Unit.HOURS)
                                .setPriceAmount(
                                        CurrencyAmount.newBuilder().setAmount(ON_DEMAND_RATE))))
                .addPerConfigurationPriceAdjustments(ComputeTierConfigPrice.newBuilder()
                        .setGuestOsType(OSType.WINDOWS)
                        .setTenancy(Tenancy.DEFAULT)
                        .addPrices(Price.newBuilder()
                                .setUnit(Unit.HOURS)
                                .setPriceAmount(
                                        CurrencyAmount.newBuilder().setAmount(ON_DEMAND_RATE_ADJ))))
                .build();

        Map<Long, ComputeTierPriceList> computeTierPriceListMap =
                ImmutableMap.<Long, ComputeTierPriceList>builder().put(TIER1_OID,
                        computeTierPriceList).put(TIER2_OID, computeTierPriceList2).build();

        Map<Long, OnDemandPriceTable> onDemandPriceTableMap =
                ImmutableMap.<Long, OnDemandPriceTable>builder().put(PRICE_SPEC_ID1,
                        OnDemandPriceTable.newBuilder()
                                .putAllComputePricesByTierId(computeTierPriceListMap)
                                .build()).build();

        PriceTable priceTable = PriceTable.newBuilder()
                .putAllOnDemandPriceByRegionId(onDemandPriceTableMap)
                .build();

        Map<Long, PriceTable> priceTableMap =
                ImmutableMap.<Long, PriceTable>builder().put(PT1_OID, priceTable).build();

        when(priceTableStore.getPriceTables(any())).thenReturn(priceTableMap);
    }

    private void mockRIPrices(final PriceTableStore priceTableStore) {
        //mock RI prices
        ReservedInstancePrice riPrice = ReservedInstancePrice.newBuilder()
                .setUpfrontPrice(Price.newBuilder()
                        .setPriceAmount(CurrencyAmount.newBuilder().setAmount(UPFRONT_PRICE)))
                .setRecurringPrice(Price.newBuilder()
                        .setPriceAmount(CurrencyAmount.newBuilder().setAmount(RECURRING_PRICE)))
                .build();

        Map<Long, ReservedInstancePrice> reservedInstancePriceMap =
                ImmutableMap.<Long, ReservedInstancePrice>builder().put(PRICE_SPEC_ID2, riPrice)
                        .build();

        ReservedInstancePriceTable reservedInstancePriceTable =
                ReservedInstancePriceTable.newBuilder()
                        .putAllRiPricesBySpecId(reservedInstancePriceMap)
                        .build();

        Map<Long, ReservedInstancePriceTable> reservedInstancePriceTableMap =
                ImmutableMap.<Long, ReservedInstancePriceTable>builder().put(PT1_OID,
                        reservedInstancePriceTable).build();

        when(priceTableStore.getRiPriceTables(any())).thenReturn(reservedInstancePriceTableMap);
    }

    /**
     * Test {@link RIBuyRateProvider#lookupOnDemandRate} with no PriceAdjustments.
     */
    @Test
    public void testLookupOnDemandRateNoAdjustment() {
        ReservedInstanceSpec reservedInstanceSpec =
                ReservedInstanceSpec.newBuilder().setReservedInstanceSpecInfo(RI_SPEC_INFO).build();

        RIBuyRegionalContext regionalContext = mock(RIBuyRegionalContext.class);
        when(regionalContext.regionOid()).thenReturn(PRICE_SPEC_ID1);
        when(regionalContext.riSpecToPurchase()).thenReturn(reservedInstanceSpec);

        TopologyEntityDTO computerTier =
                TopologyEntityDTO.newBuilder().setEntityType(0).setOid(TIER1_OID).build();

        float rate = riBuyRateProvider.lookupOnDemandRate(BA1_OID, regionalContext, computerTier);

        Assert.assertEquals(ON_DEMAND_RATE, rate, COMP_DELTA);
    }

    /**
     * Test {@link RIBuyRateProvider#lookupOnDemandRate} with PriceAdjustments.
     */
    @Test
    public void testLookupOnDemandRateWithAdjustment() {
        ReservedInstanceSpec reservedInstanceSpec =
                ReservedInstanceSpec.newBuilder().setReservedInstanceSpecInfo(RI_SPEC_INFO).build();

        RIBuyRegionalContext regionalContext = mock(RIBuyRegionalContext.class);
        when(regionalContext.regionOid()).thenReturn(PRICE_SPEC_ID1);
        when(regionalContext.riSpecToPurchase()).thenReturn(reservedInstanceSpec);

        TopologyEntityDTO computerTier =
                TopologyEntityDTO.newBuilder().setEntityType(0).setOid(TIER2_OID).build();

        float rate = riBuyRateProvider.lookupOnDemandRate(BA1_OID, regionalContext, computerTier);

        Assert.assertEquals(ON_DEMAND_RATE + ON_DEMAND_RATE_ADJ, rate, COMP_DELTA);
    }

    /**
     * Test {@link RIBuyRateProvider#lookupOnDemandRate} using only regional context lookup.
     */
    @Test
    public void testLookupOnDemandRateOnlyRegionalContext() {
        ReservedInstanceSpec reservedInstanceSpec =
                ReservedInstanceSpec.newBuilder().setReservedInstanceSpecInfo(RI_SPEC_INFO).build();

        RIBuyRegionalContext regionalContext = mock(RIBuyRegionalContext.class);
        when(regionalContext.regionOid()).thenReturn(PRICE_SPEC_ID1);
        when(regionalContext.riSpecToPurchase()).thenReturn(reservedInstanceSpec);
        when(regionalContext.computeTier()).thenReturn(
                TopologyEntityDTO.newBuilder().setEntityType(0).setOid(TIER1_OID).build());

        float rate = riBuyRateProvider.lookupOnDemandRate(BA1_OID, regionalContext);

        Assert.assertEquals(ON_DEMAND_RATE, rate, COMP_DELTA);
    }

    /**
     * Test {@link RIBuyRateProvider#findRates}.
     */
    @Test
    public void testFindRates() {

        ReservedInstanceSpec reservedInstanceSpec = ReservedInstanceSpec.newBuilder()
                .setReservedInstanceSpecInfo(RI_SPEC_INFO)
                .setId(PRICE_SPEC_ID2)
                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                        .setType(ReservedInstanceType.newBuilder().setTermYears(1)))
                .build();

        RIBuyRegionalContext regionalContext = mock(RIBuyRegionalContext.class);
        when(regionalContext.regionOid()).thenReturn(PRICE_SPEC_ID1);
        when(regionalContext.riSpecToPurchase()).thenReturn(reservedInstanceSpec);
        when(regionalContext.computeTier()).thenReturn(
                TopologyEntityDTO.newBuilder().setEntityType(0).setOid(TIER1_OID).build());

        PricingProviderResult pricing = riBuyRateProvider.findRates(BA1_OID, regionalContext);

        Assert.assertEquals(ON_DEMAND_RATE, pricing.onDemandRate(), COMP_DELTA);
        final float upfrontRateExpected = UPFRONT_PRICE /
                (RIBuyRateProvider.MONTHS_IN_A_YEAR * RIBuyRateProvider.HOURS_IN_A_MONTH);
        Assert.assertEquals(upfrontRateExpected, pricing.reservedInstanceUpfrontRate(), COMP_DELTA);
        Assert.assertEquals(RECURRING_PRICE, pricing.reservedInstanceRecurringRate(), COMP_DELTA);
        Assert.assertEquals(RECURRING_PRICE + upfrontRateExpected, pricing.reservedInstanceRate(),
                COMP_DELTA);
    }
}
