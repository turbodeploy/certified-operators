package com.vmturbo.market.runner.cost;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.market.runner.cost.MarketPriceTable.ComputePriceBundle;
import com.vmturbo.market.runner.cost.MarketPriceTable.ComputePriceBundle.ComputePrice;
import com.vmturbo.market.runner.cost.MarketPriceTable.StoragePriceBundle;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageTierPriceData;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList.ComputeTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList.StorageTierPrice;

/**
 * Unit tests for {@link MarketPriceTable}.
 */
public class MarketPriceTableTest {

    private static final long COMPUTE_TIER_ID = 7;

    private static final long STORAGE_TIER_ID = 77;

    private static final long REGION_ID = 8;

    private static final double LINUX_PRICE = 10;

    private static final double SUSE_PRICE_ADJUSTMENT = 5;

    private static final PriceTable COMPUTE_PRICE_TABLE = PriceTable.newBuilder()
        .putOnDemandPriceByRegionId(REGION_ID, OnDemandPriceTable.newBuilder()
            .putComputePricesByTierId(COMPUTE_TIER_ID, ComputeTierPriceList.newBuilder()
                .setBasePrice(ComputeTierConfigPrice.newBuilder()
                    .setGuestOsType(OSType.LINUX)
                    .addPrices(Price.newBuilder()
                        .setPriceAmount(CurrencyAmount.newBuilder().setAmount(LINUX_PRICE))))
                .addPerConfigurationPriceAdjustments(ComputeTierConfigPrice.newBuilder()
                    .setGuestOsType(OSType.SUSE)
                    .addPrices(Price.newBuilder()
                        .setPriceAmount(CurrencyAmount.newBuilder().setAmount(SUSE_PRICE_ADJUSTMENT))))
                .build())
            .build())
        .build();

    private static final TopologyEntityDTO COMPUTE_TIER = TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
        .setOid(COMPUTE_TIER_ID)
        .build();

    private static final CommodityType FOO_STORAGE_ACCESS_COMM = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE)
            .setKey("foo")
            .build();

    private static final CommodityType BAR_STORAGE_ACCESS_COMM = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.STORAGE_ACCESS_VALUE)
            .setKey("bar")
            .build();

    private static final CommodityType FOO_STORAGE_AMOUNT_COMM = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
            .setKey("foo")
            .build();

    private static final CommodityType BAR_STORAGE_AMOUNT_COMM = CommodityType.newBuilder()
            .setType(CommodityDTO.CommodityType.STORAGE_AMOUNT_VALUE)
            .setKey("bar")
            .build();

    private static final TopologyEntityDTO STORAGE_TIER = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.STORAGE_TIER_VALUE)
            .setOid(STORAGE_TIER_ID)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(FOO_STORAGE_ACCESS_COMM))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(BAR_STORAGE_ACCESS_COMM))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(FOO_STORAGE_AMOUNT_COMM))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(BAR_STORAGE_AMOUNT_COMM))
            .build();

    private static final TopologyEntityDTO REGION = TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.REGION_VALUE)
        .setOid(REGION_ID)
        .build();

    private CloudCostData cloudCostData = mock(CloudCostData.class);

    private CloudTopology<TopologyEntityDTO> topology = mock(CloudTopology.class);

    private EntityInfoExtractor<TopologyEntityDTO> infoExtractor = mock(EntityInfoExtractor.class);

    private DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory =
            mock(DiscountApplicatorFactory.class);

    @Before
    public void setup() {
        when(cloudCostData.getPriceTable()).thenReturn(COMPUTE_PRICE_TABLE);
        when(topology.getEntity(REGION_ID)).thenReturn(Optional.of(REGION));
        when(topology.getEntity(COMPUTE_TIER_ID)).thenReturn(Optional.of(COMPUTE_TIER));
        when(topology.getEntity(STORAGE_TIER_ID)).thenReturn(Optional.of(STORAGE_TIER));
    }

    @Test
    public void testComputePriceBundleNoDiscount() {
        final long baId = 7L;
        doReturn(ImmutableMap.of(baId, makeBusinessAccount(baId, DiscountApplicator.noDiscount())))
            .when(topology).getEntities();
        final MarketPriceTable mktPriceTable = new MarketPriceTable(cloudCostData, topology,
                infoExtractor, discountApplicatorFactory);
        final ComputePriceBundle priceBundle = mktPriceTable.getComputePriceBundle(COMPUTE_TIER_ID, REGION_ID);
        assertThat(priceBundle.getPrices(), containsInAnyOrder(
                new ComputePrice(baId, OSType.LINUX, LINUX_PRICE),
                new ComputePrice(baId, OSType.SUSE, LINUX_PRICE + SUSE_PRICE_ADJUSTMENT)));
    }

    @Test
    public void testComputePriceBundleWithDiscount() {
        final long noDiscountBaId = 7L;
        final DiscountApplicator<TopologyEntityDTO> noDiscount = DiscountApplicator.noDiscount();

        final long discountBaId = 17L;
        final DiscountApplicator<TopologyEntityDTO> discount = mock(DiscountApplicator.class);
        when(discount.getDiscountPercentage(COMPUTE_TIER_ID)).thenReturn(0.2);

        doReturn(ImmutableMap.of(
                noDiscountBaId, makeBusinessAccount(noDiscountBaId, noDiscount),
                discountBaId, makeBusinessAccount(discountBaId, discount)))
            .when(topology).getEntities();

        final MarketPriceTable mktPriceTable = new MarketPriceTable(cloudCostData, topology,
                infoExtractor, discountApplicatorFactory);
        final ComputePriceBundle priceBundle = mktPriceTable.getComputePriceBundle(COMPUTE_TIER_ID, REGION_ID);
        assertThat(priceBundle.getPrices(), containsInAnyOrder(
                new ComputePrice(noDiscountBaId, OSType.LINUX, LINUX_PRICE),
                new ComputePrice(noDiscountBaId, OSType.SUSE, LINUX_PRICE + SUSE_PRICE_ADJUSTMENT),
                new ComputePrice(discountBaId, OSType.LINUX, LINUX_PRICE * 0.8),
                new ComputePrice(discountBaId, OSType.SUSE, (LINUX_PRICE + SUSE_PRICE_ADJUSTMENT) * 0.8)
        ));
    }

    @Test
    public void testStoragePriceBundleGBMonth() {
        // $10 for the first 7 GB-month, $5 for the next 3, $4 afterwards.
        final PriceTable priceTable = PriceTable.newBuilder()
            .putOnDemandPriceByRegionId(REGION_ID, OnDemandPriceTable.newBuilder()
                .putCloudStoragePricesByTierId(STORAGE_TIER_ID, StorageTierPriceList.newBuilder()
                    .addCloudStoragePrice(StorageTierPrice.newBuilder()
                        .addPrices(Price.newBuilder()
                            .setPriceAmount(CurrencyAmount.newBuilder()
                                .setAmount(CostProtoUtil.getUnitPriceAmount(Unit.GB_MONTH, 10)))
                            .setUnit(Unit.GB_MONTH)
                            .setEndRangeInUnits(7))
                        .addPrices(Price.newBuilder()
                            .setPriceAmount(CurrencyAmount.newBuilder()
                                .setAmount(CostProtoUtil.getUnitPriceAmount(Unit.GB_MONTH, 5)))
                            .setUnit(Unit.GB_MONTH)
                            .setEndRangeInUnits(10))
                        .addPrices(Price.newBuilder()
                            .setPriceAmount(CurrencyAmount.newBuilder()
                                .setAmount(CostProtoUtil.getUnitPriceAmount(Unit.GB_MONTH, 4)))
                            .setUnit(Unit.GB_MONTH)))
                    .build())
                .build())
            .build();
        when(cloudCostData.getPriceTable()).thenReturn(priceTable);

        final long baId = 7L;
        doReturn(ImmutableMap.of(baId, makeBusinessAccount(baId, DiscountApplicator.noDiscount())))
                .when(topology).getEntities();
        final MarketPriceTable mktPriceTable = new MarketPriceTable(cloudCostData, topology,
                infoExtractor, discountApplicatorFactory);
        final StoragePriceBundle storagePriceBundle =
                mktPriceTable.getStoragePriceBundle(STORAGE_TIER_ID, REGION_ID);
        final StorageTierPriceData[] expectedData = new StorageTierPriceData[]{
                StorageTierPriceData.newBuilder()
                    .setBusinessAccountId(baId)
                    // Unit price true because it's priced per GB-month
                    .setIsUnitPrice(true)
                    // Accumulative price true because we have ranges
                    .setIsAccumulativeCost(true)
                    .setUpperBound(7)
                    .setPrice(10)
                    .build(),
                StorageTierPriceData.newBuilder()
                    .setBusinessAccountId(baId)
                    // Unit price true because it's priced per GB-month
                    .setIsUnitPrice(true)
                    // Accumulative price true because we have ranges
                    .setIsAccumulativeCost(true)
                    .setUpperBound(10)
                    .setPrice(5)
                    .build(),
                StorageTierPriceData.newBuilder()
                    .setBusinessAccountId(baId)
                    // Unit price true because it's priced per GB-month
                    .setIsUnitPrice(true)
                    // Accumulative price true because we have ranges
                    .setIsAccumulativeCost(true)
                    .setUpperBound(Double.POSITIVE_INFINITY)
                    .setPrice(4)
                    .build()};

        // The prices for all storage amount commodities should be the same, because the price
        // tables don't distinguish by commodity key.
        assertThat(storagePriceBundle.getPrices(FOO_STORAGE_AMOUNT_COMM), contains(expectedData));
        assertThat(storagePriceBundle.getPrices(BAR_STORAGE_AMOUNT_COMM), contains(expectedData));
        assertThat(storagePriceBundle.getPrices(FOO_STORAGE_ACCESS_COMM), is(Collections.emptyList()));
        assertThat(storagePriceBundle.getPrices(BAR_STORAGE_ACCESS_COMM), is(Collections.emptyList()));
    }

    @Test
    public void testStoragePriceBundleIOPSMonth() {
        // $10 for the first 7 million-iops, $5 afterwards.
        final PriceTable priceTable = PriceTable.newBuilder()
            .putOnDemandPriceByRegionId(REGION_ID, OnDemandPriceTable.newBuilder()
                .putCloudStoragePricesByTierId(STORAGE_TIER_ID, StorageTierPriceList.newBuilder()
                    .addCloudStoragePrice(StorageTierPrice.newBuilder()
                        .addPrices(Price.newBuilder()
                            .setPriceAmount(CurrencyAmount.newBuilder()
                                .setAmount(CostProtoUtil.getUnitPriceAmount(Unit.MILLION_IOPS, 10)))
                            .setUnit(Unit.MILLION_IOPS)
                            .setEndRangeInUnits(7))
                        .addPrices(Price.newBuilder()
                            .setPriceAmount(CurrencyAmount.newBuilder()
                                .setAmount(CostProtoUtil.getUnitPriceAmount(Unit.MILLION_IOPS, 5)))
                            .setUnit(Unit.MILLION_IOPS)))
                    .build())
                .build())
            .build();
        when(cloudCostData.getPriceTable()).thenReturn(priceTable);

        final long baId = 7L;
        doReturn(ImmutableMap.of(baId, makeBusinessAccount(baId, DiscountApplicator.noDiscount())))
                .when(topology).getEntities();
        final MarketPriceTable mktPriceTable = new MarketPriceTable(cloudCostData, topology,
                infoExtractor, discountApplicatorFactory);
        final StoragePriceBundle storagePriceBundle =
                mktPriceTable.getStoragePriceBundle(STORAGE_TIER_ID, REGION_ID);

        final List<StorageTierPriceData> expectedData = Lists.newArrayList(StorageTierPriceData.newBuilder()
                .setBusinessAccountId(baId)
                // Unit price true because it's priced per million-iops.
                .setIsUnitPrice(true)
                // Accumulative price true because we have ranges
                .setIsAccumulativeCost(true)
                .setUpperBound(7)
                .setPrice(10)
                .build(),
                StorageTierPriceData.newBuilder()
                        .setBusinessAccountId(baId)
                        // Unit price true because it's priced per million-iops.
                        .setIsUnitPrice(true)
                        // Accumulative price true because we have ranges
                        .setIsAccumulativeCost(true)
                        .setUpperBound(Double.POSITIVE_INFINITY)
                        .setPrice(5)
                        .build());

        // The prices for all storage amount commodities should be the same, because the price
        // tables don't distinguish by commodity key.
        assertThat(storagePriceBundle.getPrices(FOO_STORAGE_ACCESS_COMM), contains(expectedData.toArray()));
        assertThat(storagePriceBundle.getPrices(BAR_STORAGE_ACCESS_COMM), contains(expectedData.toArray()));
        assertThat(storagePriceBundle.getPrices(FOO_STORAGE_AMOUNT_COMM), is(Collections.emptyList()));
        assertThat(storagePriceBundle.getPrices(BAR_STORAGE_AMOUNT_COMM), is(Collections.emptyList()));
    }

    @Test
    public void testStoragePriceBundleFlatCost() {
        // $10/month straight-up.
        final PriceTable priceTable = PriceTable.newBuilder()
            .putOnDemandPriceByRegionId(REGION_ID, OnDemandPriceTable.newBuilder()
                .putCloudStoragePricesByTierId(STORAGE_TIER_ID, StorageTierPriceList.newBuilder()
                    .addCloudStoragePrice(StorageTierPrice.newBuilder()
                        .addPrices(Price.newBuilder()
                            .setPriceAmount(CurrencyAmount.newBuilder()
                                .setAmount(CostProtoUtil.getUnitPriceAmount(Unit.MONTH, 10)))
                            .setUnit(Unit.MONTH)))
                    .build())
                .build())
            .build();
        when(cloudCostData.getPriceTable()).thenReturn(priceTable);

        final long baId = 7L;
        doReturn(ImmutableMap.of(baId, makeBusinessAccount(baId, DiscountApplicator.noDiscount())))
                .when(topology).getEntities();
        final MarketPriceTable mktPriceTable = new MarketPriceTable(cloudCostData, topology,
                infoExtractor, discountApplicatorFactory);
        final StoragePriceBundle storagePriceBundle =
                mktPriceTable.getStoragePriceBundle(STORAGE_TIER_ID, REGION_ID);

        final StorageTierPriceData expectedData = StorageTierPriceData.newBuilder()
                .setBusinessAccountId(baId)
                // Not unit price, because it's a flat cost.
                .setIsUnitPrice(false)
                // Not accumulative because we don't have ranges.
                .setIsAccumulativeCost(false)
                .setUpperBound(Double.POSITIVE_INFINITY)
                .setPrice(10)
                .build();

        assertThat(storagePriceBundle.getPrices(FOO_STORAGE_AMOUNT_COMM), contains(expectedData));
        assertThat(storagePriceBundle.getPrices(BAR_STORAGE_AMOUNT_COMM), contains(expectedData));
        assertThat(storagePriceBundle.getPrices(FOO_STORAGE_ACCESS_COMM), is(Collections.emptyList()));
        assertThat(storagePriceBundle.getPrices(BAR_STORAGE_ACCESS_COMM), is(Collections.emptyList()));
    }

    @Test
    public void testStoragePriceBundleFlatCostRanges() {
        // $10/month for the first 7 GB, $5/month afterwards.
        final PriceTable priceTable = PriceTable.newBuilder()
            .putOnDemandPriceByRegionId(REGION_ID, OnDemandPriceTable.newBuilder()
                .putCloudStoragePricesByTierId(STORAGE_TIER_ID, StorageTierPriceList.newBuilder()
                    .addCloudStoragePrice(StorageTierPrice.newBuilder()
                        .addPrices(Price.newBuilder()
                            .setPriceAmount(CurrencyAmount.newBuilder()
                                .setAmount(CostProtoUtil.getUnitPriceAmount(Unit.MONTH, 10)))
                            .setUnit(Unit.MONTH)
                            .setEndRangeInUnits(7))
                        .addPrices(Price.newBuilder()
                            .setPriceAmount(CurrencyAmount.newBuilder()
                                .setAmount(CostProtoUtil.getUnitPriceAmount(Unit.MONTH, 5)))
                            .setUnit(Unit.MONTH)))
                    .build())
                .build())
            .build();
        when(cloudCostData.getPriceTable()).thenReturn(priceTable);

        final long baId = 7L;
        doReturn(ImmutableMap.of(baId, makeBusinessAccount(baId, DiscountApplicator.noDiscount())))
                .when(topology).getEntities();
        final MarketPriceTable mktPriceTable = new MarketPriceTable(cloudCostData, topology,
                infoExtractor, discountApplicatorFactory);
        final StoragePriceBundle storagePriceBundle =
                mktPriceTable.getStoragePriceBundle(STORAGE_TIER_ID, REGION_ID);

        final List<StorageTierPriceData> expectedData = Lists.newArrayList(
            StorageTierPriceData.newBuilder()
                .setBusinessAccountId(baId)
                // Not unit price, because it's a flat cost for each range.
                .setIsUnitPrice(false)
                // Accumulative price true because we have ranges
                .setIsAccumulativeCost(true)
                .setUpperBound(7)
                .setPrice(10)
                .build(),
            StorageTierPriceData.newBuilder()
                .setBusinessAccountId(baId)
                // Not unit price, because it's a flat cost for each range.
                .setIsUnitPrice(false)
                // Accumulative price true because we have ranges
                .setIsAccumulativeCost(true)
                .setUpperBound(Double.POSITIVE_INFINITY)
                .setPrice(5)
                .build());

        // The prices for all storage amount commodities should be the same, because the price
        // tables don't distinguish by commodity key.
        assertThat(storagePriceBundle.getPrices(FOO_STORAGE_AMOUNT_COMM), contains(expectedData.toArray()));
        assertThat(storagePriceBundle.getPrices(BAR_STORAGE_AMOUNT_COMM), contains(expectedData.toArray()));
        assertThat(storagePriceBundle.getPrices(FOO_STORAGE_ACCESS_COMM), is(Collections.emptyList()));
        assertThat(storagePriceBundle.getPrices(BAR_STORAGE_ACCESS_COMM), is(Collections.emptyList()));
    }

    @Test
    public void testStoragePriceBundleGBMonthWithDiscount() {
        // $10 for the first 7 GB-month, $5 afterwards.
        final PriceTable priceTable = PriceTable.newBuilder()
            .putOnDemandPriceByRegionId(REGION_ID, OnDemandPriceTable.newBuilder()
                .putCloudStoragePricesByTierId(STORAGE_TIER_ID, StorageTierPriceList.newBuilder()
                    .addCloudStoragePrice(StorageTierPrice.newBuilder()
                        .addPrices(Price.newBuilder()
                            .setPriceAmount(CurrencyAmount.newBuilder()
                                .setAmount(CostProtoUtil.getUnitPriceAmount(Unit.GB_MONTH, 10)))
                            .setUnit(Unit.GB_MONTH)
                            .setEndRangeInUnits(7))
                        .addPrices(Price.newBuilder()
                            .setPriceAmount(CurrencyAmount.newBuilder()
                                .setAmount(CostProtoUtil.getUnitPriceAmount(Unit.GB_MONTH, 5)))
                            .setUnit(Unit.GB_MONTH)))
                    .build())
                .build())
            .build();
        when(cloudCostData.getPriceTable()).thenReturn(priceTable);

        final long baId = 7L;
        // Add a 20% discount for the storage tier.
        final DiscountApplicator<TopologyEntityDTO> discount = mock(DiscountApplicator.class);
        when(discount.getDiscountPercentage(STORAGE_TIER_ID)).thenReturn(0.2);
        doReturn(ImmutableMap.of(baId, makeBusinessAccount(baId, discount)))
                .when(topology).getEntities();

        final MarketPriceTable mktPriceTable = new MarketPriceTable(cloudCostData, topology,
                infoExtractor, discountApplicatorFactory);
        final StoragePriceBundle storagePriceBundle =
                mktPriceTable.getStoragePriceBundle(STORAGE_TIER_ID, REGION_ID);
        final List<StorageTierPriceData> expectedData = Lists.newArrayList(
                StorageTierPriceData.newBuilder()
                    .setBusinessAccountId(baId)
                    // Unit price true because it's priced per GB-month
                    .setIsUnitPrice(true)
                    // Accumulative price true because we have ranges
                    .setIsAccumulativeCost(true)
                    .setUpperBound(7)
                    // 20% off $10
                    .setPrice(8)
                    .build(),
                StorageTierPriceData.newBuilder()
                    .setBusinessAccountId(baId)
                    // Unit price true because it's priced per GB-month
                    .setIsUnitPrice(true)
                    // Accumulative price true because we have ranges
                    .setIsAccumulativeCost(true)
                    .setUpperBound(Double.POSITIVE_INFINITY)
                    // 20% off $5
                    .setPrice(4)
                    .build());

        // The prices for all storage amount commodities should be the same, because the price
        // tables don't distinguish by commodity key.
        assertThat(storagePriceBundle.getPrices(FOO_STORAGE_AMOUNT_COMM), contains(expectedData.toArray()));
        assertThat(storagePriceBundle.getPrices(BAR_STORAGE_AMOUNT_COMM), contains(expectedData.toArray()));
        assertThat(storagePriceBundle.getPrices(FOO_STORAGE_ACCESS_COMM), is(Collections.emptyList()));
        assertThat(storagePriceBundle.getPrices(BAR_STORAGE_ACCESS_COMM), is(Collections.emptyList()));
    }

    private TopologyEntityDTO makeBusinessAccount(final long id,
              final DiscountApplicator<TopologyEntityDTO> discountApplicator) {
        final TopologyEntityDTO businessAccount = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setOid(id)
                .build();
        doReturn(discountApplicator).when(discountApplicatorFactory).accountDiscountApplicator(id, topology, infoExtractor, cloudCostData);
        return businessAccount;
    }
}
