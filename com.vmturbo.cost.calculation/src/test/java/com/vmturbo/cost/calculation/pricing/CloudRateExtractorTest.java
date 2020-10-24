package com.vmturbo.cost.calculation.pricing;

import static com.vmturbo.trax.Trax.trax;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.LicensePriceTuple;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.ComputePriceBundle;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.ComputePriceBundle.ComputePrice;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.StoragePriceBundle;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple;
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
 * Unit tests for {@link PriceTable}.
 */
public class CloudRateExtractorTest {

    private static final long AWS_COMPUTE_TIER_ID = 95348;
    private static final long AZURE_COMPUTE_TIER_ID = 25387;
    private static final int NUM_OF_CORES = 4;

    private static final long STORAGE_TIER_ID = 77;
    private static final long IO2_TIER_ID = 78;

    private static final long REGION_ID = 8;

    // Base Price
    private static final double LINUX_PRICE = 0.096;

    // Price Adjustments
    private static final double WINDOWS_PRICE_ADJUSTMENT = 0.092;
    private static final double WINDOWS_SQL_WEB_PRICE_ADJUSTMENT = 0.02;
    private static final double RHEL_PRICE_ADJUSTMENT = 0.06;

    private static final long BUSINESS_ACCOUNT_ID = 5;

    // License Prices
    private static final double WINDOWS_SQL_WEB_LICENSE_PRICE = 0.005;
    private static final double RHEL_LICENSE_PRICE = 0.006;

    private static final String LINUX = "Linux";
    private static final String RHEL = "RHEL";
    private static final String WINDOWS = "Windows";
    private static final String WINDOWS_WITH_SQL_WEB = "Windows_SQL_Web";
    private static final boolean BURSTABLE_CPUS = true;
    private static final boolean NOT_BURSTABLE_CPUS = !BURSTABLE_CPUS;

    private static final PriceTable COMPUTE_PRICE_TABLE = PriceTable.newBuilder()
        .putOnDemandPriceByRegionId(REGION_ID, OnDemandPriceTable.newBuilder()
            .putComputePricesByTierId(AWS_COMPUTE_TIER_ID,
                createComputeTierPriceList(OSType.LINUX,
                    Arrays.asList(createComputeTierConfigPrice(OSType.LINUX, LINUX_PRICE),
                        createComputeTierConfigPrice(OSType.WINDOWS, WINDOWS_PRICE_ADJUSTMENT),
                        createComputeTierConfigPrice(OSType.RHEL, RHEL_PRICE_ADJUSTMENT))))
            .putComputePricesByTierId(AZURE_COMPUTE_TIER_ID,
                createComputeTierPriceList(OSType.LINUX,
                    Arrays.asList(createComputeTierConfigPrice(OSType.LINUX, LINUX_PRICE),
                        createComputeTierConfigPrice(OSType.WINDOWS, WINDOWS_PRICE_ADJUSTMENT))))
            .build())
        .build();

    /**
     * Create a ComputeTierConfigPrice instance.
     *
     * @param os the OS for which we want to create a ComputeTierConfigPrice
     * @param price the dollar/hour price
     * @return A ComputeTierConfigPrice object with the given fields
     */
    private static ComputeTierConfigPrice createComputeTierConfigPrice(OSType os, double price) {
        return ComputeTierConfigPrice.newBuilder()
                .setGuestOsType(os)
                .addPrices(createPrice(price))
                .build();
    }

    /**
     * Create a ComputeTierPriceList instance.
     *
     * @param baseOS the base OS for the price list.
     * @param prices the prices for the price list.
     * @return A ComputeTierPriceList object with the given prices.
     */
    private static ComputeTierPriceList createComputeTierPriceList(OSType baseOS, List<ComputeTierConfigPrice> prices) {
        ComputeTierPriceList.Builder computePriceList =  ComputeTierPriceList.newBuilder();

        for (ComputeTierConfigPrice computePrice : prices) {
            if (computePrice.getGuestOsType() == baseOS) {
                computePriceList.setBasePrice(computePrice);
            } else {
                computePriceList.addPerConfigurationPriceAdjustments(computePrice);
            }
        }

        return computePriceList.build();
    }

    /**
     * Create a Price instance.
     *
     * @param amount number of dollars.
     * @return A price object with the given fields.
     */
    private static Price createPrice(double amount) {
        return Price.newBuilder()
            .setUnit(Unit.HOURS)
            .setPriceAmount(CurrencyAmount.newBuilder()
                .setAmount(amount)
                .build())
            .build();
    }

    /**
     * Create a CommoditySoldDTO instance.
     *
     * @param os the OS for which we want to create a license commodity
     * @return A CommoditySoldDTO object for the given OS
     */
    private static CommoditySoldDTO createCommoditySoldDTO(String os) {
        return CommoditySoldDTO.newBuilder()
            .setCommodityType(CommodityType.newBuilder()
                .setType(CommodityDTO.CommodityType.LICENSE_ACCESS_VALUE)
                .setKey(os).build()).build();
    }

    private static final TopologyEntityDTO AWS_COMPUTE_TIER = TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
        .setOid(AWS_COMPUTE_TIER_ID)
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setComputeTier(ComputeTierInfo.newBuilder()
                .setNumCoupons(1)
                .setNumCores(NUM_OF_CORES)
                .build())
            .build())
        .addCommoditySoldList(createCommoditySoldDTO(LINUX))
        .addCommoditySoldList(createCommoditySoldDTO(WINDOWS))
        .addCommoditySoldList(createCommoditySoldDTO(WINDOWS_WITH_SQL_WEB))
        .addCommoditySoldList(createCommoditySoldDTO(RHEL))
        .build();

    private static final TopologyEntityDTO AZURE_COMPUTE_TIER = TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.COMPUTE_TIER_VALUE)
        .setOid(AZURE_COMPUTE_TIER_ID)
        .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setComputeTier(ComputeTierInfo.newBuilder()
                .setNumCoupons(1)
                .setNumCores(NUM_OF_CORES)
                .build())
            .build())
        .addCommoditySoldList(createCommoditySoldDTO(LINUX))
        .addCommoditySoldList(createCommoditySoldDTO(WINDOWS))
        .addCommoditySoldList(createCommoditySoldDTO(WINDOWS_WITH_SQL_WEB))
        .addCommoditySoldList(createCommoditySoldDTO(RHEL))
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

    private static final TopologyEntityDTO IO2_STORAGE_TIER = TopologyEntityDTO.newBuilder()
            .setEntityType(EntityType.STORAGE_TIER_VALUE)
            .setDisplayName("IO2")
            .setOid(IO2_TIER_ID)
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(FOO_STORAGE_ACCESS_COMM))
            .addCommoditySoldList(CommoditySoldDTO.newBuilder()
                    .setCommodityType(FOO_STORAGE_AMOUNT_COMM))
            .build();

    private static final TopologyEntityDTO REGION = TopologyEntityDTO.newBuilder()
        .setEntityType(EntityType.REGION_VALUE)
        .setOid(REGION_ID)
        .build();

    private CloudCostData<TopologyEntityDTO> cloudCostData = mock(CloudCostData.class);

    private CloudTopology<TopologyEntityDTO> topology = mock(CloudTopology.class);

    private EntityInfoExtractor<TopologyEntityDTO> infoExtractor = new TopologyEntityInfoExtractor();

    private AccountPricingData<TopologyEntityDTO> accountPricingData =
            mock(AccountPricingData.class);

    private DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory =
            mock(DiscountApplicatorFactory.class);

    /**
     * Setup the test.
     */
    @Before
    public void setup() {
        when(cloudCostData.getAccountPricingData(BUSINESS_ACCOUNT_ID)).thenReturn(Optional.ofNullable(accountPricingData));
        when(cloudCostData.getAccountPricingData(BUSINESS_ACCOUNT_ID).get().getPriceTable()).thenReturn(COMPUTE_PRICE_TABLE);
        when(topology.getEntity(REGION_ID)).thenReturn(Optional.of(REGION));
        when(topology.getEntity(AWS_COMPUTE_TIER_ID)).thenReturn(Optional.of(AWS_COMPUTE_TIER));
        when(topology.getEntity(AZURE_COMPUTE_TIER_ID)).thenReturn(Optional.of(AZURE_COMPUTE_TIER));
        when(topology.getEntity(STORAGE_TIER_ID)).thenReturn(Optional.of(STORAGE_TIER));
        when(topology.getEntity(IO2_TIER_ID)).thenReturn(Optional.of(IO2_STORAGE_TIER));
        /*initializeAWSLicensePriceTuples();
        initializeAzureLicensePriceTuples();*/
    }

    private ComputeTierPriceList getComputePriceList(long tierID) {
        return cloudCostData.getAccountPricingData(BUSINESS_ACCOUNT_ID).get().getPriceTable().getOnDemandPriceByRegionIdMap().get(REGION_ID)
            .getComputePricesByTierIdMap().get(tierID);
    }

    private LicensePriceTuple createLicensePriceTuple(
        double implicitPrice, double explicitPrice) {
        LicensePriceTuple licensePriceTuple = Mockito.mock(LicensePriceTuple.class);
        Mockito.when(licensePriceTuple.getImplicitOnDemandLicensePrice()).thenReturn(implicitPrice);
        Mockito.when(licensePriceTuple.getExplicitOnDemandLicensePrice()).thenReturn(explicitPrice);
        return licensePriceTuple;
    }

    private void initializeAWSLicensePriceTuples(Long businessAccountId) {
        ComputeTierPriceList priceList = getComputePriceList(AWS_COMPUTE_TIER_ID);
        LicensePriceTuple emptyLicenseTuple = createLicensePriceTuple(0.0, 0.0);
        when(cloudCostData.getAccountPricingData(businessAccountId).get().getLicensePrice(OSType.LINUX, NUM_OF_CORES, priceList, NOT_BURSTABLE_CPUS ))
            .thenReturn(emptyLicenseTuple);
        LicensePriceTuple windowsLicenseTuple = createLicensePriceTuple(WINDOWS_PRICE_ADJUSTMENT,
            0.0);
        when(cloudCostData.getAccountPricingData(businessAccountId).get().getLicensePrice(OSType.WINDOWS, NUM_OF_CORES, priceList, NOT_BURSTABLE_CPUS ))
            .thenReturn(windowsLicenseTuple);
        LicensePriceTuple windowsPALicenseTuple = createLicensePriceTuple(
            WINDOWS_SQL_WEB_PRICE_ADJUSTMENT, 0.0);
        when(cloudCostData.getAccountPricingData(businessAccountId).get().getLicensePrice(OSType.WINDOWS_WITH_SQL_WEB, NUM_OF_CORES, priceList, NOT_BURSTABLE_CPUS ))
            .thenReturn(windowsPALicenseTuple);
        LicensePriceTuple redHatPALicenseTuple = createLicensePriceTuple(RHEL_PRICE_ADJUSTMENT,
            0.0);
        when(cloudCostData.getAccountPricingData(businessAccountId).get().getLicensePrice(OSType.RHEL, NUM_OF_CORES, priceList, NOT_BURSTABLE_CPUS ))
            .thenReturn(redHatPALicenseTuple);
    }

    private void initializeAzureLicensePriceTuples(Long businessAccountId) {
        ComputeTierPriceList priceList = getComputePriceList(AZURE_COMPUTE_TIER_ID);
        LicensePriceTuple emptyLicenseTuple = createLicensePriceTuple(0.0, 0.0);
        when(cloudCostData.getAccountPricingData(businessAccountId).get().getLicensePrice(OSType.LINUX, NUM_OF_CORES, priceList, NOT_BURSTABLE_CPUS ))
            .thenReturn(emptyLicenseTuple);
        LicensePriceTuple windowsLicenseTuple = createLicensePriceTuple(WINDOWS_PRICE_ADJUSTMENT,
            0.0);
        when(cloudCostData.getAccountPricingData(businessAccountId).get().getLicensePrice(OSType.WINDOWS, NUM_OF_CORES, priceList, NOT_BURSTABLE_CPUS ))
            .thenReturn(windowsLicenseTuple);
        LicensePriceTuple windowsSqlWebPriceTuple = createLicensePriceTuple(WINDOWS_SQL_WEB_PRICE_ADJUSTMENT,
            0.0);
        when(cloudCostData.getAccountPricingData(businessAccountId).get().getLicensePrice(OSType.WINDOWS_WITH_SQL_WEB, NUM_OF_CORES, priceList, NOT_BURSTABLE_CPUS ))
            .thenReturn(windowsSqlWebPriceTuple);
        LicensePriceTuple redHatLpPriceTuple = createLicensePriceTuple(RHEL_PRICE_ADJUSTMENT, 0.0);
        when(cloudCostData.getAccountPricingData(businessAccountId).get().getLicensePrice(OSType.RHEL, NUM_OF_CORES, priceList, NOT_BURSTABLE_CPUS ))
            .thenReturn(redHatLpPriceTuple);
    }

    /**
     * Test Aws compute prices with no discount.
     */
    @Test
    public void testAWSComputePriceBundleNoDiscount() {
        final long baId = 7L;
        doReturn(ImmutableMap.of(baId, makeBusinessAccount(baId, DiscountApplicator.noDiscount())))
            .when(topology).getEntities();
        final CloudRateExtractor mktPriceTable = new CloudRateExtractor(topology, infoExtractor);

        AccountPricingData<TopologyEntityDTO> accountPricingData1 =
                Mockito.mock(AccountPricingData.class);
        when(accountPricingData1.getDiscountApplicator()).thenReturn(DiscountApplicator.noDiscount());
        when(accountPricingData1.getPriceTable()).thenReturn(COMPUTE_PRICE_TABLE);
        when(accountPricingData1.getAccountPricingDataOid()).thenReturn(baId);
        when(cloudCostData.getAccountPricingData(baId))
                .thenReturn(Optional.of(accountPricingData1));
        initializeAWSLicensePriceTuples(baId);

        ComputePriceBundle priceBundle = mktPriceTable.getComputePriceBundle(AWS_COMPUTE_TIER, REGION_ID, accountPricingData1);

        assertThat(priceBundle.getPrices(), containsInAnyOrder(
                ComputePrice.builder()
                        .accountId(baId)
                        .osType(OSType.LINUX)
                        .hourlyComputeRate(LINUX_PRICE)
                        .hourlyLicenseRate(0.0)
                        .isBasePrice(true)
                        .build(),
                ComputePrice.builder()
                        .accountId(baId)
                        .osType(OSType.WINDOWS)
                        .hourlyComputeRate(LINUX_PRICE)
                        .hourlyLicenseRate(WINDOWS_PRICE_ADJUSTMENT)
                        .isBasePrice(false)
                        .build(),
                ComputePrice.builder()
                        .accountId(baId)
                        .osType(OSType.WINDOWS_WITH_SQL_WEB)
                        .hourlyComputeRate(LINUX_PRICE)
                        .hourlyLicenseRate(WINDOWS_SQL_WEB_PRICE_ADJUSTMENT)
                        .isBasePrice(false)
                        .build(),
                ComputePrice.builder()
                        .accountId(baId)
                        .osType(OSType.RHEL)
                        .hourlyComputeRate(LINUX_PRICE)
                        .hourlyLicenseRate(RHEL_PRICE_ADJUSTMENT)
                        .isBasePrice(false)
                        .build()));
    }

    /**
     * Test Azure compute with no discount.
     */
    @Test
    public void testAzureComputePriceBundleNoDiscount() {
        final long baId = 7L;
        final CloudRateExtractor mktPriceTable = new CloudRateExtractor(topology, infoExtractor);

        AccountPricingData<TopologyEntityDTO> accountPricingData1 =
                Mockito.mock(AccountPricingData.class);
        when(accountPricingData1.getDiscountApplicator()).thenReturn(DiscountApplicator.noDiscount());
        when(accountPricingData1.getPriceTable()).thenReturn(COMPUTE_PRICE_TABLE);
        when(accountPricingData1.getAccountPricingDataOid()).thenReturn(baId);
        when(cloudCostData.getAccountPricingData(baId))
                .thenReturn(Optional.of(accountPricingData1));
        initializeAzureLicensePriceTuples(baId);

        ComputePriceBundle priceBundle = mktPriceTable.getComputePriceBundle(AZURE_COMPUTE_TIER, REGION_ID, accountPricingData1);

        assertThat(priceBundle.getPrices(), containsInAnyOrder(
                ComputePrice.builder()
                        .accountId(baId)
                        .osType(OSType.LINUX)
                        .hourlyComputeRate(LINUX_PRICE)
                        .hourlyLicenseRate(0.0)
                        .isBasePrice(true)
                        .build(),
                ComputePrice.builder()
                        .accountId(baId)
                        .osType(OSType.WINDOWS)
                        .hourlyComputeRate(LINUX_PRICE)
                        .hourlyLicenseRate(WINDOWS_PRICE_ADJUSTMENT)
                        .isBasePrice(false)
                        .build(),
                ComputePrice.builder()
                        .accountId(baId)
                        .osType(OSType.WINDOWS_WITH_SQL_WEB)
                        .hourlyComputeRate(LINUX_PRICE)
                        .hourlyLicenseRate(WINDOWS_SQL_WEB_PRICE_ADJUSTMENT)
                        .isBasePrice(false)
                        .build(),
                ComputePrice.builder()
                        .accountId(baId)
                        .osType(OSType.RHEL)
                        .hourlyComputeRate(LINUX_PRICE)
                        .hourlyLicenseRate(RHEL_PRICE_ADJUSTMENT)
                        .isBasePrice(false)
                        .build()));
    }

    /**
     * Test Azure compute with discount.
     */
    @Test
    public void testAWSComputePriceBundleWithDiscount() {
        final long noDiscountBaId = 7L;
        final DiscountApplicator<TopologyEntityDTO> noDiscount = DiscountApplicator.noDiscount();

        final long discountBaId = 17L;
        final DiscountApplicator<TopologyEntityDTO> discount = mock(DiscountApplicator.class);
        when(discount.getDiscountPercentage(AWS_COMPUTE_TIER_ID)).thenReturn(trax(0.2));

        final CloudRateExtractor mktPriceTable = new CloudRateExtractor(topology, infoExtractor);

        AccountPricingData<TopologyEntityDTO> accountPricingData1 =
                Mockito.mock(AccountPricingData.class);
        when(accountPricingData1.getDiscountApplicator()).thenReturn(noDiscount);
        when(accountPricingData1.getPriceTable()).thenReturn(COMPUTE_PRICE_TABLE);
        when(accountPricingData1.getAccountPricingDataOid()).thenReturn(noDiscountBaId);
        when(cloudCostData.getAccountPricingData(noDiscountBaId))
                .thenReturn(Optional.of(accountPricingData1));
        initializeAWSLicensePriceTuples(noDiscountBaId);

        AccountPricingData<TopologyEntityDTO> accountPricingData2 =
                Mockito.mock(AccountPricingData.class);
        when(accountPricingData2.getDiscountApplicator()).thenReturn(discount);
        when(accountPricingData2.getPriceTable()).thenReturn(COMPUTE_PRICE_TABLE);
        when(accountPricingData2.getAccountPricingDataOid()).thenReturn(discountBaId);
        when(cloudCostData.getAccountPricingData(discountBaId))
                .thenReturn(Optional.of(accountPricingData2));
        initializeAWSLicensePriceTuples(discountBaId);

        final ComputePriceBundle priceBundle1 = mktPriceTable.getComputePriceBundle(AWS_COMPUTE_TIER, REGION_ID, accountPricingData1);

        final ComputePriceBundle priceBundle2 = mktPriceTable.getComputePriceBundle(AWS_COMPUTE_TIER, REGION_ID, accountPricingData2);

        assertThat(priceBundle1.getPrices(), containsInAnyOrder(
                ComputePrice.builder()
                        .accountId(noDiscountBaId)
                        .osType(OSType.LINUX)
                        .hourlyComputeRate(LINUX_PRICE)
                        .hourlyLicenseRate(0.0)
                        .isBasePrice(true)
                        .build(),
                ComputePrice.builder()
                        .accountId(noDiscountBaId)
                        .osType(OSType.WINDOWS)
                        .hourlyComputeRate(LINUX_PRICE)
                        .hourlyLicenseRate(WINDOWS_PRICE_ADJUSTMENT)
                        .isBasePrice(false)
                        .build(),
                ComputePrice.builder()
                        .accountId(noDiscountBaId)
                        .osType(OSType.WINDOWS_WITH_SQL_WEB)
                        .hourlyComputeRate(LINUX_PRICE)
                        .hourlyLicenseRate(WINDOWS_SQL_WEB_PRICE_ADJUSTMENT)
                        .isBasePrice(false)
                        .build(),
                ComputePrice.builder()
                        .accountId(noDiscountBaId)
                        .osType(OSType.RHEL)
                        .hourlyComputeRate(LINUX_PRICE)
                        .hourlyLicenseRate(RHEL_PRICE_ADJUSTMENT)
                        .isBasePrice(false)
                        .build()));

        assertThat(priceBundle2.getPrices(), containsInAnyOrder(
                ComputePrice.builder()
                        .accountId(discountBaId)
                        .osType(OSType.LINUX)
                        .hourlyComputeRate(LINUX_PRICE * 0.8)
                        .hourlyLicenseRate(0.0)
                        .isBasePrice(true)
                        .build(),
                ComputePrice.builder()
                        .accountId(discountBaId)
                        .osType(OSType.WINDOWS)
                        .hourlyComputeRate(LINUX_PRICE * 0.8)
                        .hourlyLicenseRate(WINDOWS_PRICE_ADJUSTMENT * 0.8)
                        .isBasePrice(false)
                        .build(),
                ComputePrice.builder()
                        .accountId(discountBaId)
                        .osType(OSType.WINDOWS_WITH_SQL_WEB)
                        .hourlyComputeRate(LINUX_PRICE * 0.8)
                        .hourlyLicenseRate(WINDOWS_SQL_WEB_PRICE_ADJUSTMENT * 0.8)
                        .isBasePrice(false)
                        .build(),
                ComputePrice.builder()
                        .accountId(discountBaId)
                        .osType(OSType.RHEL)
                        .hourlyComputeRate(LINUX_PRICE * 0.8)
                        .hourlyLicenseRate(RHEL_PRICE_ADJUSTMENT * 0.8)
                        .isBasePrice(false)
                        .build()));

    }

    /**
     * Test Azure compute price with discount.
     */
    @Test
    public void testAzureComputePriceBundleWithDiscount() {
        final long noDiscountBaId = 7L;
        final DiscountApplicator<TopologyEntityDTO> noDiscount = DiscountApplicator.noDiscount();

        final long discountBaId = 17L;
        final DiscountApplicator<TopologyEntityDTO> discount = mock(DiscountApplicator.class);
        when(discount.getDiscountPercentage(AZURE_COMPUTE_TIER_ID)).thenReturn(trax(0.2));

        final CloudRateExtractor mktPriceTable = new CloudRateExtractor(topology, infoExtractor);

        AccountPricingData<TopologyEntityDTO> accountPricingData1 =
                Mockito.mock(AccountPricingData.class);
        when(accountPricingData1.getDiscountApplicator()).thenReturn(noDiscount);
        when(accountPricingData1.getPriceTable()).thenReturn(COMPUTE_PRICE_TABLE);
        when(accountPricingData1.getAccountPricingDataOid()).thenReturn(noDiscountBaId);
        when(cloudCostData.getAccountPricingData(noDiscountBaId))
                .thenReturn(Optional.of(accountPricingData1));
        initializeAzureLicensePriceTuples(noDiscountBaId);

        AccountPricingData<TopologyEntityDTO> accountPricingData2 =
                Mockito.mock(AccountPricingData.class);
        when(accountPricingData2.getDiscountApplicator()).thenReturn(discount);
        when(accountPricingData2.getPriceTable()).thenReturn(COMPUTE_PRICE_TABLE);
        when(accountPricingData2.getAccountPricingDataOid()).thenReturn(discountBaId);
        when(cloudCostData.getAccountPricingData(discountBaId))
                .thenReturn(Optional.of(accountPricingData2));
        initializeAzureLicensePriceTuples(discountBaId);

        final ComputePriceBundle priceBundle1 = mktPriceTable.getComputePriceBundle(AZURE_COMPUTE_TIER, REGION_ID, accountPricingData1);

        final ComputePriceBundle priceBundle2 = mktPriceTable.getComputePriceBundle(AZURE_COMPUTE_TIER, REGION_ID, accountPricingData2);

        assertThat(priceBundle1.getPrices(), containsInAnyOrder(
                ComputePrice.builder()
                        .accountId(noDiscountBaId)
                        .osType(OSType.LINUX)
                        .hourlyComputeRate(LINUX_PRICE)
                        .hourlyLicenseRate(0.0)
                        .isBasePrice(true)
                        .build(),
                ComputePrice.builder()
                        .accountId(noDiscountBaId)
                        .osType(OSType.WINDOWS)
                        .hourlyComputeRate(LINUX_PRICE)
                        .hourlyLicenseRate(WINDOWS_PRICE_ADJUSTMENT)
                        .isBasePrice(false)
                        .build(),
                ComputePrice.builder()
                        .accountId(noDiscountBaId)
                        .osType(OSType.WINDOWS_WITH_SQL_WEB)
                        .hourlyComputeRate(LINUX_PRICE)
                        .hourlyLicenseRate(WINDOWS_SQL_WEB_PRICE_ADJUSTMENT)
                        .isBasePrice(false)
                        .build(),
                ComputePrice.builder()
                        .accountId(noDiscountBaId)
                        .osType(OSType.RHEL)
                        .hourlyComputeRate(LINUX_PRICE)
                        .hourlyLicenseRate(RHEL_PRICE_ADJUSTMENT)
                        .isBasePrice(false)
                        .build()));

        assertThat(priceBundle2.getPrices(), containsInAnyOrder(
                ComputePrice.builder()
                        .accountId(discountBaId)
                        .osType(OSType.LINUX)
                        .hourlyComputeRate(LINUX_PRICE * 0.8)
                        .hourlyLicenseRate(0.0)
                        .isBasePrice(true)
                        .build(),
                ComputePrice.builder()
                        .accountId(discountBaId)
                        .osType(OSType.WINDOWS)
                        .hourlyComputeRate(LINUX_PRICE * 0.8)
                        .hourlyLicenseRate(WINDOWS_PRICE_ADJUSTMENT * 0.8)
                        .isBasePrice(false)
                        .build(),
                ComputePrice.builder()
                        .accountId(discountBaId)
                        .osType(OSType.WINDOWS_WITH_SQL_WEB)
                        .hourlyComputeRate(LINUX_PRICE * 0.8)
                        .hourlyLicenseRate(WINDOWS_SQL_WEB_PRICE_ADJUSTMENT * 0.8)
                        .isBasePrice(false)
                        .build(),
                ComputePrice.builder()
                        .accountId(discountBaId)
                        .osType(OSType.RHEL)
                        .hourlyComputeRate(LINUX_PRICE * 0.8)
                        .hourlyLicenseRate(RHEL_PRICE_ADJUSTMENT * 0.8)
                        .isBasePrice(false)
                        .build()));
    }

    /**
     * Test storage price bundle per month.
     */
    @Test
    public void testStoragePriceBundleGBMonth() {
        // $10 for the first 7 GB-month, $15 for the next 3, $20 afterwards. (Not accumulative)
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
                                .setAmount(CostProtoUtil.getUnitPriceAmount(Unit.GB_MONTH, 15)))
                            .setUnit(Unit.GB_MONTH)
                            .setEndRangeInUnits(10))
                        .addPrices(Price.newBuilder()
                            .setPriceAmount(CurrencyAmount.newBuilder()
                                .setAmount(CostProtoUtil.getUnitPriceAmount(Unit.GB_MONTH, 20)))
                            .setUnit(Unit.GB_MONTH)))
                    .build())
                .build())
            .build();
        final long baId = 7L;
        final CloudRateExtractor mktPriceTable = new CloudRateExtractor(topology, infoExtractor);
        AccountPricingData accountPricingData =
                new AccountPricingData<>(DiscountApplicator.noDiscount(), priceTable, baId);
        final StoragePriceBundle storagePriceBundle =
                mktPriceTable.getStoragePriceBundle(STORAGE_TIER_ID, REGION_ID, accountPricingData);
        final StorageTierPriceData[] expectedData = new StorageTierPriceData[]{
                StorageTierPriceData.newBuilder()
                    .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(baId)
                            .setRegionId(REGION_ID).setPrice(10).build())
                    // Unit price true because it's priced per GB-month
                    .setIsUnitPrice(true)
                    // Accumulative price is always false for storage tiers.
                    .setIsAccumulativeCost(false)
                    .setUpperBound(7)
                    .build(),
                StorageTierPriceData.newBuilder()
                    .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(baId)
                            .setRegionId(REGION_ID).setPrice(15).build())
                    // Unit price true because it's priced per GB-month
                    .setIsUnitPrice(true)
                    // Accumulative price is always false for storage tiers.
                    .setIsAccumulativeCost(false)
                    .setUpperBound(10)
                    .build(),
                StorageTierPriceData.newBuilder()
                    .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(baId)
                            .setRegionId(REGION_ID).setPrice(20).build())
                    // Unit price true because it's priced per GB-month
                    .setIsUnitPrice(true)
                    // Accumulative price is always false for storage tiers.
                    .setIsAccumulativeCost(false)
                    .setUpperBound(Double.POSITIVE_INFINITY)
                    .build()};

        // The prices for all storage amount commodities should be the same, because the price
        // tables don't distinguish by commodity key.
        assertThat(storagePriceBundle.getPrices(FOO_STORAGE_AMOUNT_COMM), contains(expectedData));
        assertThat(storagePriceBundle.getPrices(BAR_STORAGE_AMOUNT_COMM), contains(expectedData));
        assertThat(storagePriceBundle.getPrices(FOO_STORAGE_ACCESS_COMM), is(Collections.emptyList()));
        assertThat(storagePriceBundle.getPrices(BAR_STORAGE_ACCESS_COMM), is(Collections.emptyList()));
    }

    /**
     * Test storage price for iops.
     */
    @Test
    public void testStoragePriceBundleIOPSMonth() {
        // $10 for the first 7 million-iops, $15 afterwards. (Not accumulative)
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
                                .setAmount(CostProtoUtil.getUnitPriceAmount(Unit.MILLION_IOPS, 15)))
                            .setUnit(Unit.MILLION_IOPS)))
                    .build())
                .build())
            .build();

        final long baId = 7L;
        doReturn(ImmutableMap.of(baId, makeBusinessAccount(baId, DiscountApplicator.noDiscount())))
                .when(topology).getEntities();
        final CloudRateExtractor mktPriceTable = new CloudRateExtractor(topology, infoExtractor);
        AccountPricingData accountPricingData =
                new AccountPricingData<>(DiscountApplicator.noDiscount(), priceTable, baId);
        final StoragePriceBundle storagePriceBundle =
                mktPriceTable.getStoragePriceBundle(STORAGE_TIER_ID, REGION_ID, accountPricingData);

        final List<StorageTierPriceData> expectedData = Lists.newArrayList(StorageTierPriceData.newBuilder()
                .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(baId)
                        .setRegionId(REGION_ID).setPrice(10).build())
                // Unit price true because it's priced per million-iops.
                .setIsUnitPrice(true)
                // Accumulative price is always false for storage tiers.
                .setIsAccumulativeCost(false)
                .setUpperBound(7)
                .build(),
                StorageTierPriceData.newBuilder()
                        .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(baId)
                                .setRegionId(REGION_ID).setPrice(15).build())
                        // Unit price true because it's priced per million-iops.
                        .setIsUnitPrice(true)
                        // Accumulative price is always false for storage tiers.
                        .setIsAccumulativeCost(false)
                        .setUpperBound(Double.POSITIVE_INFINITY)
                        .build());

        // The prices for all storage amount commodities should be the same, because the price
        // tables don't distinguish by commodity key.
        assertThat(storagePriceBundle.getPrices(FOO_STORAGE_ACCESS_COMM), contains(expectedData.toArray()));
        assertThat(storagePriceBundle.getPrices(BAR_STORAGE_ACCESS_COMM), contains(expectedData.toArray()));
        assertThat(storagePriceBundle.getPrices(FOO_STORAGE_AMOUNT_COMM), is(Collections.emptyList()));
        assertThat(storagePriceBundle.getPrices(BAR_STORAGE_AMOUNT_COMM), is(Collections.emptyList()));
    }

    /**
     * Test Storage price bundle flat cost.
     */
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
        when(cloudCostData.getAccountPricingData(BUSINESS_ACCOUNT_ID)).thenReturn(Optional.ofNullable(accountPricingData));
        when(cloudCostData.getAccountPricingData(BUSINESS_ACCOUNT_ID).get().getPriceTable()).thenReturn(priceTable);
        when(accountPricingData.getDiscountApplicator()).thenReturn(DiscountApplicator.noDiscount());
        final long baId = 7L;
        when(accountPricingData.getAccountPricingDataOid()).thenReturn(baId);
        doReturn(ImmutableMap.of(baId, makeBusinessAccount(baId, DiscountApplicator.noDiscount())))
                .when(topology).getEntities();
        final CloudRateExtractor mktPriceTable = new CloudRateExtractor(topology, infoExtractor);
        final StoragePriceBundle storagePriceBundle =
                mktPriceTable.getStoragePriceBundle(STORAGE_TIER_ID, REGION_ID, accountPricingData);

        final StorageTierPriceData expectedData = StorageTierPriceData.newBuilder()
                .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(baId)
                        .setRegionId(REGION_ID).setPrice(10).build())
                // Not unit price, because it's a flat cost.
                .setIsUnitPrice(false)
                // Not accumulative because we don't have ranges.
                .setIsAccumulativeCost(false)
                .setUpperBound(Double.POSITIVE_INFINITY)
                .build();

        assertThat(storagePriceBundle.getPrices(FOO_STORAGE_AMOUNT_COMM), contains(expectedData));
        assertThat(storagePriceBundle.getPrices(BAR_STORAGE_AMOUNT_COMM), contains(expectedData));
        assertThat(storagePriceBundle.getPrices(FOO_STORAGE_ACCESS_COMM), is(Collections.emptyList()));
        assertThat(storagePriceBundle.getPrices(BAR_STORAGE_ACCESS_COMM), is(Collections.emptyList()));
    }

    /**
     * Test Storage price bundle with flat cost ranges.
     */
    @Test
    public void testStoragePriceBundleFlatCostRanges() {
        // $10/month for the first 7 GB, $15/month afterwards. (not accumulative)
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
                                .setAmount(CostProtoUtil.getUnitPriceAmount(Unit.MONTH, 15)))
                            .setUnit(Unit.MONTH)))
                    .build())
                .build())
            .build();

        final long baId = 7L;
        doReturn(ImmutableMap.of(baId, makeBusinessAccount(baId, DiscountApplicator.noDiscount())))
                .when(topology).getEntities();
        final CloudRateExtractor mktPriceTable = new CloudRateExtractor(topology, infoExtractor);
        AccountPricingData<TopologyEntityDTO> accountPricingData =
                Mockito.mock(AccountPricingData.class);
        when(accountPricingData.getAccountPricingDataOid()).thenReturn(baId);
        when(accountPricingData.getDiscountApplicator()).thenReturn(DiscountApplicator.noDiscount());
        when(cloudCostData.getAccountPricingData(baId)).thenReturn(Optional.of(accountPricingData));
        when(cloudCostData.getAccountPricingData(baId).get().getPriceTable()).thenReturn(priceTable);
        final StoragePriceBundle storagePriceBundle =
                mktPriceTable.getStoragePriceBundle(STORAGE_TIER_ID, REGION_ID, accountPricingData);

        final List<StorageTierPriceData> expectedData = Lists.newArrayList(
            StorageTierPriceData.newBuilder()
                .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(baId)
                        .setRegionId(REGION_ID).setPrice(10).build())
                // Not unit price, because it's a flat cost for each range.
                .setIsUnitPrice(false)
                // Accumulative price is false for all storage tiers.
                .setIsAccumulativeCost(false)
                .setUpperBound(7)
                .build(),
            StorageTierPriceData.newBuilder()
                .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(baId)
                        .setRegionId(REGION_ID).setPrice(15).build())
                // Not unit price, because it's a flat cost for each range.
                .setIsUnitPrice(false)
                // Accumulative price is always false for storage tiers.
                .setIsAccumulativeCost(false)
                .setUpperBound(Double.POSITIVE_INFINITY)
                .build());

        // The prices for all storage amount commodities should be the same, because the price
        // tables don't distinguish by commodity key.
        assertThat(storagePriceBundle.getPrices(FOO_STORAGE_AMOUNT_COMM), contains(expectedData.toArray()));
        assertThat(storagePriceBundle.getPrices(BAR_STORAGE_AMOUNT_COMM), contains(expectedData.toArray()));
        assertThat(storagePriceBundle.getPrices(FOO_STORAGE_ACCESS_COMM), is(Collections.emptyList()));
        assertThat(storagePriceBundle.getPrices(BAR_STORAGE_ACCESS_COMM), is(Collections.emptyList()));
    }

    /**
     * Test storage price per month with a discount.
     */
    @Test
    public void testStoragePriceBundleGBMonthWithDiscount() {
        // $10 for the first 7 GB-month, $15 afterwards. (Not accumulative)
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
                                .setAmount(CostProtoUtil.getUnitPriceAmount(Unit.GB_MONTH, 15)))
                            .setUnit(Unit.GB_MONTH)))
                    .build())
                .build())
            .build();

        final long baId = 7L;
        // Add a 20% discount for the storage tier.
        final DiscountApplicator<TopologyEntityDTO> discount = mock(DiscountApplicator.class);
        when(discount.getDiscountPercentage(STORAGE_TIER_ID)).thenReturn(trax(0.2));

        final CloudRateExtractor mktPriceTable = new CloudRateExtractor(topology, infoExtractor);
        AccountPricingData<TopologyEntityDTO> accountPricingData
                = Mockito.mock(AccountPricingData.class);
        when(cloudCostData.getAccountPricingData(baId)).thenReturn(Optional.of(accountPricingData));
        when(cloudCostData.getAccountPricingData(baId).get().getPriceTable()).thenReturn(priceTable);
        when(accountPricingData.getDiscountApplicator()).thenReturn(discount);
        when(accountPricingData.getAccountPricingDataOid()).thenReturn(baId);
        final StoragePriceBundle storagePriceBundle =
                mktPriceTable.getStoragePriceBundle(STORAGE_TIER_ID, REGION_ID, accountPricingData);
        final List<StorageTierPriceData> expectedData = Lists.newArrayList(
                StorageTierPriceData.newBuilder()
                    .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(baId)
                            .setRegionId(REGION_ID).setPrice(8).build())
                    // Unit price true because it's priced per GB-month
                    .setIsUnitPrice(true)
                    // Accumulative price is always false for storage tiers.
                    .setIsAccumulativeCost(false)
                    .setUpperBound(7)
                    // 20% off $10
                    .build(),
                StorageTierPriceData.newBuilder()
                    .addCostTupleList(CostTuple.newBuilder().setBusinessAccountId(baId)
                            .setRegionId(REGION_ID).setPrice(12).build())
                    // Unit price true because it's priced per GB-month
                    .setIsUnitPrice(true)
                    // Accumulative price is always false for storage tiers.
                    .setIsAccumulativeCost(false)
                    .setUpperBound(Double.POSITIVE_INFINITY)
                    .build());

        // The prices for all storage amount commodities should be the same, because the price
        // tables don't distinguish by commodity key.
        assertThat(storagePriceBundle.getPrices(FOO_STORAGE_AMOUNT_COMM), contains(expectedData.toArray()));
        assertThat(storagePriceBundle.getPrices(BAR_STORAGE_AMOUNT_COMM), contains(expectedData.toArray()));
        assertThat(storagePriceBundle.getPrices(FOO_STORAGE_ACCESS_COMM), is(Collections.emptyList()));
        assertThat(storagePriceBundle.getPrices(BAR_STORAGE_ACCESS_COMM), is(Collections.emptyList()));
    }

    /**
     * Test that IO2 storageTier cost price has a tiny decrease.
     */
    @Test
    public void testIO2StorageTierPriceWithTinyDecrease() {
        final double stAmountHourlyPrice = 2E-4;
        final double iopsHourlyPrice = 1E-4;
        final PriceTable priceTable = PriceTable.newBuilder()
                .putOnDemandPriceByRegionId(REGION_ID, OnDemandPriceTable.newBuilder()
                        .putCloudStoragePricesByTierId(IO2_TIER_ID, StorageTierPriceList.newBuilder()
                                .addCloudStoragePrice(StorageTierPrice.newBuilder()
                                        .addPrices(Price.newBuilder()
                                                .setPriceAmount(CurrencyAmount.newBuilder()
                                                        .setAmount(CostProtoUtil.getUnitPriceAmount(Unit.GB_MONTH, stAmountHourlyPrice)))
                                                .setUnit(Unit.GB_MONTH))
                                        .addPrices(Price.newBuilder()
                                                .setPriceAmount(CurrencyAmount.newBuilder()
                                                        .setAmount(CostProtoUtil.getUnitPriceAmount(Unit.MILLION_IOPS, iopsHourlyPrice)))
                                                .setUnit(Unit.MILLION_IOPS)))
                                .build())
                        .build())
                .build();
        final long baId = 7L;
        final CloudRateExtractor mktPriceTable = new CloudRateExtractor(topology, infoExtractor);
        AccountPricingData accountPricingData =
                new AccountPricingData<>(DiscountApplicator.noDiscount(), priceTable, baId);
        final StoragePriceBundle storagePriceBundle =
                mktPriceTable.getStoragePriceBundle(IO2_TIER_ID, REGION_ID, accountPricingData);
        final double tinyDecrease = Math.ulp(1.0);
        assertEquals(1, storagePriceBundle.getPrices(FOO_STORAGE_AMOUNT_COMM).size());
        StorageTierPriceData stAmountPriceData = storagePriceBundle.getPrices(FOO_STORAGE_AMOUNT_COMM).get(0);
        assertEquals(1, stAmountPriceData.getCostTupleListCount());
        CostTuple stAmountCostTuple = stAmountPriceData.getCostTupleList(0);
        assertEquals(stAmountHourlyPrice - tinyDecrease, stAmountCostTuple.getPrice(), 1E-16);

        assertEquals(1, storagePriceBundle.getPrices(FOO_STORAGE_ACCESS_COMM).size());
        StorageTierPriceData iopsPriceData = storagePriceBundle.getPrices(FOO_STORAGE_ACCESS_COMM).get(0);
        assertEquals(1, iopsPriceData.getCostTupleListCount());
        CostTuple iopsCostTuple = iopsPriceData.getCostTupleList(0);
        assertEquals(iopsHourlyPrice - tinyDecrease, iopsCostTuple.getPrice(), 1E-16);
    }

    private TopologyEntityDTO makeBusinessAccount(final long id,
              final DiscountApplicator<TopologyEntityDTO> discountApplicator) {
        final TopologyEntityDTO businessAccount = TopologyEntityDTO.newBuilder()
                .setEntityType(EntityType.BUSINESS_ACCOUNT_VALUE)
                .setOid(id)
                .build();
        doReturn(discountApplicator).when(discountApplicatorFactory).accountDiscountApplicator(id, topology, infoExtractor,
                Optional.ofNullable(discountApplicator.getDiscount()));
        return businessAccount;
    }
}
