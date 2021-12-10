package com.vmturbo.cost.calculation;

import static com.vmturbo.trax.Trax.trax;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.cost.EntityUptime.EntityUptimeDTO;
import com.vmturbo.common.protobuf.cost.Pricing.DbServerTierOnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.DbTierOnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.SpotInstancePriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.SpotInstancePriceTable.PriceForGuestOsType;
import com.vmturbo.common.protobuf.cost.Pricing.SpotInstancePriceTable.SpotPricesForTier;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.commons.Units;
import com.vmturbo.cost.calculation.CloudCostCalculator.CloudCostCalculatorFactory;
import com.vmturbo.cost.calculation.CloudCostCalculator.DependentCostLookup;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.ReservedInstanceApplicator.ReservedInstanceApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.ComputeConfig;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.ComputeTierConfig;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.NetworkConfig;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.VirtualVolumeConfig;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.cost.calculation.journal.CostJournal.CostSourceFilter;
import com.vmturbo.cost.calculation.topology.AccountPricingData;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.RedundancyType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.LicenseModel;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList.ComputeTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseServerTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseServerTierPriceList.DatabaseServerTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.IpPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.IpPriceList.IpConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry.LicensePrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList.StorageTierPrice;
import com.vmturbo.trax.TraxNumber;

/**
 * Unit tests for {@link CloudCostCalculator}.
 */
@SuppressWarnings("unchecked")
public class CloudCostCalculatorTest {

    private static final CloudTopology<TestEntityClass> topology =
            (CloudTopology<TestEntityClass>)mock(CloudTopology.class);

    private static final EntityInfoExtractor<TestEntityClass> infoExtractor =
            (EntityInfoExtractor<TestEntityClass>)mock(EntityInfoExtractor.class);

    private static final CloudCostCalculatorFactory<TestEntityClass> calculatorFactory =
            CloudCostCalculator.newFactory();

    private static final DiscountApplicatorFactory<TestEntityClass> discountApplicatorFactory =
            (DiscountApplicatorFactory<TestEntityClass>)mock(DiscountApplicatorFactory.class);

    private static final ReservedInstanceApplicatorFactory<TestEntityClass> reservedInstanceApplicatorFactory =
            (ReservedInstanceApplicatorFactory<TestEntityClass>)mock(ReservedInstanceApplicatorFactory.class);

    private static final Map<Long, EntityReservedInstanceCoverage> topologyRiCoverage = Maps.newHashMap();

    enum Provider {
        AWS_Azure,
        GCP
    }

    private static final NetworkConfig networkConfig = mock(NetworkConfig.class);
    private static final long ACCOUNT_PRICING_DATA_OID = 15L;
    private static final long RI_ID = 100L;
    private static final Long RI_SPEC_ID = 101L;

    private final ComputeConfig computeConfig = new ComputeConfig(OSType.WINDOWS, Tenancy.DEFAULT,
            VMBillingType.RESERVED, 4, EntityDTO.LicenseModel.LICENSE_INCLUDED, null);

    private static final double BASE_PRICE = 10.0;
    private static final double MEM_PROVISIONED_PRICE = 5.0;
    private static final double NUM_VCORE_PRICE = 6.0;
    private static final double DEFAULT_RI_COVERAGE = 0.2;
    private static final double SUSE_ADJUSTMENT = 5;
    private static final double WSQL_ADJUSTMENT = 7;
    private static final double WSQL_ENTERPRISE_1 = 10;
    private static final double WSQL_ENTERPRISE_2 = 20;
    private static final double WSQL_ENTERPRISE_8 = 40;
    private static final long DEFAULT_ELASTIC_IPS_BOUGHT = 10;
    private static final int IP_COUNT = 5;
    private static final int IP_RANGE = 3;
    private static final double IP_PRICE_RANGE_1 = 8;
    private static final double IP_PRICE = 14;
    private static final double EXPECTED_IP_COST = IP_RANGE * IP_PRICE_RANGE_1 + (IP_COUNT - IP_RANGE) * IP_PRICE;
    private static final double EXPECTED_IDLE_IP_COST =
        IP_RANGE * IP_PRICE_RANGE_1 + (DEFAULT_ELASTIC_IPS_BOUGHT - IP_RANGE) * IP_PRICE;
    private static final double MYSQL_ADJUSTMENT = 5;
    private static final int GB_RANGE = 11;
    private static final int GB_RANGE_SLOT_2 = 21;
    private static final int INCREMENT_INTERVAL_GB = 1;
    private static final int INCREMENT_INTERVAL_GB_SLOT_2 = 5;
    private static final int STORAGE_RANGE = 11264; // 11 GB in MBytes.
    private static final double STORAGE_PRICE = 1.0;
    private static final double GB_PRICE_RANGE_1 = 13.0; // price per GB within GB_RANGE
    private static final double GB_PRICE = 9.0; // price per GB above the range
    private static final double GB_PRICE_RAGRS = 10.0;
    private static final double GB_PRICE_GRS = 11.0;
    private static final int IOPS_RANGE = 11;
    private static final double IOPS_PRICE_RANGE_1 = 16.0; // price per GB within GB_RANGE
    private static final double IOPS_PRICE = 4.5; // price per GB above the range
    private static final double GB_MONTH_PRICE_10 = 14.0;
    private static final double GB_MONTH_PRICE_20 = 26.0;
    private static final double GB_MONTH_PRICE_32 = 38.0;
    private static final double MBPS_PRICE = 23.42;
    private static final double IO_REQUESTS_PRICE = 5.00;
    private static final long V_VOLUME_SIZE_IOPS = 17;
    private static final long V_VOLUME_SIZE_MBPS = 23;
    private static final long V_VOLUME_IO_REQUESTS = 34;

    private static final long REGION_ID = 1;
    private static final long AVAILABILITY_ZONE_ID = 3;
    private static final long BUSINESS_ACCOUNT_ID = 2;
    private static final long STORAGE_TIER_ID = 10;
    private static final long STORAGE_TIER_ID_2 = 13;
    private static final long COMPUTE_TIER_ID = 11;
    private static final long DB_TIER_ID = 4;
    private static final long DB_SERVER_TIER_ID = 5;
    private static final long VOLUME_ID = 7;
    private static final long DEFAULT_VM_ID = 7;
    private static final long DEFAULT_SERVICE_ID = 0;
    private static final int MIN_RDS_STORAGE = 20;
    private static final int MAX_RDS_STORAGE = 1000;
    private static final int MIN_RDS_IOPS = 100;
    private static final int MAX_RDS_IOPS = 1000;

    private static final TestEntityClass region = TestEntityClass.newBuilder(REGION_ID)
                    .build(infoExtractor);
    private static final TestEntityClass availabilityZone = TestEntityClass.newBuilder(AVAILABILITY_ZONE_ID)
                    .build(infoExtractor);
    private static final TestEntityClass businessAccount = TestEntityClass.newBuilder(BUSINESS_ACCOUNT_ID)
            .build(infoExtractor);
    private static final TestEntityClass storageTier = TestEntityClass.newBuilder(STORAGE_TIER_ID)
                    .build(infoExtractor);
    private static final TestEntityClass computeTier = TestEntityClass.newBuilder(COMPUTE_TIER_ID)
                    .build(infoExtractor);
    private static final TestEntityClass databaseTier = TestEntityClass.newBuilder(DB_TIER_ID)
                    .build(infoExtractor);
    private static final TestEntityClass databaseServerTier = TestEntityClass.newBuilder(DB_SERVER_TIER_ID)
        .build(infoExtractor);

    private static final PriceTable PRICE_TABLE_AWS = thePriceTable(Provider.AWS_Azure);
    private static final PriceTable PRICE_TABLE_GCP = thePriceTable(Provider.GCP);

    private static final CloudCostData CLOUD_COST_DATA = new CloudCostData(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Optional.empty());
    private static final double DELTA = 0.0001;

    private static Price price = Price.newBuilder().setPriceAmount(CurrencyAmount.newBuilder().setAmount(0)).build();

    /**
     * Test a simple on-demand calculation (no RI, no discount) for a VM.
     */
    @Before
    public void init() {
        // Configure NetworkConfig
        when(networkConfig.getNumElasticIps()).thenReturn(DEFAULT_ELASTIC_IPS_BOUGHT);

        final TestEntityClass service = TestEntityClass.newBuilder(DEFAULT_SERVICE_ID)
            .build(infoExtractor);
        // Configure CloudTopology
        when(topology.getConnectedRegion(DEFAULT_VM_ID)).thenReturn(Optional.of(region));
        when(topology.getConnectedAvailabilityZone(DEFAULT_VM_ID)).thenReturn(Optional.of(availabilityZone));
        when(topology.getOwner(DEFAULT_VM_ID)).thenReturn(Optional.of(businessAccount));
        when(topology.getConnectedService(COMPUTE_TIER_ID)).thenReturn(Optional.of(service));
        when(topology.getComputeTier(DEFAULT_VM_ID)).thenReturn(Optional.of(computeTier));

        // Configure ReservedInstanceApplicator
        final ReservedInstanceApplicator<TestEntityClass> riApplicator =
            mock(ReservedInstanceApplicator.class);
        when(riApplicator.recordRICoverage(computeTier, price, true)).thenReturn(trax(DEFAULT_RI_COVERAGE));
        when(reservedInstanceApplicatorFactory.newReservedInstanceApplicator(
            any(), eq(infoExtractor), eq(CLOUD_COST_DATA), eq(topologyRiCoverage)))
            .thenReturn(riApplicator);
    }

    private TestEntityClass createVmTestEntity(long id, EntityState state, OSType osType,
            Tenancy tenancy, VMBillingType billingType, int numCores,
            EntityDTO.LicenseModel licenseModel, Map<com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType, Double> pricedCommoditiesBought) {
        return TestEntityClass.newBuilder(id)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEntityState(state)
                .setComputeConfig(new EntityInfoExtractor.ComputeConfig(osType,
                        tenancy, billingType, numCores, licenseModel, pricedCommoditiesBought))
                .setNetworkConfig(networkConfig)
                .build(infoExtractor);
    }

    private TestEntityClass createVmTestEntity(long id, OSType osType, Tenancy tenancy,
            VMBillingType billingType, int numCores, EntityDTO.LicenseModel licenseModel,
            Map<com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType, Double> pricedCommoditiesBought) {
        return createVmTestEntity(id, EntityState.POWERED_ON, osType, tenancy, billingType,
                numCores, licenseModel, pricedCommoditiesBought);
    }

    private DiscountApplicator<TestEntityClass> setupDiscountApplicator(double returnDiscount) {
        DiscountApplicator<TestEntityClass> discountApplicator =
                (DiscountApplicator<TestEntityClass>)mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(any())).thenReturn(trax(returnDiscount));
        when(discountApplicatorFactory.accountDiscountApplicator(any(), any(), any(), any()))
                .thenReturn(discountApplicator);
        return discountApplicator;
    }

    @Test
    public void testCalculateOnDemandCostForComputeWithPartialUptime() {
        DiscountApplicator<TestEntityClass> discountApplicator = setupDiscountApplicator(0.0);
        final float entityDiscountMultiplier = 0.2f;
        TestEntityClass linuxVM = createVmTestEntity(DEFAULT_VM_ID, OSType.LINUX, Tenancy.DEFAULT,
                VMBillingType.ONDEMAND, 4,
                EntityDTO.LicenseModel.LICENSE_INCLUDED, null);

        AccountPricingData accountPricingData = new AccountPricingData(discountApplicator, PRICE_TABLE_AWS,
                15L, 20L, businessAccount.getId());
        final ComputeTierConfig computeTierConfig = ComputeTierConfig.builder()
                .computeTierOid(computeTier.getId())
                .numCores(4)
                .numCoupons(0)
                .isBurstableCPU(false)
                .build();
        when(infoExtractor.getComputeTierConfig(computeTier)).thenReturn(Optional.of(computeTierConfig));

        CloudCostData cloudCostData = createCloudCostDataWithAccountPricingTable(businessAccount.getId(), accountPricingData);
        cloudCostData.entityUptimeByEntityId.put(DEFAULT_VM_ID,
                EntityUptimeDTO.newBuilder().setUptimePercentage(80D).build());
        when(topology.getConnectedRegion(DEFAULT_VM_ID)).thenReturn(Optional.of(region));
        when(topology.getOwner(DEFAULT_VM_ID)).thenReturn(Optional.of(businessAccount));
        when(topology.getStorageTier(DEFAULT_VM_ID)).thenReturn(Optional.of(storageTier));
        when(topology.getComputeTier(DEFAULT_VM_ID)).thenReturn(Optional.of(computeTier));

        final TestEntityClass volume = TestEntityClass.newBuilder(123)
                .setType(EntityType.VIRTUAL_VOLUME_VALUE)
                .build(infoExtractor);
        when(topology.getAttachedVolumes(DEFAULT_VM_ID)).thenReturn(Collections.singletonList(volume));
        when(topology.getOwner(123)).thenReturn(Optional.of(businessAccount));
        when(topology.getConnectedRegion(123)).thenReturn(Optional.of(region));
        when(topology.getConnectedAvailabilityZone(123)).thenReturn(Optional.of(availabilityZone));

        // Configure ReservedInstanceApplicator to give 0 coverage
        final ReservedInstanceApplicator<TestEntityClass> riApplicator =
                mock(ReservedInstanceApplicator.class);
        when(riApplicator.recordRICoverage(eq(computeTier), any(), any())).thenReturn(trax(0F));
        when(reservedInstanceApplicatorFactory.newReservedInstanceApplicator(
                any(), eq(infoExtractor), eq(cloudCostData), eq(topologyRiCoverage)))
                .thenReturn(riApplicator);

        final CostJournal<TestEntityClass> volumeJournal =
                CostJournal.newBuilder(volume, infoExtractor, region, discountApplicator, e2 -> null)
                        // Just a mock price that's easy to work with.
                        .recordOnDemandCost(CostCategory.STORAGE, storageTier, Price.newBuilder()
                                .setUnit(Unit.HOURS)
                                .setPriceAmount(CurrencyAmount.newBuilder()
                                        .setAmount(5))
                                .build(), trax(1), Optional.empty())
                        .build();
        // The cost lookup will tell the VM's cost journal what the cost journal of the volume
        // looks like.
        final DependentCostLookup<TestEntityClass> volumeCostLookup = e -> volumeJournal;
        final CloudCostCalculator<TestEntityClass> calculator =
                calculatorFactory.newCalculator(cloudCostData, topology, infoExtractor,
                        reservedInstanceApplicatorFactory,
                        volumeCostLookup, topologyRiCoverage);

        final CostJournal<TestEntityClass> vmJournal = calculator.calculateCost(linuxVM);

        // The cost of the RI isn't factored in because we mocked out the RI Applicator.
        Assert.assertEquals(vmJournal.getTotalHourlyCost().getValue(),
                ((BASE_PRICE ) *  (1 - entityDiscountMultiplier) // Ondemand
                        + 5 // storage
                        + EXPECTED_IP_COST ), 0.0001F); // IP


    }

    @Test
    public void testCalculateOnDemandCostForComputeWithRIAndUptime() {
        DiscountApplicator<TestEntityClass> discountApplicator = setupDiscountApplicator(0.0);
        when(discountApplicator.getDiscountPercentage(computeTier.getId())).thenReturn(trax(0D));
        final float entityDiscountMultiplier = 0.2f;
        TestEntityClass linuxVM = createVmTestEntity(DEFAULT_VM_ID, OSType.LINUX, Tenancy.DEFAULT,
                VMBillingType.ONDEMAND, 4,
                EntityDTO.LicenseModel.LICENSE_INCLUDED, null);

        AccountPricingData accountPricingData = new AccountPricingData(discountApplicator, PRICE_TABLE_AWS,
                15L, 20L, businessAccount.getId());
        final ComputeTierConfig computeTierConfig = ComputeTierConfig.builder()
                .computeTierOid(computeTier.getId())
                .numCores(4)
                .numCoupons(10)
                .isBurstableCPU(false)
                .build();
        when(infoExtractor.getComputeTierConfig(computeTier)).thenReturn(Optional.of(computeTierConfig));

        Map<Long, AccountPricingData> accountPricingDataByBusinessAccount = new HashMap<>();
        accountPricingDataByBusinessAccount.put(businessAccount.getId(), accountPricingData);
        CloudCostData cloudCostData = new CloudCostData(Collections.emptyMap(), Collections.emptyMap(),
                ImmutableMap.of (RI_ID, ReservedInstanceBought.newBuilder()
                        .setId(RI_ID)
                        .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                                                    .setNumBought(10).setBusinessAccountId(businessAccount.getId())
                                                    .setReservedInstanceSpec(RI_SPEC_ID)
                                                    .setReservedInstanceBoughtCost(ReservedInstanceBoughtCost.newBuilder()
                                                            .setRecurringCostPerHour(CurrencyAmount.newBuilder().setAmount(0).build())
                                                            .setUsageCostPerHour(CurrencyAmount.newBuilder().setAmount(0).build())
                                                            .setFixedCost(CurrencyAmount.newBuilder().setAmount(0).build()).build())
                                                    .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                                                            .setNumberOfCoupons(10).setNumberOfCouponsUsed(10).build()).build()).build()),
                ImmutableMap.of (RI_SPEC_ID, ReservedInstanceSpec.newBuilder().setId(RI_SPEC_ID)
                        .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                                .setTierId(computeTier.getId())
                                .setType(ReservedInstanceType.newBuilder().setTermYears(1).build()).build()).build())
                , Collections.emptyMap(), accountPricingDataByBusinessAccount, new HashMap<>(), Optional.empty());

        cloudCostData.entityUptimeByEntityId.put(DEFAULT_VM_ID,
                EntityUptimeDTO.newBuilder().setUptimePercentage(80D).build());
        when(topology.getConnectedRegion(DEFAULT_VM_ID)).thenReturn(Optional.of(region));
        when(topology.getOwner(DEFAULT_VM_ID)).thenReturn(Optional.of(businessAccount));
        when(topology.getStorageTier(DEFAULT_VM_ID)).thenReturn(Optional.of(storageTier));
        when(topology.getComputeTier(DEFAULT_VM_ID)).thenReturn(Optional.of(computeTier));

        final TestEntityClass volume = TestEntityClass.newBuilder(123)
                .setType(EntityType.VIRTUAL_VOLUME_VALUE)
                .build(infoExtractor);
        when(topology.getAttachedVolumes(DEFAULT_VM_ID)).thenReturn(Collections.singletonList(volume));
        when(topology.getOwner(123)).thenReturn(Optional.of(businessAccount));
        when(topology.getConnectedRegion(123)).thenReturn(Optional.of(region));
        when(topology.getConnectedAvailabilityZone(123)).thenReturn(Optional.of(availabilityZone));

        // Configure ReservedInstanceApplicator to give 0 coverage
        topologyRiCoverage.put(linuxVM.getId(), EntityReservedInstanceCoverage.newBuilder().putCouponsCoveredByRi(RI_ID, 10)
                                                .setEntityId(linuxVM.getId()).setEntityCouponCapacity(10).build());
        final CostJournal<TestEntityClass> volumeJournal =
                CostJournal.newBuilder(volume, infoExtractor, region, discountApplicator, e2 -> null)
                        // Just a mock price that's easy to work with.
                        .recordOnDemandCost(CostCategory.STORAGE, storageTier, Price.newBuilder()
                                .setUnit(Unit.HOURS)
                                .setPriceAmount(CurrencyAmount.newBuilder()
                                        .setAmount(5))
                                .build(), trax(1), Optional.empty())
                        .build();
        // The cost lookup will tell the VM's cost journal what the cost journal of the volume
        // looks like.
        final DependentCostLookup<TestEntityClass> volumeCostLookup = e -> volumeJournal;
        final CloudCostCalculator<TestEntityClass> calculator =
                calculatorFactory.newCalculator(cloudCostData, topology, infoExtractor,
                        ReservedInstanceApplicator.newFactory(),
                        volumeCostLookup, topologyRiCoverage);

        final CostJournal<TestEntityClass> vmJournal = calculator.calculateCost(linuxVM);

        // The cost of the RI isn't factored in because we mocked out the RI Applicator.
        Assert.assertEquals(0 // 100 % coverage
                        + 5 // storage
                        + EXPECTED_IP_COST,
                        vmJournal.getTotalHourlyCost().getValue(), 0.0001F);
    }

    @Test
    public void testCalculateOnDemandCostForCompute() {
        DiscountApplicator<TestEntityClass> discountApplicator = setupDiscountApplicator(0.0);
        TestEntityClass wsqlVm4Cores = createVmTestEntity(DEFAULT_VM_ID, OSType.WINDOWS_WITH_SQL_ENTERPRISE, Tenancy.DEFAULT, VMBillingType.ONDEMAND, 4,
                EntityDTO.LicenseModel.LICENSE_INCLUDED, null);
        // act
        AccountPricingData accountPricingData = new AccountPricingData(discountApplicator, PRICE_TABLE_AWS, 15L, 20L, 30L);
        CloudCostData cloudCostData = createCloudCostDataWithAccountPricingTable(BUSINESS_ACCOUNT_ID, accountPricingData);
        final ComputeTierConfig computeTierConfig = ComputeTierConfig.builder()
                .computeTierOid(computeTier.getId())
                .numCores(4)
                .numCoupons(0)
                .isBurstableCPU(false)
                .build();
        when(infoExtractor.getComputeTierConfig(computeTier)).thenReturn(Optional.of(computeTierConfig));
        CloudCostCalculator cloudCostCalculator = calculator(cloudCostData);
        // Configure ReservedInstanceApplicator
        final ReservedInstanceApplicator<TestEntityClass> riApplicator =
                mock(ReservedInstanceApplicator.class);
        when(riApplicator.recordRICoverage(eq(computeTier), any(), any())).thenReturn(trax(DEFAULT_RI_COVERAGE));
        when(reservedInstanceApplicatorFactory.newReservedInstanceApplicator(
                any(), eq(infoExtractor), eq(cloudCostData), eq(topologyRiCoverage)))
                .thenReturn(riApplicator);
        final CostJournal<TestEntityClass> journal1 = cloudCostCalculator.calculateCost(wsqlVm4Cores);

        // assert

        // The cost of the RI isn't factored in because we mocked out the RI Applicator.
        assertThat(journal1.getTotalHourlyCost().getValue(),
            is((BASE_PRICE + WSQL_ADJUSTMENT) * 1 + EXPECTED_IP_COST));
        assertThat(journal1.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE).getValue(),
            is(BASE_PRICE * 1));
        assertThat(journal1.getHourlyCostForCategory(CostCategory.ON_DEMAND_LICENSE).getValue(),
            is(WSQL_ADJUSTMENT));
        assertThat(journal1.getHourlyCostForCategory(CostCategory.IP).getValue(), is(EXPECTED_IP_COST));

        // Once for the compute and once for the adjustment
        verify(discountApplicator, times(2)).getDiscountPercentage(computeTier);

        // In the previous test, the number of CPUs (4) was in between LicensePrice number of cores
        // (2 and 8) and we verified we use the higher cores license price.
        // Here we verify that when the number of CPUs equals one of the LicensePrice number of cores
        // then that price is used.
        TestEntityClass wsqlVm2Cores = createVmTestEntity(DEFAULT_VM_ID, OSType.WINDOWS_WITH_SQL_ENTERPRISE, Tenancy.DEFAULT, VMBillingType.ONDEMAND, 2,
                EntityDTO.LicenseModel.LICENSE_INCLUDED, null);
        // setup the 2 core config
        final ComputeTierConfig computeTierConfig2 = ComputeTierConfig.builder()
                .computeTierOid(computeTier.getId())
                .numCores(2)
                .numCoupons(0)
                .isBurstableCPU(false)
                .build();
        when(infoExtractor.getComputeTierConfig(computeTier)).thenReturn(Optional.of(computeTierConfig2));
        final CostJournal<TestEntityClass> journal2 = cloudCostCalculator.calculateCost(wsqlVm2Cores);
        assertThat(journal2.getHourlyCostForCategory(CostCategory.ON_DEMAND_LICENSE).getValue(),
            is(WSQL_ADJUSTMENT));

        final TestEntityClass spotVm = createVmTestEntity(DEFAULT_VM_ID, OSType.SUSE, Tenancy.DEFAULT, VMBillingType.BIDDING, 2,
                EntityDTO.LicenseModel.LICENSE_INCLUDED, null);
        final CostJournal<TestEntityClass> spotJournal = cloudCostCalculator.calculateCost(spotVm);
        // Spot costs do not have any portion covered by RI's.
        assertThat(spotJournal.getHourlyCostForCategory(CostCategory.SPOT).getValue(),
            is(BASE_PRICE * 1));
        // No adjustment and license costs for spot instances
        assertThat(spotJournal.getHourlyCostForCategory(CostCategory.ON_DEMAND_LICENSE).getValue(), is(0.0));

        final TestEntityClass suseVm = createVmTestEntity(DEFAULT_VM_ID, OSType.SUSE, Tenancy.DEFAULT, VMBillingType.ONDEMAND, 2,
                EntityDTO.LicenseModel.LICENSE_INCLUDED, null);

        final CostJournal<TestEntityClass> journal3 = cloudCostCalculator.calculateCost(suseVm);
        assertThat(journal3.getHourlyCostForCategory(CostCategory.ON_DEMAND_LICENSE).getValue(),
            is(SUSE_ADJUSTMENT * 1));
    }

    /**
     * Test that for AHuB VMs (Azure VMs which use Windows BYOL) we don't add the Windows price
     * for the total VM cost.
     */
    @Test
    public void testCalculateOnDemandCostForComputeForAhub() {
        DiscountApplicator<TestEntityClass> discountApplicator = setupDiscountApplicator(0.0);
        TestEntityClass windowsVm4CoresBYOL = createVmTestEntity(DEFAULT_VM_ID, OSType.WINDOWS, Tenancy.DEFAULT,
                VMBillingType.ONDEMAND, 4, EntityDTO.LicenseModel.AHUB, null);

        final ComputeTierConfig computeTierConfig = ComputeTierConfig.builder()
                .computeTierOid(computeTier.getId())
                .numCores(4)
                .numCoupons(0)
                .isBurstableCPU(false)
                .build();
        when(infoExtractor.getComputeTierConfig(computeTier)).thenReturn(Optional.of(computeTierConfig));
        AccountPricingData accountPricingData = new AccountPricingData(discountApplicator, PRICE_TABLE_AWS, 15L, 20L, 35L);
        CloudCostData cloudCostData = createCloudCostDataWithAccountPricingTable(BUSINESS_ACCOUNT_ID, accountPricingData);
        CloudCostCalculator cloudCostCalculator = calculator(cloudCostData);

        // Configure ReservedInstanceApplicator
        final ReservedInstanceApplicator<TestEntityClass> riApplicator =
                mock(ReservedInstanceApplicator.class);
        when(riApplicator.recordRICoverage(eq(computeTier), any(), any())).thenReturn(trax(DEFAULT_RI_COVERAGE));
        when(reservedInstanceApplicatorFactory.newReservedInstanceApplicator(
                any(), eq(infoExtractor), eq(cloudCostData), eq(topologyRiCoverage)))
                .thenReturn(riApplicator);
        // act
        final CostJournal<TestEntityClass> journal1 = cloudCostCalculator.calculateCost(windowsVm4CoresBYOL);

        // The cost of the RI isn't factored in because we mocked out the RI Applicator.
        assertThat(journal1.getTotalHourlyCost().getValue(), is(BASE_PRICE * 1
                + EXPECTED_IP_COST));
        assertThat(journal1.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE).getValue(),
                is(BASE_PRICE * 1));
        // assert no license price
        assertThat(journal1.getHourlyCostForCategory(CostCategory.ON_DEMAND_LICENSE).getValue(), is(0.0));
    }

    /**
     * This is more of an "integration test" to make sure that a VM correctly inherits the
     * storage cost of the journal.
     */
    @Test
    public void testCalculateVMStorageCost() {
        final long vmId = 7;

        // Set up the VM
        final TestEntityClass vm = TestEntityClass.newBuilder(vmId)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build(infoExtractor);
        when(topology.getConnectedRegion(vmId)).thenReturn(Optional.of(region));
        when(topology.getOwner(vmId)).thenReturn(Optional.of(businessAccount));
        when(topology.getStorageTier(vmId)).thenReturn(Optional.of(storageTier));
        when(topology.getComputeTier(vmId)).thenReturn(Optional.of(computeTier));

        // Set up the discount applicator (no discount)
        DiscountApplicator<TestEntityClass> discountApplicator = setupDiscountApplicator(0.0);

        AccountPricingData accountPricingData = new AccountPricingData(setupDiscountApplicator(0.0), PRICE_TABLE_AWS, 15L,20L, 35L);
        CloudCostData cloudCostData = createCloudCostDataWithAccountPricingTable(BUSINESS_ACCOUNT_ID, accountPricingData);

        // Set up the RI applicator (no RI)
        final ReservedInstanceApplicator<TestEntityClass> riApplicator =
                mock(ReservedInstanceApplicator.class);
        when(riApplicator.recordRICoverage(computeTier, price, true)).thenReturn(trax(0.0));
        when(reservedInstanceApplicatorFactory.newReservedInstanceApplicator(
            any(), eq(infoExtractor), eq(cloudCostData), eq(topologyRiCoverage)))
                .thenReturn(riApplicator);

        // Set up a volume, and the cost lookup for the volume.
        final TestEntityClass volume = TestEntityClass.newBuilder(123)
                .setType(EntityType.VIRTUAL_VOLUME_VALUE)
                .build(infoExtractor);
        when(topology.getAttachedVolumes(vmId)).thenReturn(Collections.singletonList(volume));
        // A simple cost journal for the volume.
        final CostJournal<TestEntityClass> volumeJournal =
            CostJournal.newBuilder(volume, infoExtractor, region, discountApplicator, e2 -> null)
                // Just a mock price that's easy to work with.
                .recordOnDemandCost(CostCategory.STORAGE, storageTier, Price.newBuilder()
                        .setUnit(Unit.HOURS)
                        .setPriceAmount(CurrencyAmount.newBuilder()
                                .setAmount(10))
                        .build(), trax(1), Optional.empty())
                .build();
        // The cost lookup will tell the VM's cost journal what the cost journal of the volume
        // looks like.
        final DependentCostLookup<TestEntityClass> volumeCostLookup = e -> volumeJournal;

        final CloudCostCalculator<TestEntityClass> calculator =
                calculatorFactory.newCalculator(cloudCostData, topology, infoExtractor, reservedInstanceApplicatorFactory,
                        volumeCostLookup, topologyRiCoverage);

        final CostJournal<TestEntityClass> vmJournal = calculator.calculateCost(vm);
        // The cost for the VM sh
        assertThat(vmJournal.getTotalHourlyCost().getValue(),
                closeTo(volumeJournal.getTotalHourlyCost().getValue(), DELTA));
        assertThat(vmJournal.getHourlyCostForCategory(CostCategory.STORAGE).getValue(),
                closeTo(volumeJournal.getHourlyCostForCategory(CostCategory.STORAGE).getValue(), DELTA));
    }

    @Test
    public void testCalculateVolumeCostIOPS() {
        final TestEntityClass volume = TestEntityClass.newBuilder(VOLUME_ID)
                .setType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setVolumeConfig(new VirtualVolumeConfig(V_VOLUME_SIZE_IOPS, 0, 0, 0, false, null))
                .build(infoExtractor);
        when(topology.getConnectedRegion(VOLUME_ID)).thenReturn(Optional.of(region));
        when(topology.getOwner(VOLUME_ID)).thenReturn(Optional.of(businessAccount));
        when(topology.getStorageTier(VOLUME_ID)).thenReturn(Optional.of(storageTier));
        AccountPricingData accountPricingData = new AccountPricingData(setupDiscountApplicator(0.0), PRICE_TABLE_AWS, 15L, 20L, 35L);
        CloudCostData cloudCostData = createCloudCostDataWithAccountPricingTable(BUSINESS_ACCOUNT_ID, accountPricingData);
        CloudCostCalculator cloudCostCalculator = calculator(cloudCostData);
        final CostJournal<TestEntityClass> journal = cloudCostCalculator.calculateCost(volume);
        assertThat(journal.getTotalHourlyCost().getValue(),
            closeTo(IOPS_RANGE * IOPS_PRICE_RANGE_1 + (V_VOLUME_SIZE_IOPS - IOPS_RANGE)
                    * IOPS_PRICE, DELTA));
    }

    /**
     * Test that volume cost calculation includes IO throughput pricing.
     */
    @Test
    public void testCalculateVolumeCostMBPS() {
        final TestEntityClass volume = TestEntityClass.newBuilder(VOLUME_ID)
            .setType(EntityType.VIRTUAL_VOLUME_VALUE)
            .setVolumeConfig(new VirtualVolumeConfig(0, 0, V_VOLUME_SIZE_MBPS, 0, false, null))
            .build(infoExtractor);
        when(topology.getConnectedRegion(VOLUME_ID)).thenReturn(Optional.of(region));
        when(topology.getOwner(VOLUME_ID)).thenReturn(Optional.of(businessAccount));
        when(topology.getStorageTier(VOLUME_ID)).thenReturn(Optional.of(storageTier));
        AccountPricingData accountPricingData = new AccountPricingData(setupDiscountApplicator(0.0),
                PRICE_TABLE_AWS, ACCOUNT_PRICING_DATA_OID, 15L, 20L);
        CloudCostData cloudCostData = createCloudCostDataWithAccountPricingTable(BUSINESS_ACCOUNT_ID,
            accountPricingData);
        CloudCostCalculator cloudCostCalculator = calculator(cloudCostData);
        final CostJournal<TestEntityClass> journal = cloudCostCalculator.calculateCost(volume);
        assertThat(journal.getTotalHourlyCost().getValue(),
            closeTo(V_VOLUME_SIZE_MBPS * MBPS_PRICE, DELTA));
    }

    /**
     * Test that volume cost calculation includes IO requests pricing.
     */
    @Test
    public void testCalculateVolumeCostIoRequests() {
        final VirtualVolumeConfig config = new VirtualVolumeConfig(
                0, 0, 0, V_VOLUME_IO_REQUESTS, false, null);
        final TestEntityClass volume = TestEntityClass.newBuilder(VOLUME_ID)
                .setType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setVolumeConfig(config)
                .build(infoExtractor);
        when(topology.getConnectedRegion(VOLUME_ID)).thenReturn(Optional.of(region));
        when(topology.getOwner(VOLUME_ID)).thenReturn(Optional.of(businessAccount));
        when(topology.getStorageTier(VOLUME_ID)).thenReturn(Optional.of(storageTier));
        final AccountPricingData accountPricingData = new AccountPricingData(
                setupDiscountApplicator(0.0), PRICE_TABLE_AWS, ACCOUNT_PRICING_DATA_OID, 15L, 20L);
        final CloudCostData cloudCostData = createCloudCostDataWithAccountPricingTable(
                BUSINESS_ACCOUNT_ID, accountPricingData);
        final CloudCostCalculator<TestEntityClass> cloudCostCalculator = calculator(cloudCostData);
        final CostJournal<TestEntityClass> journal = cloudCostCalculator.calculateCost(volume);
        assertThat(journal.getTotalHourlyCost().getValue(),
                closeTo(V_VOLUME_IO_REQUESTS * IO_REQUESTS_PRICE, DELTA));
    }

    /**
     * Test both Unit.GB_MONTH and Unit.MONTH components of volume cost.
     */
    @Test
    public void testCalculateVolumeCostGBMonth() {
        final int vVolSizeMb = 19; // should be >= GB_RANGE
        final CostJournal<TestEntityClass> journal =
                createCostJournalForVolumeCostCalculation(vVolSizeMb, false, null, STORAGE_TIER_ID);

        assertThat(journal.getTotalHourlyCost().getValue(),
            closeTo(GB_RANGE * GB_PRICE_RANGE_1 + (vVolSizeMb - GB_RANGE) * GB_PRICE
                + GB_MONTH_PRICE_20, DELTA));
    }

    /**
     * Test that cost of volume with GRS redundancy type.
     */
    @Test
    public void testCalculateVolumeCostWithGRSRedundancyType() {
        final int vVolSizeMb = 19;
        final CostJournal<TestEntityClass> journal =
            createCostJournalForVolumeCostCalculation(vVolSizeMb, false, RedundancyType.GRS,
                STORAGE_TIER_ID_2);

        assertThat(journal.getTotalHourlyCost().getValue(),
            closeTo(vVolSizeMb * GB_PRICE_GRS, DELTA));
    }

    /**
     * Test that cost of volume with RAGRS redundancy type.
     */
    @Test
    public void testCalculateVolumeCostWithRAGRSRedundancyType() {
        final int vVolSizeMb = 19;
        final CostJournal<TestEntityClass> journal =
            createCostJournalForVolumeCostCalculation(vVolSizeMb, false, RedundancyType.RAGRS,
                STORAGE_TIER_ID_2);

        assertThat(journal.getTotalHourlyCost().getValue(),
            closeTo(vVolSizeMb * GB_PRICE_RAGRS, DELTA));
    }

    /**
     * Test the case when Unit.GB_MONTH has a size that equals to the end range.
     */
    @Test
    public void testCalculateVolumeCostSizeEqualToEndRange() {
        final int vVolSizeMb = 32;
        final CostJournal<TestEntityClass> journal =
                createCostJournalForVolumeCostCalculation(vVolSizeMb, false, null, STORAGE_TIER_ID);
        assertThat(journal.getTotalHourlyCost().getValue(),
                closeTo(GB_RANGE * GB_PRICE_RANGE_1 + (vVolSizeMb - GB_RANGE) * GB_PRICE
                        + GB_MONTH_PRICE_32, DELTA));
    }

    /**
     * Creates cloudCost data for a given virtual volume size.
     *
     * @param vVolSizeMb virtual volume size.
     * @param isEphemeral true if volume is ephemeral.
     * @param redundancyType of the volume, null if not applicable.
     * @param storageTierId of the tier to which the volume belongs.
     * @return {@link CostJournal} for volume of a given virtual volume size.
     */
    private CostJournal<TestEntityClass> createCostJournalForVolumeCostCalculation(
            final int vVolSizeMb,
            final boolean isEphemeral,
            final RedundancyType redundancyType,
            final long storageTierId) {
        final TestEntityClass volume = TestEntityClass.newBuilder(VOLUME_ID)
                .setType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setVolumeConfig(new VirtualVolumeConfig(0, vVolSizeMb * 1024, 0, 0, isEphemeral,
                    redundancyType))
                .build(infoExtractor);
        when(topology.getConnectedRegion(VOLUME_ID)).thenReturn(Optional.of(region));
        when(topology.getOwner(VOLUME_ID)).thenReturn(Optional.of(businessAccount));
        final TestEntityClass storageTier = createStorageTier(storageTierId);
        when(topology.getStorageTier(VOLUME_ID))
            .thenReturn(Optional.of(storageTier));
        AccountPricingData accountPricingData = new AccountPricingData(setupDiscountApplicator(0.0),
                PRICE_TABLE_AWS, ACCOUNT_PRICING_DATA_OID, 15L, 20L);
        CloudCostData cloudCostData = createCloudCostDataWithAccountPricingTable(BUSINESS_ACCOUNT_ID, accountPricingData);
        CloudCostCalculator cloudCostCalculator = calculator(cloudCostData);
        return cloudCostCalculator.calculateCost(volume);
    }

    /**
     * Test a on-demand calculation (no discount) for a Database.
     */
    @Test
    public void testCalculateOnDemandCostForDatabase() {
        // arrange
        final long dbId = 9;

        final TestEntityClass db = TestEntityClass.newBuilder(dbId)
                .setType(EntityType.DATABASE_VALUE)
                .setDatabaseConfig(new EntityInfoExtractor.DatabaseConfig(
                    DatabaseEdition.ENTERPRISE,
                    DatabaseEngine.MYSQL, LicenseModel.LICENSE_INCLUDED, DeploymentType.SINGLE_AZ))
                .build(infoExtractor);

        when(topology.getConnectedRegion(dbId)).thenReturn(Optional.of(region));
        when(topology.getConnectedAvailabilityZone(dbId)).thenReturn(Optional.of(availabilityZone));
        when(topology.getOwner(dbId)).thenReturn(Optional.of(businessAccount));
        when(topology.getDatabaseTier(dbId)).thenReturn(Optional.of(databaseTier));
        when(infoExtractor.getRDBCommodityCapacity(any(), eq(CommodityType.STORAGE_AMOUNT))).thenReturn(Optional.of((float)STORAGE_RANGE));
        final DiscountApplicator<TestEntityClass> discountApplicator = setupDiscountApplicator(0.0);
        AccountPricingData accountPricingData = new AccountPricingData(discountApplicator, PRICE_TABLE_AWS,
            ACCOUNT_PRICING_DATA_OID, 15L, 20L);
        CloudCostData cloudCostData = createCloudCostDataWithAccountPricingTable(BUSINESS_ACCOUNT_ID, accountPricingData);
        CloudCostCalculator cloudCostCalculator = calculator(cloudCostData);

        // act
        final CostJournal<TestEntityClass> journal = cloudCostCalculator.calculateCost(db);

        // assert
        assertThat(journal.getTotalHourlyCost().getValue(),
                is(BASE_PRICE + MYSQL_ADJUSTMENT +
                        CostProtoUtil.getHourlyPriceAmount(price(Unit.GB_MONTH,
                                GB_RANGE * STORAGE_PRICE))));
        assertThat(journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE).getValue(),
                is(BASE_PRICE ));
        assertThat(journal.getHourlyCostForCategory(CostCategory.STORAGE).getValue(),
                is(CostProtoUtil.getHourlyPriceAmount(price(Unit.GB_MONTH,
                        GB_RANGE * STORAGE_PRICE))));
        assertThat(journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_LICENSE).getValue(),
                is(MYSQL_ADJUSTMENT));

        // Once for the compute, once for the license, because both costs are "paid to" the
        // database tier.
        verify(discountApplicator, times(3)).getDiscountPercentage(databaseTier);
    }

    /**
     * Test a on-demand calculation (no discount) for a Database.
     */
    @Test
    public void testCalculateOnDemandCostForDatabaseWithTwoStorageSlabs() {
        // arrange
        final long dbId = 9;
        final long extraStorageInGB = 10;
        final TestEntityClass db = TestEntityClass.newBuilder(dbId)
                .setType(EntityType.DATABASE_VALUE)
                .setDatabaseConfig(new EntityInfoExtractor.DatabaseConfig(
                        DatabaseEdition.ENTERPRISE,
                        DatabaseEngine.MYSQL, LicenseModel.LICENSE_INCLUDED, DeploymentType.SINGLE_AZ))
                .build(infoExtractor);

        when(topology.getConnectedRegion(dbId)).thenReturn(Optional.of(region));
        when(topology.getConnectedAvailabilityZone(dbId)).thenReturn(Optional.of(availabilityZone));
        when(topology.getOwner(dbId)).thenReturn(Optional.of(businessAccount));
        when(topology.getDatabaseTier(dbId)).thenReturn(Optional.of(databaseTier));
        // 21 GB.
        when(infoExtractor.getRDBCommodityCapacity(any(), eq(CommodityType.STORAGE_AMOUNT)))
                .thenReturn(Optional.of((float)STORAGE_RANGE + (extraStorageInGB * Units.KBYTE)));
        final DiscountApplicator<TestEntityClass> discountApplicator = setupDiscountApplicator(0.0);
        AccountPricingData accountPricingData = new AccountPricingData(discountApplicator, PRICE_TABLE_AWS,
                ACCOUNT_PRICING_DATA_OID, 15L, 20L);
        CloudCostData cloudCostData = createCloudCostDataWithAccountPricingTable(BUSINESS_ACCOUNT_ID, accountPricingData);
        CloudCostCalculator cloudCostCalculator = calculator(cloudCostData);

        // act
        final CostJournal<TestEntityClass> journal = cloudCostCalculator.calculateCost(db);

        // assert
        assertThat(journal.getTotalHourlyCost().getValue(),
                is(BASE_PRICE + MYSQL_ADJUSTMENT +
                        CostProtoUtil.getHourlyPriceAmount(price(Unit.GB_MONTH,
                                (GB_RANGE + extraStorageInGB ) * STORAGE_PRICE))));
        assertThat(journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE).getValue(),
                is(BASE_PRICE));
        assertThat(journal.getHourlyCostForCategory(CostCategory.STORAGE).getValue(),
                is(CostProtoUtil.getHourlyPriceAmount(price(Unit.GB_MONTH,
                        (GB_RANGE + extraStorageInGB ) * STORAGE_PRICE))));
        assertThat(journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_LICENSE).getValue(),
                is(MYSQL_ADJUSTMENT));

        // Once for the compute, once for the license, because both costs are "paid to" the
        // database tier.
        verify(discountApplicator, times(3)).getDiscountPercentage(databaseTier);
    }

    /**
     * Test a on-demand calculation (no discount) for a Database Server.
     */
    @Test
    public void testCalculateOnDemandCostForDatabaseServer() {
        // arrange
        final long dbId = 9;
        final float storageAmount = 1024f;
        final float iopsAmount = 100f;

        final TestEntityClass db = TestEntityClass.newBuilder(dbId)
                .setType(EntityType.DATABASE_SERVER_VALUE)
                .setDatabaseConfig(new EntityInfoExtractor.DatabaseConfig(
                        DatabaseEdition.ENTERPRISE,
                        DatabaseEngine.MYSQL, LicenseModel.LICENSE_INCLUDED, DeploymentType.SINGLE_AZ))
                .build(infoExtractor);
        mockDBSEntityCall(dbId, storageAmount, iopsAmount);

        final DiscountApplicator<TestEntityClass> discountApplicator = setupDiscountApplicator(0.0);
        AccountPricingData accountPricingData = new AccountPricingData(discountApplicator, PRICE_TABLE_AWS,
                ACCOUNT_PRICING_DATA_OID,15L, 20L);
        CloudCostData cloudCostData = createCloudCostDataWithAccountPricingTable(BUSINESS_ACCOUNT_ID, accountPricingData);
        CloudCostCalculator cloudCostCalculator = calculator(cloudCostData);

        // act
        final CostJournal<TestEntityClass> journal = cloudCostCalculator.calculateCost(db);

        // assert
        assertThat(journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE).getValue(), is(BASE_PRICE + MYSQL_ADJUSTMENT));
        assertThat(journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_LICENSE).getValue(), is(0.0));
        final double expectedStorageCost =
                (Math.max(storageAmount / 1024, MIN_RDS_STORAGE) * STORAGE_PRICE + Math.max(iopsAmount, MIN_RDS_IOPS) * IOPS_PRICE)
                        / CostProtoUtil.HOURS_IN_MONTH;
        assertThat(journal.getHourlyCostForCategory(CostCategory.STORAGE).getValue(), is(expectedStorageCost));

        // Once for the compute, once for storage,  no license cost
        verify(discountApplicator, times(3)).getDiscountPercentage(databaseServerTier);
    }

    /**
     * Test a on-demand calculation (no discount) for a Database Server.
     */
    @Test
    public void testCalculateOnDemandCostForDatabaseServerWithNoIncrement() {
        // arrange
        final long dbId = 10;
        final float storageAmount = 2f;
        final float iopsAmount = 1000f;
        final TestEntityClass db = TestEntityClass.newBuilder(dbId)
                .setType(EntityType.DATABASE_SERVER_VALUE)
                .setDatabaseConfig(new EntityInfoExtractor.DatabaseConfig(
                        DatabaseEdition.NONE,
                        DatabaseEngine.POSTGRESQL, LicenseModel.NO_LICENSE_REQUIRED, DeploymentType.SINGLE_AZ))
                .build(infoExtractor);
        mockDBSEntityCall(dbId, storageAmount, iopsAmount);

        final DiscountApplicator<TestEntityClass> discountApplicator = setupDiscountApplicator(0.0);
        AccountPricingData accountPricingData = new AccountPricingData(discountApplicator, PRICE_TABLE_AWS,
                ACCOUNT_PRICING_DATA_OID,15L, 20L);
        CloudCostData cloudCostData = createCloudCostDataWithAccountPricingTable(BUSINESS_ACCOUNT_ID, accountPricingData);
        CloudCostCalculator cloudCostCalculator = calculator(cloudCostData);
        // act
        final CostJournal<TestEntityClass> journal = cloudCostCalculator.calculateCost(db);
        final double expectedStorageCost =
                (storageAmount / 1024 * STORAGE_PRICE + Math.max(iopsAmount, MIN_RDS_IOPS) * IOPS_PRICE)
                        / CostProtoUtil.HOURS_IN_MONTH;
        assertThat(journal.getHourlyCostForCategory(CostCategory.STORAGE).getValue(), is(expectedStorageCost));
        // Once for the compute, once for storage,  no license cost
        verify(discountApplicator, times(3)).getDiscountPercentage(databaseServerTier);
    }

    /**
     * Test cost calculation for idle VM.
     */
    @Test
    public void testCalculateCostForIdleVm() {
        // Arrange
        final long vmId = 10;
        final TestEntityClass vm = createVmTestEntity(vmId, EntityState.POWERED_OFF, OSType.WINDOWS_WITH_SQL_ENTERPRISE,
                Tenancy.DEFAULT, VMBillingType.ONDEMAND, 1,
                EntityDTO.LicenseModel.LICENSE_INCLUDED, null);

        when(topology.getConnectedRegion(vmId)).thenReturn(Optional.of(region));
        when(topology.getConnectedAvailabilityZone(vmId)).thenReturn(Optional.of(availabilityZone));
        when(topology.getOwner(vmId)).thenReturn(Optional.of(businessAccount));
        when(topology.getComputeTier(vmId)).thenReturn(Optional.of(computeTier));

        // Set up CloudCostData
        final DiscountApplicator<TestEntityClass> discountApplicator = setupDiscountApplicator(0.0);
        final AccountPricingData accountPricingData = new AccountPricingData(discountApplicator,
                PRICE_TABLE_AWS, ACCOUNT_PRICING_DATA_OID, 15L, 20L);
        final CloudCostData cloudCostData = createCloudCostDataWithAccountPricingTable(
                BUSINESS_ACCOUNT_ID, accountPricingData);

        // Set up the RI applicator (no RI)
        final ReservedInstanceApplicator<TestEntityClass> riApplicator =
                mock(ReservedInstanceApplicator.class);
        when(riApplicator.recordRICoverage(eq(computeTier), any(), any())).thenReturn(trax(0.0));
        when(reservedInstanceApplicatorFactory.newReservedInstanceApplicator(
                any(), eq(infoExtractor), eq(cloudCostData), eq(topologyRiCoverage)))
                .thenReturn(riApplicator);

        final CloudCostCalculator cloudCostCalculator = calculator(cloudCostData);

        // Act
        final CostJournal<TestEntityClass> journal = cloudCostCalculator.calculateCost(vm);

        // Assert
        assertThat(journal.getTotalHourlyCost().getValue(), closeTo(EXPECTED_IDLE_IP_COST, DELTA));

        assertThat(journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE).getValue(),
                closeTo(0D, DELTA));

        // Make sure the on-demand rates are recorded as zero for both compute and license
        final Map<CostSource, TraxNumber> computeCostsBySource =
                journal.getFilteredCategoryCostsBySource(CostCategory.ON_DEMAND_COMPUTE, CostSourceFilter.INCLUDE_ALL);
        assertThat(computeCostsBySource, hasKey(CostSource.ON_DEMAND_RATE));
        assertThat(computeCostsBySource.get(CostSource.ON_DEMAND_RATE).getValue(), closeTo(0.0, .00001));

        assertThat(journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_LICENSE).getValue(),
                closeTo(0D, DELTA));

        final Map<CostSource, TraxNumber> licenseCostBySource =
                journal.getFilteredCategoryCostsBySource(CostCategory.ON_DEMAND_LICENSE, CostSourceFilter.INCLUDE_ALL);
        assertThat(licenseCostBySource, hasKey(CostSource.ON_DEMAND_RATE));
        assertThat(licenseCostBySource.get(CostSource.ON_DEMAND_RATE).getValue(), closeTo(0.0, .00001));

        assertThat(journal.getHourlyCostForCategory(CostCategory.IP).getValue(),
                closeTo(EXPECTED_IDLE_IP_COST, DELTA));
    }

    /**
     * Test cost calculation for idle DB.
     */
    @Test
    public void testCalculateCostForIdleDb() {
        // Arrange
        final long dbId = 9;
        final TestEntityClass db = TestEntityClass.newBuilder(dbId)
                .setType(EntityType.DATABASE_SERVER_VALUE)
                .setEntityState(EntityState.POWERED_OFF)
                .setDatabaseConfig(new EntityInfoExtractor.DatabaseConfig(
                        DatabaseEdition.ENTERPRISE,
                        DatabaseEngine.MYSQL, LicenseModel.LICENSE_INCLUDED, DeploymentType.SINGLE_AZ))
                .build(infoExtractor);

        when(infoExtractor.getRDBCommodityCapacity(any(), any())).thenReturn(Optional.empty());

        when(topology.getConnectedRegion(dbId)).thenReturn(Optional.of(region));
        when(topology.getConnectedAvailabilityZone(dbId)).thenReturn(Optional.of(availabilityZone));
        when(topology.getOwner(dbId)).thenReturn(Optional.of(businessAccount));
        when(topology.getDatabaseServerTier(dbId)).thenReturn(Optional.of(databaseServerTier));

        final DiscountApplicator<TestEntityClass> discountApplicator = setupDiscountApplicator(0.0);
        final AccountPricingData accountPricingData = new AccountPricingData(discountApplicator,
                PRICE_TABLE_AWS, ACCOUNT_PRICING_DATA_OID, 15L, 20L);
        final CloudCostData cloudCostData = createCloudCostDataWithAccountPricingTable(
                BUSINESS_ACCOUNT_ID, accountPricingData);
        final CloudCostCalculator cloudCostCalculator = calculator(cloudCostData);

        // Act
        final CostJournal<TestEntityClass> journal = cloudCostCalculator.calculateCost(db);

        // Assert
        assertThat(journal.getTotalHourlyCost().getValue(), is(0D));
        assertThat(journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE).getValue(), is(0D));

        // Make sure the on-demand rates are recorded as zero for compute. DB server does not support
        // a license cost atm.
        final Map<CostSource, TraxNumber> computeCostsBySource =
                journal.getFilteredCategoryCostsBySource(CostCategory.ON_DEMAND_COMPUTE, CostSourceFilter.INCLUDE_ALL);
        assertThat(computeCostsBySource, hasKey(CostSource.ON_DEMAND_RATE));
        assertThat(computeCostsBySource.get(CostSource.ON_DEMAND_RATE).getValue(), closeTo(0.0, .00001));

        assertThat(journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_LICENSE).getValue(), is(0D));
    }

    /**
     * Verify that an entity of a type that doesn't have a cost (e.g. NETWORK) returns
     * an empty journal.
     */
    @Test
    public void testEmptyCost() {
        final TestEntityClass noCostEntity = TestEntityClass.newBuilder(7)
            .setType(EntityType.NETWORK_VALUE)
            .build(infoExtractor);
        AccountPricingData accountPricingData = new AccountPricingData(setupDiscountApplicator(0.0),
                PRICE_TABLE_AWS, ACCOUNT_PRICING_DATA_OID, 15L, 20L);
        CloudCostData cloudCostData = createCloudCostDataWithAccountPricingTable(BUSINESS_ACCOUNT_ID, accountPricingData);
        CloudCostCalculator cloudCostCalculator = calculator(cloudCostData);
        final CostJournal<TestEntityClass> journal = cloudCostCalculator.calculateCost(noCostEntity);
        assertThat(journal.getEntity(), is(noCostEntity));
        assertThat(journal.getTotalHourlyCost().getValue(), is(0.0));
        assertThat(journal.getCategories(), is(Collections.emptySet()));
    }

    /**
     * Verify that an Ephemeral Volume returns an empty journal.
     */
    @Test
    public void testEphemeralVolumeEmptyCost() {
        final int vVolSizeMb = 19;
        final CostJournal<TestEntityClass> journal =
                createCostJournalForVolumeCostCalculation(vVolSizeMb, true, null, STORAGE_TIER_ID);
        assertThat(journal.getTotalHourlyCost().getValue(), is(0.0));
        assertThat(journal.getCategories(), is(Collections.emptySet()));
    }

    /**
     * Verify that when consumer commodity prices are set, do not use the base price.
     */
    @Test
    public void testOnDemandVmCommodityCost() {
        DiscountApplicator<TestEntityClass> discountApplicator = setupDiscountApplicator(0.0);

        // create a fake commodities bought map
        final Map<com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType, Double> pricedCommoditiesBought = new HashMap<>();
        // memory amount => 2
        com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType commodityType = com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.MEM_PROVISIONED_VALUE).build();
        pricedCommoditiesBought.put(commodityType, 2.0);
        // vcore amount => 1
        commodityType = com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType.newBuilder()
                .setType(CommodityType.NUM_VCORE_VALUE).build();
        pricedCommoditiesBought.put(commodityType, 1.0);

        TestEntityClass wsqlVm4Cores = createVmTestEntity(DEFAULT_VM_ID, OSType.WINDOWS_WITH_SQL_ENTERPRISE,
                Tenancy.DEFAULT, VMBillingType.ONDEMAND, 4,
                EntityDTO.LicenseModel.LICENSE_INCLUDED, pricedCommoditiesBought);

        // act
        AccountPricingData accountPricingData = new AccountPricingData(discountApplicator, PRICE_TABLE_GCP, 15L, 20L, 30L);

        final ComputeTierConfig computeTierConfig = ComputeTierConfig.builder()
                .computeTierOid(computeTier.getId())
                .numCores(4)
                .numCoupons(0)
                .isBurstableCPU(false)
                .build();
        when(infoExtractor.getComputeTierConfig(computeTier)).thenReturn(Optional.of(computeTierConfig));
        CloudCostData cloudCostData = createCloudCostDataWithAccountPricingTable(BUSINESS_ACCOUNT_ID, accountPricingData);

        CloudCostCalculator cloudCostCalculator = calculator(cloudCostData);
        // Configure ReservedInstanceApplicator
        final ReservedInstanceApplicator<TestEntityClass> riApplicator = mock(ReservedInstanceApplicator.class);
        when(riApplicator.recordRICoverage(eq(computeTier), any(), any())).thenReturn(trax(DEFAULT_RI_COVERAGE));
        when(reservedInstanceApplicatorFactory.newReservedInstanceApplicator(
                any(), eq(infoExtractor), eq(cloudCostData), eq(topologyRiCoverage)))
                .thenReturn(riApplicator);

        // act
        final CostJournal<TestEntityClass> journal = cloudCostCalculator.calculateCost(wsqlVm4Cores);

        // assert
        assertThat(journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE).getValue(),
                is(MEM_PROVISIONED_PRICE * 2 + NUM_VCORE_PRICE * 1));
    }

    private static Price price(Price.Unit unit, double amount) {
        return Price.newBuilder()
                        .setUnit(unit)
                        .setPriceAmount(CurrencyAmount.newBuilder()
                            .setAmount(amount)
                            .build())
                        .build();
    }

    private static Price price(Price.Unit unit, int endRange, double amount) {
        return Price.newBuilder()
                        .setUnit(unit)
                        .setEndRangeInUnits(endRange)
                        .setPriceAmount(CurrencyAmount.newBuilder()
                            .setAmount(amount)
                            .build())
                        .build();
    }

    private static Price price(Price.Unit unit, int endRange, int incrementInterval, double amount) {
        return Price.newBuilder()
                .setUnit(unit)
                .setEndRangeInUnits(endRange)
                .setIncrementInterval(incrementInterval)
                .setPriceAmount(CurrencyAmount.newBuilder()
                        .setAmount(amount)
                        .build())
                .build();
    }

    private static LicensePrice licensePrice(int numCores, Price price) {
        return LicensePrice.newBuilder()
                        .setNumberOfCores(numCores)
                        .setPrice(price)
                        .build();
    }

    private static PriceTable thePriceTable(Provider provider) {
        CurrencyAmount storagePrice = CurrencyAmount.newBuilder().setAmount(STORAGE_PRICE).build();
        CurrencyAmount iopsPrice = CurrencyAmount.newBuilder().setAmount(IOPS_PRICE).build();
        DatabaseServerTierPriceList dbsPriceList = DatabaseServerTierPriceList.newBuilder()
                .addConfigPrices(DatabaseServerTierConfigPrice.newBuilder()
                        .setDatabaseTierConfigPrice(DatabaseTierConfigPrice.newBuilder()
                                .setDbEdition(DatabaseEdition.NONE)
                                .setDbEngine(DatabaseEngine.MARIADB)
                                .setDbDeploymentType(DeploymentType.SINGLE_AZ)
                                .setDbLicenseModel(LicenseModel.NO_LICENSE_REQUIRED)
                                .addPrices(price(Unit.HOURS, BASE_PRICE))
                                .build())
                        .build())
                .addConfigPrices(DatabaseServerTierConfigPrice.newBuilder()
                        .setDatabaseTierConfigPrice(DatabaseTierConfigPrice.newBuilder()
                                .setDbEdition(DatabaseEdition.NONE)
                                .setDbEngine(DatabaseEngine.POSTGRESQL)
                                .setDbDeploymentType(DeploymentType.SINGLE_AZ)
                                .setDbLicenseModel(LicenseModel.NO_LICENSE_REQUIRED)
                                .addPrices(price(Unit.HOURS, BASE_PRICE))
                                .build())
                        .addDependentPrices(Price.newBuilder().setUnit(Unit.GB_MONTH).setEndRangeInUnits(MIN_RDS_STORAGE).setPriceAmount(storagePrice))
                        .addDependentPrices(Price.newBuilder().setUnit(Unit.MILLION_IOPS).setEndRangeInUnits(MIN_RDS_IOPS).setIncrementInterval(MIN_RDS_IOPS).setPriceAmount(iopsPrice))
                        .addDependentPrices(Price.newBuilder().setUnit(Unit.MILLION_IOPS).setEndRangeInUnits(MAX_RDS_IOPS).setIncrementInterval(1).setPriceAmount(iopsPrice))
                        .build())
                .addConfigPrices(DatabaseServerTierConfigPrice.newBuilder()
                        .setDatabaseTierConfigPrice(DatabaseTierConfigPrice.newBuilder()
                                .setDbEdition(DatabaseEdition.ENTERPRISE)
                                .setDbEngine(DatabaseEngine.MYSQL)
                                .setDbDeploymentType(DeploymentType.SINGLE_AZ)
                                .setDbLicenseModel(LicenseModel.LICENSE_INCLUDED)
                                .addPrices(price(Unit.HOURS, BASE_PRICE + MYSQL_ADJUSTMENT))
                                .build())
                        .addDependentPrices(Price.newBuilder().setUnit(Unit.GB_MONTH).setEndRangeInUnits(MIN_RDS_STORAGE).setIncrementInterval(MIN_RDS_STORAGE).setPriceAmount(storagePrice))
                        .addDependentPrices(Price.newBuilder().setUnit(Unit.GB_MONTH).setEndRangeInUnits(MAX_RDS_STORAGE).setIncrementInterval(1).setPriceAmount(storagePrice))
                        .addDependentPrices(Price.newBuilder().setUnit(Unit.MILLION_IOPS).setEndRangeInUnits(MIN_RDS_IOPS).setIncrementInterval(MIN_RDS_IOPS).setPriceAmount(iopsPrice))
                        .addDependentPrices(Price.newBuilder().setUnit(Unit.MILLION_IOPS).setEndRangeInUnits(MAX_RDS_IOPS).setIncrementInterval(1).setPriceAmount(iopsPrice))
                        .build()).build();

        ComputeTierPriceList computeTierPriceList;
        switch (provider) {
            case GCP:
                computeTierPriceList = ComputeTierPriceList.newBuilder()
                        .setBasePrice(ComputeTierConfigPrice.newBuilder()
                                .setGuestOsType(OSType.LINUX)
                                .setTenancy(Tenancy.DEFAULT)
                                .addConsumerCommodityPrices(PricingDTO.ConsumerCommodityPrice.newBuilder()
                                        .setCommodityType(CommodityType.MEM_PROVISIONED)
                                        .setPrice(price(Unit.HOURS, MEM_PROVISIONED_PRICE)).build())
                                .addConsumerCommodityPrices(PricingDTO.ConsumerCommodityPrice.newBuilder()
                                        .setCommodityType(CommodityType.NUM_VCORE)
                                        .setPrice(price(Unit.HOURS, NUM_VCORE_PRICE)).build())
                                .addPrices(price(Unit.HOURS, BASE_PRICE)))
                        .addPerConfigurationPriceAdjustments(ComputeTierConfigPrice.newBuilder()
                                .setGuestOsType(OSType.SUSE)
                                .setTenancy(Tenancy.DEFAULT)
                                .addPrices(price(Unit.HOURS, SUSE_ADJUSTMENT)))
                        .addPerConfigurationPriceAdjustments(ComputeTierConfigPrice.newBuilder()
                                .setGuestOsType(OSType.WINDOWS_WITH_SQL_ENTERPRISE)
                                .setTenancy(Tenancy.DEFAULT)
                                .addPrices(price(Unit.HOURS, WSQL_ADJUSTMENT)))
                        .build();
                break;
            default:
                computeTierPriceList = ComputeTierPriceList.newBuilder()
                        .setBasePrice(ComputeTierConfigPrice.newBuilder()
                                .setGuestOsType(OSType.LINUX)
                                .setTenancy(Tenancy.DEFAULT)
                                .addPrices(price(Unit.HOURS, BASE_PRICE)))
                        .addPerConfigurationPriceAdjustments(ComputeTierConfigPrice.newBuilder()
                                .setGuestOsType(OSType.SUSE)
                                .setTenancy(Tenancy.DEFAULT)
                                .addPrices(price(Unit.HOURS, SUSE_ADJUSTMENT)))
                        .addPerConfigurationPriceAdjustments(ComputeTierConfigPrice.newBuilder()
                                .setGuestOsType(OSType.WINDOWS_WITH_SQL_ENTERPRISE)
                                .setTenancy(Tenancy.DEFAULT)
                                .addPrices(price(Unit.HOURS, WSQL_ADJUSTMENT)))
                        .build();
                break;
        }

        return PriceTable.newBuilder()
            .putOnDemandPriceByRegionId(REGION_ID, OnDemandPriceTable.newBuilder()
                .putCloudStoragePricesByTierId(STORAGE_TIER_ID, StorageTierPriceList.newBuilder()
                    .addCloudStoragePrice(StorageTierPrice.newBuilder()
                        .addPrices(price(Unit.MILLION_IOPS, IOPS_RANGE, IOPS_PRICE_RANGE_1
                            * CostProtoUtil.HOURS_IN_MONTH))
                        .addPrices(price(Unit.MILLION_IOPS, IOPS_PRICE * CostProtoUtil.HOURS_IN_MONTH))
                        .addPrices(price(Unit.MBPS_MONTH, MBPS_PRICE * CostProtoUtil.HOURS_IN_MONTH))
                        .addPrices(price(Unit.IO_REQUESTS, IO_REQUESTS_PRICE)))
                    .addCloudStoragePrice(StorageTierPrice.newBuilder()
                        .addPrices(price(Unit.GB_MONTH, GB_RANGE, GB_PRICE_RANGE_1
                            * CostProtoUtil.HOURS_IN_MONTH))
                        .addPrices(price(Unit.GB_MONTH, GB_PRICE * CostProtoUtil.HOURS_IN_MONTH)))
                    .addCloudStoragePrice(StorageTierPrice.newBuilder()
                        // 10GB disk - $10/hr
                        .addPrices(price(Unit.MONTH, 10, GB_MONTH_PRICE_10
                            * CostProtoUtil.HOURS_IN_MONTH))
                        // 20GB disk - $16/hr
                        .addPrices(price(Unit.MONTH, 20, GB_MONTH_PRICE_20
                            * CostProtoUtil.HOURS_IN_MONTH))
                        // 32GB disk - $16/hr
                        .addPrices(price(Unit.MONTH, 32, GB_MONTH_PRICE_32
                            * CostProtoUtil.HOURS_IN_MONTH))
                        .build())
                    .build())
                .putCloudStoragePricesByTierId(STORAGE_TIER_ID_2, StorageTierPriceList.newBuilder()
                    .addCloudStoragePrice(StorageTierPrice.newBuilder()
                        .addPrices(price(Unit.GB_MONTH, GB_PRICE_RAGRS
                            * CostProtoUtil.HOURS_IN_MONTH))
                        .setRedundancyType(RedundancyType.RAGRS)
                        .build())
                    .addCloudStoragePrice(StorageTierPrice.newBuilder()
                        .addPrices(price(Unit.GB_MONTH, GB_PRICE_GRS
                            * CostProtoUtil.HOURS_IN_MONTH))
                        .setRedundancyType(RedundancyType.GRS)
                        .build())
                    .build())
                .putDbPricesByInstanceId(DB_TIER_ID,
                    DbTierOnDemandPriceTable.newBuilder()
                        .putDbPricesByDeploymentType(DeploymentType.SINGLE_AZ.getNumber(),
                            DatabaseTierPriceList.newBuilder()
                                .setBasePrice(DatabaseTierConfigPrice.newBuilder()
                                    .setDbEdition(DatabaseEdition.NONE)
                                    .setDbEngine(DatabaseEngine.MARIADB)
                                    .setDbDeploymentType(DeploymentType.SINGLE_AZ)
                                    .setDbLicenseModel(LicenseModel.NO_LICENSE_REQUIRED)
                                    .addPrices(price(Unit.HOURS, BASE_PRICE)))
                                .addConfigurationPriceAdjustments(DatabaseTierConfigPrice.newBuilder()
                                    .setDbEdition(DatabaseEdition.ENTERPRISE)
                                    .setDbEngine(DatabaseEngine.MYSQL)
                                    .setDbDeploymentType(DeploymentType.SINGLE_AZ)
                                    .setDbLicenseModel(LicenseModel.LICENSE_INCLUDED)
                                    .addPrices(price(Unit.HOURS, MYSQL_ADJUSTMENT)))
                                .addDependentPrices(price(Unit.GB_MONTH, GB_RANGE,
                                        INCREMENT_INTERVAL_GB, STORAGE_PRICE))
                                // second increment.
                                .addDependentPrices(price(Unit.GB_MONTH, GB_RANGE_SLOT_2,
                                        INCREMENT_INTERVAL_GB_SLOT_2,
                                        STORAGE_PRICE))
                                .build())
                        .build())
                .putDbsPricesByInstanceId(DB_SERVER_TIER_ID, DbServerTierOnDemandPriceTable.newBuilder()
                    .putDbsPricesByTierId("", dbsPriceList).build())
                .putComputePricesByTierId(COMPUTE_TIER_ID, computeTierPriceList)
                .setIpPrices(IpPriceList.newBuilder().addIpPrice(IpConfigPrice.newBuilder()
                    .setFreeIpCount(IP_COUNT)
                    .addPrices(price(Unit.HOURS, IP_RANGE, IP_PRICE_RANGE_1))
                    .addPrices(price(Unit.HOURS, IP_PRICE))
                    .build())
                    .build())
                .build())
            .putSpotPriceByZoneOrRegionId(AVAILABILITY_ZONE_ID, SpotInstancePriceTable.newBuilder()
                    .putSpotPricesByTierOid(COMPUTE_TIER_ID, SpotPricesForTier.newBuilder()
                        .addPriceForGuestOsType(PriceForGuestOsType.newBuilder()
                                .setGuestOsType(OSType.SUSE)
                                .setPrice(price(Unit.HOURS, BASE_PRICE)))
                        .build())
                    .build())
            .addOnDemandLicensePrices(LicensePriceEntry.newBuilder()
                .setOsType(OSType.WINDOWS_WITH_SQL_ENTERPRISE)
                .addLicensePrices(licensePrice(1, price(Unit.HOURS, WSQL_ENTERPRISE_1)))
                .addLicensePrices(licensePrice(2, price(Unit.HOURS, WSQL_ENTERPRISE_2)))
                .addLicensePrices(licensePrice(8, price(Unit.HOURS, WSQL_ENTERPRISE_8)))
                .build())
            .build();
    }

    private static CloudCostCalculator<TestEntityClass> calculator(CloudCostData cloudCostData,
                                                                   DependentCostLookup dependentCostLookup) {
        return calculatorFactory.newCalculator(cloudCostData, topology, infoExtractor, reservedInstanceApplicatorFactory,
                dependentCostLookup, topologyRiCoverage);
    }

    private static CloudCostCalculator<TestEntityClass> calculator(CloudCostData cloudCostData) {
        return calculatorFactory.newCalculator(cloudCostData, topology, infoExtractor, reservedInstanceApplicatorFactory,
                 e -> null, topologyRiCoverage);
    }

    /**
     * Populate and return the cloud cost data object with given business account and accountPricing Data mapping.
     *
     * @param baOid The business account oid.
     * @param accountPricingData The account pricing data.
     *
     * @return The cloud cost data object.
     */
    private CloudCostData createCloudCostDataWithAccountPricingTable(Long baOid,
                                                                     AccountPricingData accountPricingData) {
        Map<Long, AccountPricingData> accountPricingDataByBusinessAccount = new HashMap<>();
        accountPricingDataByBusinessAccount.put(baOid, accountPricingData);
        return new CloudCostData(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
                Collections.emptyMap(), accountPricingDataByBusinessAccount, new HashMap<>(), Optional.empty());
    }

    private void mockDBSEntityCall(final long dbId, final float storageAmount, final float iopsAmount) {
        when(infoExtractor.getRDBCommodityCapacity(any(), eq(CommodityType.STORAGE_AMOUNT))).thenReturn(Optional.of(storageAmount));
        when(infoExtractor.getRDBCommodityCapacity(any(), eq(CommodityType.STORAGE_ACCESS))).thenReturn(Optional.of(iopsAmount));
        when(topology.getConnectedRegion(dbId)).thenReturn(Optional.of(region));
        when(topology.getConnectedAvailabilityZone(dbId)).thenReturn(Optional.of(availabilityZone));
        when(topology.getOwner(dbId)).thenReturn(Optional.of(businessAccount));
        when(topology.getDatabaseServerTier(dbId)).thenReturn(Optional.of(databaseServerTier));
    }

    private static TestEntityClass createStorageTier(final long storageTierId) {
        return TestEntityClass.newBuilder(storageTierId)
            .build(infoExtractor);
    }
}
