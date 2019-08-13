package com.vmturbo.cost.calculation;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Pricing;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.cost.calculation.CloudCostCalculator.CloudCostCalculatorFactory;
import com.vmturbo.cost.calculation.CloudCostCalculator.DependentCostLookup;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.ReservedInstanceApplicator.ReservedInstanceApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.NetworkConfig;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.VirtualVolumeConfig;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DeploymentType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.LicenseModel;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList.ComputeTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.DatabaseTierPriceList.DatabaseTierConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.IpPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.IpPriceList.IpConfigPrice;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceByOsEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceByOsEntry.LicensePrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList.StorageTierPrice;

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
            CloudCostCalculator.<TestEntityClass>newFactory();

    private static final DiscountApplicatorFactory<TestEntityClass> discountApplicatorFactory =
            (DiscountApplicatorFactory<TestEntityClass>)mock(DiscountApplicatorFactory.class);

    private static final ReservedInstanceApplicatorFactory<TestEntityClass> reservedInstanceApplicatorFactory =
            (ReservedInstanceApplicatorFactory<TestEntityClass>)mock(ReservedInstanceApplicatorFactory.class);

    private static final Map<Long, EntityReservedInstanceCoverage> topologyRiCoverage = Maps.newHashMap();

    private static final NetworkConfig networkConfig = mock(NetworkConfig.class);

    private static final double BASE_PRICE = 10.0;
    private static final double DEFAULT_RI_COVERAGE = 0.2;
    private static final double SUSE_ADJUSTMENT = 5;
    private static final double WSQL_ADJUSTMENT = 7;
    private static final double WSQL_ENTERPRISE_1 = 10;
    private static final double WSQL_ENTERPRISE_2 = 20;
    private static final double WSQL_ENTERPRISE_8 = 40;
    public static final int IP_COUNT = 5;
    private static final int IP_RANGE = 3;
    private static final double IP_PRICE_RANGE_1 = 8;
    private static final double IP_PRICE = 14;
    private static final double MYSQL_ADJUSTMENT = 5;
    private static final int GB_RANGE = 11;
    private static final double GB_PRICE_RANGE_1 = 13.0; // price per GB within GB_RANGE
    private static final double GB_PRICE = 9.0; // price per GB above the range
    private static final int IOPS_RANGE = 11;
    private static final double IOPS_PRICE_RANGE_1 = 16.0; // price per GB within GB_RANGE
    private static final double IOPS_PRICE = 4.5; // price per GB above the range
    private static final double GB_MONTH_PRICE_10 = 14.0;
    private static final double GB_MONTH_PRICE_20 = 26.0;
    private static final long V_VOLUME_SIZRE_IOPS = 17;

    private static final long REGION_ID = 1;
    private static final long STORAGE_TIER_ID = 10;
    private static final long COMPUTE_TIER_ID = 11;
    private static final long DB_TIER_ID = 4;
    private static final long VOLUME_ID = 7;
    private static final long DEFAULT_VM_ID = 7;
    private static final long DEFAULT_SERVICE_ID = 0;
    private static final long DEFAULT_ELASTIC_IPS_BOUGHT = 10;

    private static final TestEntityClass region = TestEntityClass.newBuilder(REGION_ID)
                    .build(infoExtractor);
    private static final TestEntityClass storageTier = TestEntityClass.newBuilder(STORAGE_TIER_ID)
                    .build(infoExtractor);
    private static final TestEntityClass computeTier = TestEntityClass.newBuilder(COMPUTE_TIER_ID)
                    .build(infoExtractor);
    private static final TestEntityClass databaseTier = TestEntityClass.newBuilder(DB_TIER_ID)
                    .build(infoExtractor);

    private static final PriceTable PRICE_TABLE = thePriceTable();
    private static final CloudCostData CLOUD_COST_DATA = new CloudCostData(PRICE_TABLE,
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap());
    private static final CloudCostCalculator<TestEntityClass> CALCULATOR = calculator();

    private static final double DELTA = 0.0001;

    /**
     * Test a simple on-demand calculation (no RI, no discount) for a VM.
     *
     * @throws CloudCostDataRetrievalException not expected to happen in the test
     */
    @Before
    public void init(){
        // Configure NetworkConfig
        when(networkConfig.getNumElasticIps()).thenReturn(DEFAULT_ELASTIC_IPS_BOUGHT);

        final TestEntityClass service = TestEntityClass.newBuilder(DEFAULT_SERVICE_ID)
            .build(infoExtractor);
        // Configure CloudTopology
        when(topology.getConnectedRegion(DEFAULT_VM_ID)).thenReturn(Optional.of(region));
        when(topology.getConnectedService(COMPUTE_TIER_ID)).thenReturn(Optional.of(service));
        when(topology.getComputeTier(DEFAULT_VM_ID)).thenReturn(Optional.of(computeTier));

        // Configure ReservedInstanceApplicator
        final ReservedInstanceApplicator<TestEntityClass> riApplicator =
            mock(ReservedInstanceApplicator.class);
        when(riApplicator.recordRICoverage(computeTier)).thenReturn(DEFAULT_RI_COVERAGE);
        when(reservedInstanceApplicatorFactory.newReservedInstanceApplicator(
            any(), eq(infoExtractor), eq(CLOUD_COST_DATA), eq(topologyRiCoverage)))
            .thenReturn(riApplicator);
    }

    @Test
    public void testCalculateOnDemandCostForCompute() {
        DiscountApplicator<TestEntityClass> discountApplicator = setupDiscountApplicator(0.0);
        TestEntityClass wsqlVm4Cores = createVmTestEntity(DEFAULT_VM_ID, EntityType.VIRTUAL_MACHINE_VALUE,
            OSType.WINDOWS_WITH_SQL_ENTERPRISE, Tenancy.DEFAULT, VMBillingType.ONDEMAND, 4);
        // act
        final CostJournal<TestEntityClass> journal1 = CALCULATOR.calculateCost(wsqlVm4Cores);

        // assert
        double expectedIpAdjustment = IP_RANGE * IP_PRICE_RANGE_1 + (IP_COUNT - IP_RANGE) * IP_PRICE;

        // The cost of the RI isn't factored in because we mocked out the RI Applicator.
        assertThat(journal1.getTotalHourlyCost(),
            is((BASE_PRICE + WSQL_ADJUSTMENT) * (1 - DEFAULT_RI_COVERAGE) + expectedIpAdjustment
                + WSQL_ENTERPRISE_8));
        assertThat(journal1.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE),
            is(BASE_PRICE * (1 - DEFAULT_RI_COVERAGE)));
        assertThat(journal1.getHourlyCostForCategory(CostCategory.LICENSE),
            is(WSQL_ADJUSTMENT * (1 - DEFAULT_RI_COVERAGE) + WSQL_ENTERPRISE_8));
        assertThat(journal1.getHourlyCostForCategory(CostCategory.IP), is(expectedIpAdjustment));

        // Once for the compute, once for the adjustment, once for the license, because all costs
        // are "paid to" the compute tier.
        verify(discountApplicator, times(3)).getDiscountPercentage(computeTier);

        // In the previous test, the number of CPUs (4) was in between LicensePrice number of cores
        // (2 and 8) and we verified we use the higher cores license price.
        // Here we verify that when the number of CPUs equals one of the LicensePrice number of cores
        // then that price is used.
        TestEntityClass wsqlVm2Cores = createVmTestEntity(DEFAULT_VM_ID, EntityType.VIRTUAL_MACHINE_VALUE,
            OSType.WINDOWS_WITH_SQL_ENTERPRISE, Tenancy.DEFAULT, VMBillingType.ONDEMAND, 2);
        final CostJournal<TestEntityClass> journal2 = CALCULATOR.calculateCost(wsqlVm2Cores);
        assertThat(journal2.getHourlyCostForCategory(CostCategory.LICENSE),
            is(WSQL_ADJUSTMENT * (1 - DEFAULT_RI_COVERAGE) + WSQL_ENTERPRISE_2));

        final TestEntityClass spotVm = createVmTestEntity(DEFAULT_VM_ID, EntityType.VIRTUAL_MACHINE_VALUE,
            OSType.SUSE, Tenancy.DEFAULT, VMBillingType.BIDDING, 2);
        final CostJournal<TestEntityClass> spotJournal = CALCULATOR.calculateCost(spotVm);
        assertThat(spotJournal.getHourlyCostForCategory(CostCategory.SPOT),
            is(BASE_PRICE * (1 - DEFAULT_RI_COVERAGE)));
        // No adjustment and license costs for spot instances
        assertThat(spotJournal.getHourlyCostForCategory(CostCategory.LICENSE), is(0.0));

        final TestEntityClass suseVm =  createVmTestEntity(DEFAULT_VM_ID, EntityType.VIRTUAL_MACHINE_VALUE,
            OSType.SUSE, Tenancy.DEFAULT, VMBillingType.ONDEMAND, 2);

        final CostJournal<TestEntityClass> journal3 = CALCULATOR.calculateCost(suseVm);
        assertThat(journal3.getHourlyCostForCategory(CostCategory.LICENSE),
            is(SUSE_ADJUSTMENT * (1 - DEFAULT_RI_COVERAGE)));
    }

    private TestEntityClass createVmTestEntity(long id, int entityType, OSType osType, Tenancy tenancy,
                                               VMBillingType billingType, int numCores){
        return TestEntityClass.newBuilder(id)
            .setType(entityType)
            .setComputeConfig(new EntityInfoExtractor.ComputeConfig(osType,
                tenancy, billingType, numCores))
            .setNetworkConfig(networkConfig)
            .build(infoExtractor);
    }

    private DiscountApplicator<TestEntityClass> setupDiscountApplicator(double returnDiscount) {
        DiscountApplicator<TestEntityClass> discountApplicator =
            (DiscountApplicator<TestEntityClass>)mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(any())).thenReturn(returnDiscount);
        when(discountApplicatorFactory.entityDiscountApplicator(
            any(), eq(topology), eq(infoExtractor), eq(CLOUD_COST_DATA)))
            .thenReturn(discountApplicator);
        return discountApplicator;
    }

    /**
     * This is more of an "integration test" to make sure that a VM correctly inherits the
     * storage cost of the journal.
     *
     * @throws CloudCostDataRetrievalException not expected to happen in this test
     */
    @Test
    public void testCalculateVMStorageCost() throws CloudCostDataRetrievalException {
        final long vmId = 7;

        // Set up the VM
        final TestEntityClass vm = TestEntityClass.newBuilder(vmId)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build(infoExtractor);
        when(topology.getConnectedRegion(vmId)).thenReturn(Optional.of(region));
        when(topology.getStorageTier(vmId)).thenReturn(Optional.of(storageTier));
        when(topology.getComputeTier(vmId)).thenReturn(Optional.of(computeTier));

        // Set up the discount applicator (no discount)
        DiscountApplicator<TestEntityClass> discountApplicator = setupDiscountApplicator(0.0);

        // Set up the RI applicator (no RI)
        final ReservedInstanceApplicator<TestEntityClass> riApplicator =
                mock(ReservedInstanceApplicator.class);
        when(riApplicator.recordRICoverage(computeTier)).thenReturn(0.0);
        when(reservedInstanceApplicatorFactory.newReservedInstanceApplicator(
            any(), eq(infoExtractor), eq(CLOUD_COST_DATA), eq(topologyRiCoverage)))
                .thenReturn(riApplicator);

        // Set up a volume, and the cost lookup for the volume.
        final TestEntityClass volume = TestEntityClass.newBuilder(123)
                .setType(EntityType.VIRTUAL_VOLUME_VALUE)
                .build(infoExtractor);
        when(topology.getConnectedVolumes(vmId)).thenReturn(Collections.singletonList(volume));
        // A simple cost journal for the volume.
        final CostJournal<TestEntityClass> volumeJournal =
            CostJournal.newBuilder(volume, infoExtractor, region, discountApplicator, e2 -> null)
                // Just a mock price that's easy to work with.
                .recordOnDemandCost(CostCategory.STORAGE, storageTier, Price.newBuilder()
                        .setUnit(Unit.HOURS)
                        .setPriceAmount(CurrencyAmount.newBuilder()
                                .setAmount(10))
                        .build(), 1)
                .build();
        // The cost lookup will tell the VM's cost journal what the cost journal of the volume
        // looks like.
        final DependentCostLookup<TestEntityClass> volumeCostLookup = e -> volumeJournal;

        final CloudCostCalculator<TestEntityClass> calculator =
                calculatorFactory.newCalculator(CLOUD_COST_DATA, topology, infoExtractor,
                        discountApplicatorFactory, reservedInstanceApplicatorFactory,
                        volumeCostLookup, topologyRiCoverage);

        final CostJournal<TestEntityClass> vmJournal = calculator.calculateCost(vm);
        // The cost for the VM sh
        assertThat(vmJournal.getTotalHourlyCost(),
                closeTo(volumeJournal.getTotalHourlyCost(), DELTA));
        assertThat(vmJournal.getHourlyCostForCategory(CostCategory.STORAGE),
                closeTo(volumeJournal.getHourlyCostForCategory(CostCategory.STORAGE), DELTA));
    }

    @Test
    public void fooTest() {
        List<String> list = Collections.emptyList();
        Iterators.partition(list.iterator(), 1).forEachRemaining(chunk -> {
            System.out.println("Got chunk: " + chunk);
        });
    }

    @Test
    public void testCalculateVolumeCostIOPS() {
        final TestEntityClass volume = TestEntityClass.newBuilder(VOLUME_ID)
                .setType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setVolumeConfig(new VirtualVolumeConfig(V_VOLUME_SIZRE_IOPS, 0))
                .build(infoExtractor);
        when(topology.getConnectedRegion(VOLUME_ID)).thenReturn(Optional.of(region));
        when(topology.getStorageTier(VOLUME_ID)).thenReturn(Optional.of(storageTier));

        setupDiscountApplicator(0.0);

        final CostJournal<TestEntityClass> journal = CALCULATOR.calculateCost(volume);
        assertThat(journal.getTotalHourlyCost(),
            closeTo(IOPS_RANGE * IOPS_PRICE_RANGE_1 + (V_VOLUME_SIZRE_IOPS - IOPS_RANGE)
                    * IOPS_PRICE, DELTA));
    }

    /**
     * Test both Unit.GB_MONTH and Unit.MONTH components of volume cost.
     *
     * @throws CloudCostDataRetrievalException not expected to happen
     */
    @Test
    public void testCalculateVolumeCostGBMonth() {
        final int vVolSizeMb = 19; // should be >= GB_RANGE

        final TestEntityClass volume = TestEntityClass.newBuilder(VOLUME_ID)
                .setType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setVolumeConfig(new VirtualVolumeConfig(0, vVolSizeMb * 1024))
                .build(infoExtractor);
        when(topology.getConnectedRegion(VOLUME_ID)).thenReturn(Optional.of(region));
        when(topology.getStorageTier(VOLUME_ID)).thenReturn(Optional.of(storageTier));

        setupDiscountApplicator(0.0);

        final CostJournal<TestEntityClass> journal = CALCULATOR.calculateCost(volume);
        assertThat(journal.getTotalHourlyCost(),
            closeTo(GB_RANGE * GB_PRICE_RANGE_1 + (vVolSizeMb - GB_RANGE) * GB_PRICE
                + GB_MONTH_PRICE_20, DELTA));
    }

    /**
     * Test a on-demand calculation (no discount) for a Database.
     *
     * @throws CloudCostDataRetrievalException not expected to happen in this test
     */
    @Test
    public void testCalculateOnDemandCostForDatabase() {
        // arrange
        final long dbId = 9;

        final TestEntityClass db = TestEntityClass.newBuilder(dbId)
                .setType(EntityType.DATABASE_VALUE)
                .setDatabaseConfig(new EntityInfoExtractor.DatabaseConfig(
                    DatabaseEdition.SQL_SERVER_ENTERPRISE,
                    DatabaseEngine.MYSQL, LicenseModel.BRING_YOUR_OWN_LICENSE, DeploymentType.SINGLE_AZ))
                .build(infoExtractor);

        when(topology.getConnectedRegion(dbId)).thenReturn(Optional.of(region));
        when(topology.getDatabaseTier(dbId)).thenReturn(Optional.of(databaseTier));

        final DiscountApplicator<TestEntityClass> discountApplicator = setupDiscountApplicator(0.0);
        // act
        final CostJournal<TestEntityClass> journal = CALCULATOR.calculateCost(db);

        // assert
        assertThat(journal.getTotalHourlyCost(), is(BASE_PRICE + MYSQL_ADJUSTMENT));
        assertThat(journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE), is(BASE_PRICE));
        assertThat(journal.getHourlyCostForCategory(CostCategory.LICENSE), is(MYSQL_ADJUSTMENT));

        // Once for the compute, once for the license, because both costs are "paid to" the
        // database tier.
        verify(discountApplicator, times(2)).getDiscountPercentage(databaseTier);
    }

    /**
     * Verify that an entity of a type that doesn't have a cost (e.g. NETWORK) returns
     * an empty journal.
     *
     * @throws CloudCostDataRetrievalException not expected to happen in the test
     */
    @Test
    public void testEmptyCost() throws CloudCostDataRetrievalException {
        final TestEntityClass noCostEntity = TestEntityClass.newBuilder(7)
            .setType(EntityType.NETWORK_VALUE)
            .build(infoExtractor);
        final CostJournal<TestEntityClass> journal = CALCULATOR.calculateCost(noCostEntity);
        assertThat(journal.getEntity(), is(noCostEntity));
        assertThat(journal.getTotalHourlyCost(), is(0.0));
        assertThat(journal.getCategories(), is(Collections.emptySet()));
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

    private static LicensePrice licensePrice(int numCores, Price price) {
        return LicensePrice.newBuilder()
                        .setNumberOfCores(numCores)
                        .setPrice(price)
                        .build();
    }

    private static PriceTable thePriceTable() {
        return PriceTable.newBuilder()
            .putOnDemandPriceByRegionId(REGION_ID, OnDemandPriceTable.newBuilder()
                .putCloudStoragePricesByTierId(STORAGE_TIER_ID, StorageTierPriceList.newBuilder()
                    .addCloudStoragePrice(StorageTierPrice.newBuilder()
                        .addPrices(price(Unit.MILLION_IOPS, IOPS_RANGE, IOPS_PRICE_RANGE_1
                            * CostProtoUtil.HOURS_IN_MONTH))
                        .addPrices(price(Unit.MILLION_IOPS, IOPS_PRICE * CostProtoUtil.HOURS_IN_MONTH)))
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
                        .build())
                    .build())
                .putDbPricesByInstanceId(DB_TIER_ID, DatabaseTierPriceList.newBuilder()
                    .setBasePrice(DatabaseTierConfigPrice.newBuilder()
                            .setDbEdition(DatabaseEdition.NONE)
                            .setDbEngine(DatabaseEngine.MARIADB)
                            .addPrices(price(Unit.HOURS, BASE_PRICE)))
                    .addConfigurationPriceAdjustments(DatabaseTierConfigPrice.newBuilder()
                            .setDbEdition(DatabaseEdition.SQL_SERVER_ENTERPRISE)
                            .setDbEngine(DatabaseEngine.MYSQL)
                            .addPrices(price(Unit.HOURS, MYSQL_ADJUSTMENT)))
                    .build())
                .putComputePricesByTierId(COMPUTE_TIER_ID, ComputeTierPriceList.newBuilder()
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
                    .build())
                .setIpPrices(IpPriceList.newBuilder().addIpPrice(IpConfigPrice.newBuilder()
                    .setFreeIpCount(IP_COUNT)
                    .addPrices(price(Unit.HOURS, IP_RANGE, IP_PRICE_RANGE_1))
                    .addPrices(price(Unit.HOURS, IP_PRICE))
                    .build())
                    .build())
                .build())
            .putSpotPriceByRegionId(REGION_ID, Pricing.SpotInstancePriceTable.newBuilder()
                .putSpotPriceByInstanceId(COMPUTE_TIER_ID, price(Unit.HOURS, BASE_PRICE))
                .build())
            .addLicensePrices(LicensePriceByOsEntry.newBuilder()
                .setOsType(OSType.WINDOWS_WITH_SQL_ENTERPRISE)
                .addLicensePrices(licensePrice(1, price(Unit.HOURS, WSQL_ENTERPRISE_1)))
                .addLicensePrices(licensePrice(2, price(Unit.HOURS, WSQL_ENTERPRISE_2)))
                .addLicensePrices(licensePrice(8, price(Unit.HOURS, WSQL_ENTERPRISE_8)))
                .build())
            .build();
    }

    private static CloudCostCalculator<TestEntityClass> calculator() {
        try {
            return calculatorFactory.newCalculator(CLOUD_COST_DATA, topology, infoExtractor,
                discountApplicatorFactory, reservedInstanceApplicatorFactory,
                e -> null, topologyRiCoverage);
        } catch (CloudCostDataRetrievalException e) {
            // no expected to happen
            return null;
        }
    }
}
