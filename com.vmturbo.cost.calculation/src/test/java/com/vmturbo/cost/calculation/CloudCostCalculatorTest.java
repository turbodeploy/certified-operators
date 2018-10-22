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
import java.util.Optional;

import org.junit.Test;

import com.google.common.collect.Iterators;

import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.cost.calculation.CloudCostCalculator.CloudCostCalculatorFactory;
import com.vmturbo.cost.calculation.CloudCostCalculator.DependentCostLookup;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.ReservedInstanceApplicator.ReservedInstanceApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.NetworkConfig;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.VirtualVolumeConfig;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
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
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList.StorageTierPrice;

public class CloudCostCalculatorTest {

    private final CloudCostDataProvider dataProvider = mock(CloudCostDataProvider.class);

    private final CloudTopology<TestEntityClass> topology =
            (CloudTopology<TestEntityClass>)mock(CloudTopology.class);

    private final EntityInfoExtractor<TestEntityClass> infoExtractor =
            (EntityInfoExtractor<TestEntityClass>)mock(EntityInfoExtractor.class);

    private CloudCostCalculatorFactory<TestEntityClass> calculatorFactory =
            CloudCostCalculator.<TestEntityClass>newFactory();

    private DiscountApplicatorFactory<TestEntityClass> discountApplicatorFactory =
            (DiscountApplicatorFactory<TestEntityClass>)mock(DiscountApplicatorFactory.class);

    private ReservedInstanceApplicatorFactory<TestEntityClass> reservedInstanceApplicatorFactory =
            (ReservedInstanceApplicatorFactory<TestEntityClass>)mock(ReservedInstanceApplicatorFactory.class);

    private NetworkConfig networkConfig = (NetworkConfig)mock(NetworkConfig.class);

    /**
     * Test a simple on-demand calculation (no RI, no discount) for a VM.
     */
    @Test
    public void testCalculateOnDemandCostForCompute() throws CloudCostDataRetrievalException {
        // arrange
        final long regionId = 1;
        final long computeTierId = 2;
        final long serviceId = 3;
        final long entityId = 7;
        final long numElasticIpsBought = 10;
        final double basePrice = 10;
        final double suseAdjustment = 5;
        // VM buys 5 free IPs, 3 IPs at 2 units of price and 2 IPs at 4 units of price
        final double ipAdjustment = 3*2 + 2*4;
        final PriceTable priceTable = PriceTable.newBuilder()
                .putOnDemandPriceByRegionId(regionId, OnDemandPriceTable.newBuilder()
                        .putComputePricesByTierId(computeTierId, ComputeTierPriceList.newBuilder()
                                .setBasePrice(ComputeTierConfigPrice.newBuilder()
                                        .setGuestOsType(OSType.LINUX)
                                        .setTenancy(Tenancy.DEFAULT)
                                        .addPrices(Price.newBuilder()
                                            .setUnit(Unit.HOURS)
                                            .setPriceAmount(CurrencyAmount.newBuilder()
                                                .setAmount(basePrice))))
                                .addPerConfigurationPriceAdjustments(ComputeTierConfigPrice.newBuilder()
                                        .setGuestOsType(OSType.SUSE)
                                        .setTenancy(Tenancy.DEFAULT)
                                        .addPrices(Price.newBuilder()
                                                .setUnit(Unit.HOURS)
                                                .setPriceAmount(CurrencyAmount.newBuilder()
                                                        .setAmount(suseAdjustment))))
                                .build())
                        .setIpPrices(IpPriceList.newBuilder().addIpPrice(IpConfigPrice.newBuilder()
                                        .setFreeIpCount(5)
                                        .addPrices(Price.newBuilder()
                                                .setUnit(Unit.HOURS)
                                                .setEndRangeInUnits(3)
                                                .setPriceAmount(CurrencyAmount.newBuilder()
                                                        .setAmount(2)))
                                        .addPrices(Price.newBuilder()
                                                .setUnit(Unit.HOURS)
                                                .setEndRangeInUnits(5)
                                                .setPriceAmount(CurrencyAmount.newBuilder()
                                                        .setAmount(4)))
                                        .build())
                                .build())
                        .build())
                .build();

        // configure networkConfig
        when(networkConfig.getNumElasticIps()).thenReturn(numElasticIpsBought);

        final CloudCostData cloudCostData = new CloudCostData(priceTable, Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        when(dataProvider.getCloudCostData()).thenReturn(cloudCostData);

        final CloudCostCalculator<TestEntityClass> calculator =
                calculatorFactory.newCalculator(dataProvider, topology, infoExtractor,
                        discountApplicatorFactory, reservedInstanceApplicatorFactory, e -> null);

        final TestEntityClass testEntity = TestEntityClass.newBuilder(entityId)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setComputeConfig(new EntityInfoExtractor.ComputeConfig(OSType.SUSE, Tenancy.DEFAULT))
                .setNetworkConfig(networkConfig)
                .build(infoExtractor);

        final TestEntityClass region = TestEntityClass.newBuilder(regionId)
                .build(infoExtractor);

        final TestEntityClass service = TestEntityClass.newBuilder(serviceId)
                .build(infoExtractor);

        final TestEntityClass computeTier = TestEntityClass.newBuilder(computeTierId)
                .build(infoExtractor);

        when(topology.getConnectedRegion(entityId)).thenReturn(Optional.of(region));
        when(topology.getConnectedService(computeTierId)).thenReturn(Optional.of(service));
        when(topology.getComputeTier(entityId)).thenReturn(Optional.of(computeTier));

        final DiscountApplicator<TestEntityClass> discountApplicator =
                (DiscountApplicator<TestEntityClass>)mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(any())).thenReturn(0.0);
        when(discountApplicatorFactory.entityDiscountApplicator(testEntity, topology, infoExtractor, cloudCostData))
            .thenReturn(discountApplicator);

        final double riCoverage = 0.2;
        final ReservedInstanceApplicator<TestEntityClass> riApplicator =
                mock(ReservedInstanceApplicator.class);
        when(riApplicator.recordRICoverage(computeTier)).thenReturn(riCoverage);
        when(reservedInstanceApplicatorFactory.newReservedInstanceApplicator(any(), eq(infoExtractor), eq(cloudCostData)))
                .thenReturn(riApplicator);

        // act
        final CostJournal<TestEntityClass> journal = calculator.calculateCost(testEntity);

        // assert
        // The cost of the RI isn't factored in because we mocked out the RI Applicator.
        assertThat(journal.getTotalHourlyCost(), is((basePrice + suseAdjustment) * (1 - riCoverage) + ipAdjustment));
        assertThat(journal.getHourlyCostForCategory(CostCategory.COMPUTE), is(basePrice * (1 - riCoverage)));
        assertThat(journal.getHourlyCostForCategory(CostCategory.LICENSE), is(suseAdjustment * (1 - riCoverage)));
        assertThat(journal.getHourlyCostForCategory(CostCategory.IP), is(ipAdjustment));

        // Once for the compute, once for the license, because both costs are "paid to" the
        // compute tier.
        verify(discountApplicator, times(2)).getDiscountPercentage(computeTier);
    }

    /**
     * This is more of an "integration test" to make sure that a VM correctly inherits the
     * storage cost of the journal.
     */
    @Test
    public void testCalculateVMStorageCost() throws CloudCostDataRetrievalException {
        final long regionId = 1;
        final long vmId = 7;
        final long storageTierId = 10;
        final long computeTierId = 11;

        // No price data necessary - we're going to hard-code the price data.
        final CloudCostData cloudCostData = new CloudCostData(PriceTable.getDefaultInstance(), Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());;
        when(dataProvider.getCloudCostData()).thenReturn(cloudCostData);

        // Set up the VM
        final TestEntityClass vm = TestEntityClass.newBuilder(vmId)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .build(infoExtractor);
        final TestEntityClass region = TestEntityClass.newBuilder(regionId)
                .build(infoExtractor);
        final TestEntityClass storageTier = TestEntityClass.newBuilder(storageTierId)
                .build(infoExtractor);
        final TestEntityClass computeTier = TestEntityClass.newBuilder(computeTierId)
                .build(infoExtractor);
        when(topology.getConnectedRegion(vmId)).thenReturn(Optional.of(region));
        when(topology.getStorageTier(vmId)).thenReturn(Optional.of(storageTier));
        when(topology.getComputeTier(vmId)).thenReturn(Optional.of(computeTier));

        // Set up the discount applicator (no discount)
        final DiscountApplicator<TestEntityClass> discountApplicator =
                (DiscountApplicator<TestEntityClass>)mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(any())).thenReturn(0.0);
        when(discountApplicatorFactory.entityDiscountApplicator(vm, topology, infoExtractor, cloudCostData))
                .thenReturn(discountApplicator);

        // Set up the RI applicator (no RI)
        final ReservedInstanceApplicator<TestEntityClass> riApplicator =
                mock(ReservedInstanceApplicator.class);
        when(riApplicator.recordRICoverage(computeTier)).thenReturn(0.0);
        when(reservedInstanceApplicatorFactory.newReservedInstanceApplicator(any(), eq(infoExtractor), eq(cloudCostData)))
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
                calculatorFactory.newCalculator(dataProvider, topology, infoExtractor,
                        discountApplicatorFactory, reservedInstanceApplicatorFactory, volumeCostLookup);

        final CostJournal<TestEntityClass> vmJournal = calculator.calculateCost(vm);
        // The cost for the VM sh
        assertThat(vmJournal.getTotalHourlyCost(),
                closeTo(volumeJournal.getTotalHourlyCost(), 0.0001));
        assertThat(vmJournal.getHourlyCostForCategory(CostCategory.STORAGE),
                closeTo(volumeJournal.getHourlyCostForCategory(CostCategory.STORAGE), 0.0001));
    }

    @Test
    public void fooTest() {
        List<String> list = Collections.emptyList();
        Iterators.partition(list.iterator(), 1).forEachRemaining(chunk -> {
            System.out.println("Got chunk: " + chunk);
        });
    }

    @Test
    public void testCalculateVolumeCostIOPS() throws CloudCostDataRetrievalException {
        final long regionId = 1;
        final long volumeId = 7;
        final long storageTierId = 10;
        final PriceTable priceTable = PriceTable.newBuilder()
            .putOnDemandPriceByRegionId(regionId, OnDemandPriceTable.newBuilder()
                .putCloudStoragePricesByTierId(storageTierId, StorageTierPriceList.newBuilder()
                    .addCloudStoragePrice(StorageTierPrice.newBuilder()
                            .addPrices(Price.newBuilder()
                                .setUnit(Unit.MILLION_IOPS)
                                .setEndRangeInUnits(10)
                                .setPriceAmount(CurrencyAmount.newBuilder()
                                    .setAmount(10 * CostProtoUtil.HOURS_IN_MONTH)))
                            .addPrices(Price.newBuilder()
                                .setUnit(Unit.MILLION_IOPS)
                                .setPriceAmount(CurrencyAmount.newBuilder()
                                    .setAmount(5 * CostProtoUtil.HOURS_IN_MONTH))))
                    .build())
                .build())
            .build();

        final CloudCostData cloudCostData = new CloudCostData(priceTable, Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());;
        when(dataProvider.getCloudCostData()).thenReturn(cloudCostData);

        final CloudCostCalculator<TestEntityClass> calculator =
                calculatorFactory.newCalculator(dataProvider, topology, infoExtractor, discountApplicatorFactory,
                        reservedInstanceApplicatorFactory, e -> null);

        final TestEntityClass volume = TestEntityClass.newBuilder(volumeId)
                .setType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setVolumeConfig(new VirtualVolumeConfig(15, 0))
                .build(infoExtractor);
        final TestEntityClass region = TestEntityClass.newBuilder(regionId)
                .build(infoExtractor);
        final TestEntityClass storageTier = TestEntityClass.newBuilder(storageTierId)
                .build(infoExtractor);

        when(topology.getConnectedRegion(volumeId)).thenReturn(Optional.of(region));
        when(topology.getStorageTier(volumeId)).thenReturn(Optional.of(storageTier));

        final DiscountApplicator<TestEntityClass> discountApplicator =
                (DiscountApplicator<TestEntityClass>)mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(any())).thenReturn(0.0);
        when(discountApplicatorFactory.entityDiscountApplicator(volume, topology, infoExtractor, cloudCostData))
                .thenReturn(discountApplicator);

        final CostJournal<TestEntityClass> journal = calculator.calculateCost(volume);
        assertThat(journal.getTotalHourlyCost(), closeTo(10 * 10 + 5 * 5, 0.001));

    }

    @Test
    public void testCalculateVolumeCostGBMonth() throws CloudCostDataRetrievalException {
        final long regionId = 1;
        final long volumeId = 7;
        final long storageTierId = 10;
        final PriceTable priceTable = PriceTable.newBuilder()
                .putOnDemandPriceByRegionId(regionId, OnDemandPriceTable.newBuilder()
                        .putCloudStoragePricesByTierId(storageTierId, StorageTierPriceList.newBuilder()
                                .addCloudStoragePrice(StorageTierPrice.newBuilder()
                                        .addPrices(Price.newBuilder()
                                                .setUnit(Unit.GB_MONTH)
                                                .setEndRangeInUnits(10)
                                                .setPriceAmount(CurrencyAmount.newBuilder()
                                                        .setAmount(10 * CostProtoUtil.HOURS_IN_MONTH)))
                                        .addPrices(Price.newBuilder()
                                                .setUnit(Unit.GB_MONTH)
                                                .setPriceAmount(CurrencyAmount.newBuilder()
                                                        .setAmount(5 * CostProtoUtil.HOURS_IN_MONTH))))
                                .build())
                        .build())
                .build();

        final CloudCostData cloudCostData = new CloudCostData(priceTable, Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());;
        when(dataProvider.getCloudCostData()).thenReturn(cloudCostData);

        final CloudCostCalculator<TestEntityClass> calculator =
                calculatorFactory.newCalculator(dataProvider, topology, infoExtractor, discountApplicatorFactory,
                        reservedInstanceApplicatorFactory, e -> null);

        final TestEntityClass volume = TestEntityClass.newBuilder(volumeId)
                .setType(EntityType.VIRTUAL_VOLUME_VALUE)
                .setVolumeConfig(new VirtualVolumeConfig(0, 15 * 1024))
                .build(infoExtractor);
        final TestEntityClass region = TestEntityClass.newBuilder(regionId)
                .build(infoExtractor);
        final TestEntityClass storageTier = TestEntityClass.newBuilder(storageTierId)
                .build(infoExtractor);

        when(topology.getConnectedRegion(volumeId)).thenReturn(Optional.of(region));
        when(topology.getStorageTier(volumeId)).thenReturn(Optional.of(storageTier));

        final DiscountApplicator<TestEntityClass> discountApplicator =
                (DiscountApplicator<TestEntityClass>)mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(any())).thenReturn(0.0);
        when(discountApplicatorFactory.entityDiscountApplicator(volume, topology, infoExtractor, cloudCostData))
                .thenReturn(discountApplicator);

        final CostJournal<TestEntityClass> journal = calculator.calculateCost(volume);
        assertThat(journal.getTotalHourlyCost(), closeTo(10 * 10 + 5 * 5, 0.001));
    }

    @Test
    public void testCalculateVolumeCostMonthly() throws CloudCostDataRetrievalException {
        final long regionId = 1;
        final long volumeId = 7;
        final long storageTierId = 10;
        final PriceTable priceTable = PriceTable.newBuilder()
                .putOnDemandPriceByRegionId(regionId, OnDemandPriceTable.newBuilder()
                        .putCloudStoragePricesByTierId(storageTierId, StorageTierPriceList.newBuilder()
                                .addCloudStoragePrice(StorageTierPrice.newBuilder()
                                        .addPrices(Price.newBuilder()
                                                .setUnit(Unit.MONTH)
                                                // 10GB disk - $10/hr
                                                .setEndRangeInUnits(10)
                                                .setPriceAmount(CurrencyAmount.newBuilder()
                                                        .setAmount(10 * CostProtoUtil.HOURS_IN_MONTH)))
                                        .addPrices(Price.newBuilder()
                                                .setUnit(Unit.MONTH)
                                                // 20GB disk - $15/hr
                                                .setEndRangeInUnits(20)
                                                .setPriceAmount(CurrencyAmount.newBuilder()
                                                        .setAmount(15 * CostProtoUtil.HOURS_IN_MONTH))))
                                .build())
                        .build())
                .build();

        final CloudCostData cloudCostData = new CloudCostData(priceTable, Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());;
        when(dataProvider.getCloudCostData()).thenReturn(cloudCostData);

        final CloudCostCalculator<TestEntityClass> calculator =
                calculatorFactory.newCalculator(dataProvider, topology, infoExtractor, discountApplicatorFactory,
                        reservedInstanceApplicatorFactory, e -> null);

        final TestEntityClass volume = TestEntityClass.newBuilder(volumeId)
                .setType(EntityType.VIRTUAL_VOLUME_VALUE)
                // 15GB capacity - needs 20GB disk.
                .setVolumeConfig(new VirtualVolumeConfig(0, 15 * 1024))
                .build(infoExtractor);
        final TestEntityClass region = TestEntityClass.newBuilder(regionId)
                .build(infoExtractor);
        final TestEntityClass storageTier = TestEntityClass.newBuilder(storageTierId)
                .build(infoExtractor);

        when(topology.getConnectedRegion(volumeId)).thenReturn(Optional.of(region));
        when(topology.getStorageTier(volumeId)).thenReturn(Optional.of(storageTier));

        final DiscountApplicator<TestEntityClass> discountApplicator =
                (DiscountApplicator<TestEntityClass>)mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(any())).thenReturn(0.0);
        when(discountApplicatorFactory.entityDiscountApplicator(volume, topology, infoExtractor, cloudCostData))
                .thenReturn(discountApplicator);

        final CostJournal<TestEntityClass> journal = calculator.calculateCost(volume);
        // Price for the 20GB disk.
        assertThat(journal.getTotalHourlyCost(), closeTo(15, 0.001));
    }

    /**
     * Test a on-demand calculation (no discount) for a Database.
     */
    @Test
    public void testCalculateOnDemandCostForDatabase() throws CloudCostDataRetrievalException {
        // arrange
        final long regionId = 1;
        final long dbTierId = 4;
        final long entityId = 9;
        final double basePrice = 10;
        final double mysqlAdjustment = 5;
        final PriceTable priceTable = PriceTable.newBuilder()
                .putOnDemandPriceByRegionId(regionId, OnDemandPriceTable.newBuilder()
                        .putDbPricesByInstanceId(dbTierId, DatabaseTierPriceList.newBuilder()
                                .setBasePrice(DatabaseTierConfigPrice.newBuilder()
                                        .setDbEdition(DatabaseEdition.NONE)
                                        .setDbEngine(DatabaseEngine.MARIADB)
                                        .addPrices(Price.newBuilder()
                                            .setUnit(Unit.HOURS)
                                            .setPriceAmount(CurrencyAmount.newBuilder()
                                                .setAmount(basePrice))))
                                .addConfigurationPriceAdjustments(DatabaseTierConfigPrice.newBuilder()
                                        .setDbEdition(DatabaseEdition.SQL_SERVER_ENTERPRISE)
                                        .setDbEngine(DatabaseEngine.MYSQL)
                                        .addPrices(Price.newBuilder()
                                                .setUnit(Unit.HOURS)
                                                .setPriceAmount(CurrencyAmount.newBuilder()
                                                        .setAmount(mysqlAdjustment))))
                                .build())
                        .build())
                .build();

        final CloudCostData cloudCostData = new CloudCostData(priceTable, Collections.emptyMap(),
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());;
        when(dataProvider.getCloudCostData()).thenReturn(cloudCostData);

        final CloudCostCalculator<TestEntityClass> calculator =
                calculatorFactory.newCalculator(dataProvider, topology, infoExtractor, discountApplicatorFactory,
                        reservedInstanceApplicatorFactory, e -> null);

        final TestEntityClass testEntity = TestEntityClass.newBuilder(entityId)
                .setType(EntityType.DATABASE_VALUE)
                .setDatabaseConfig(new EntityInfoExtractor.DatabaseConfig(DatabaseEdition.SQL_SERVER_ENTERPRISE
                        , DatabaseEngine.MYSQL, LicenseModel.BRING_YOUR_OWN_LICENSE, DeploymentType.SINGLE_AZ))
                .build(infoExtractor);

        final TestEntityClass region = TestEntityClass.newBuilder(regionId)
                .build(infoExtractor);

        final TestEntityClass databaseTier = TestEntityClass.newBuilder(dbTierId)
                .build(infoExtractor);

        when(topology.getConnectedRegion(entityId)).thenReturn(Optional.of(region));
        when(topology.getDatabaseTier(entityId)).thenReturn(Optional.of(databaseTier));

        final DiscountApplicator<TestEntityClass> discountApplicator =
                (DiscountApplicator<TestEntityClass>)mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(any())).thenReturn(0.0);
        when(discountApplicatorFactory.entityDiscountApplicator(testEntity, topology, infoExtractor, cloudCostData))
            .thenReturn(discountApplicator);

        // act
        final CostJournal<TestEntityClass> journal = calculator.calculateCost(testEntity);

        // assert
        assertThat(journal.getTotalHourlyCost(), is(basePrice + mysqlAdjustment));
        assertThat(journal.getHourlyCostForCategory(CostCategory.COMPUTE), is(basePrice));
        assertThat(journal.getHourlyCostForCategory(CostCategory.LICENSE), is(mysqlAdjustment));

        // Once for the compute, once for the license, because both costs are "paid to" the
        // database tier.
        verify(discountApplicator, times(2)).getDiscountPercentage(databaseTier);
    }

    @Test
    public void testEmptyCost() throws CloudCostDataRetrievalException {
        final CloudCostData cloudCostData = new CloudCostData(PriceTable.getDefaultInstance(),
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
                Collections.emptyMap());;
        when(dataProvider.getCloudCostData()).thenReturn(cloudCostData);

        final CloudCostCalculator<TestEntityClass> calculator =
            calculatorFactory.newCalculator(dataProvider, topology, infoExtractor, discountApplicatorFactory,
                    reservedInstanceApplicatorFactory, e -> null);
        final TestEntityClass noCostEntity = TestEntityClass.newBuilder(7)
                .setType(EntityType.HYPERVISOR_VALUE)
                .build(infoExtractor);
        final CostJournal<TestEntityClass> journal = calculator.calculateCost(noCostEntity);
        assertThat(journal.getEntity(), is(noCostEntity));
        assertThat(journal.getTotalHourlyCost(), is(0.0));
        assertThat(journal.getCategories(), is(Collections.emptySet()));
    }

}
