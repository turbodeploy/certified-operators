package com.vmturbo.cost.calculation;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.cost.calculation.CloudCostCalculator.CloudCostCalculatorFactory;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.ReservedInstanceApplicator.ReservedInstanceApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.NetworkConfig;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEdition;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DatabaseEngine;
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
                        discountApplicatorFactory, reservedInstanceApplicatorFactory);

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
                        reservedInstanceApplicatorFactory);

        final TestEntityClass testEntity = TestEntityClass.newBuilder(entityId)
                .setType(EntityType.DATABASE_VALUE)
                .setDatabaseConfig(new EntityInfoExtractor.DatabaseConfig(DatabaseEdition.SQL_SERVER_ENTERPRISE
                        , DatabaseEngine.MYSQL))
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

}
