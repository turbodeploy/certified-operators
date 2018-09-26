package com.vmturbo.cost.calculation;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
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
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostDataRetrievalException;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList.ComputeTierConfigPrice;
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

    /**
     * Test a simple on-demand calculation (no RI, no discount) for a VM.
     */
    @Test
    public void testCalculateOnDemandCost() throws CloudCostDataRetrievalException {
        // arrange
        final long regionId = 1;
        final long computeTierId = 2;
        final long entityId = 7;
        final double basePrice = 10;
        final double suseAdjustment = 5;
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
                        .build())
                .build();

        final CloudCostData cloudCostData = new CloudCostData(priceTable, Collections.emptyMap());
        when(dataProvider.getCloudCostData()).thenReturn(cloudCostData);

        final CloudCostCalculator<TestEntityClass> calculator =
                calculatorFactory.newCalculator(dataProvider, topology, infoExtractor, discountApplicatorFactory);

        final TestEntityClass testEntity = TestEntityClass.newBuilder(entityId)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setComputeConfig(new EntityInfoExtractor.ComputeConfig(OSType.SUSE, Tenancy.DEFAULT))
                .build(infoExtractor);

        final TestEntityClass region = TestEntityClass.newBuilder(regionId)
                .build(infoExtractor);

        final TestEntityClass computeTier = TestEntityClass.newBuilder(computeTierId)
                .build(infoExtractor);

        when(topology.getConnectedRegion(entityId)).thenReturn(Optional.of(region));
        when(topology.getComputeTier(entityId)).thenReturn(Optional.of(computeTier));

        final DiscountApplicator<TestEntityClass> discountApplicator =
                (DiscountApplicator<TestEntityClass>)mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(any())).thenReturn(0.0);
        when(discountApplicatorFactory.entityDiscountApplicator(testEntity, topology, infoExtractor, cloudCostData))
            .thenReturn(discountApplicator);

        // act
        final CostJournal<TestEntityClass> journal = calculator.calculateCost(testEntity);

        // assert
        assertThat(journal.getTotalHourlyCost(), is(basePrice + suseAdjustment));
        assertThat(journal.getHourlyCostForCategory(CostCategory.COMPUTE), is(basePrice));
        assertThat(journal.getHourlyCostForCategory(CostCategory.LICENSE), is(suseAdjustment));

        // Once for the compute, once for the license, because both costs are "paid to" the
        // compute tier.
        verify(discountApplicator, times(2)).getDiscountPercentage(computeTier);
    }

}
