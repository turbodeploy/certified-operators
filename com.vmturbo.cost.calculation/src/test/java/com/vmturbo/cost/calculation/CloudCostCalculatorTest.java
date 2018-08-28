package com.vmturbo.cost.calculation;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.cost.calculation.CostJournal.CostCategory;
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

        final CloudCostData cloudCostData = new CloudCostData(priceTable);
        when(dataProvider.getCloudCostData()).thenReturn(cloudCostData);

        final CloudCostCalculator<TestEntityClass> calculator =
                new CloudCostCalculator<>(dataProvider, topology, infoExtractor);

        final TestEntityClass testEntity = mock(TestEntityClass.class);

        when(infoExtractor.getEntityType(testEntity)).thenReturn(EntityType.VIRTUAL_MACHINE_VALUE);
        when(infoExtractor.getId(testEntity)).thenReturn(entityId);
        when(infoExtractor.getComputeConfig(testEntity))
            .thenReturn(Optional.of(new EntityInfoExtractor.ComputeConfig(OSType.SUSE, Tenancy.DEFAULT)));

        final TestEntityClass region = mock(TestEntityClass.class);
        when(infoExtractor.getId(region)).thenReturn(regionId);

        final TestEntityClass computeTier = mock(TestEntityClass.class);
        when(infoExtractor.getId(computeTier)).thenReturn(computeTierId);

        when(topology.getRegion(entityId)).thenReturn(Optional.of(region));
        when(topology.getComputeTier(entityId)).thenReturn(Optional.of(computeTier));


        // act
        final CostJournal<TestEntityClass> journal = calculator.calculateCost(testEntity);

        // assert
        assertThat(journal.getTotalCost(), is(basePrice + suseAdjustment));
        assertThat(journal.getCostForCategory(CostCategory.COMPUTE), is(basePrice));
        assertThat(journal.getCostForCategory(CostCategory.LICENSE), is(suseAdjustment));
    }

    public static class TestEntityClass {
    }
}
