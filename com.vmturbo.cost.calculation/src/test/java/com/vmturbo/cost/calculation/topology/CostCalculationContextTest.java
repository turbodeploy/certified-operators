package com.vmturbo.cost.calculation.topology;

import static org.mockito.Mockito.mock;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.cost.Pricing.OnDemandPriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.SpotInstancePriceTable;
import com.vmturbo.common.protobuf.cost.Pricing.SpotInstancePriceTable.PriceForGuestOsType;
import com.vmturbo.common.protobuf.cost.Pricing.SpotInstancePriceTable.SpotPricesForTier;
import com.vmturbo.cost.calculation.CostCalculationContext;
import com.vmturbo.cost.calculation.CostJournal;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.TestEntityClass;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.StorageTierPriceList.StorageTierPrice;

/**
 * Test class to test the Cost Calculation Context.
 */
public class CostCalculationContextTest {

    private PriceTable priceTable;

    private AccountPricingData accountPricingData;

    private DiscountApplicator discountApplicator = mock(DiscountApplicator.class);

    private static final EntityInfoExtractor<TestEntityClass> infoExtractor =
            (EntityInfoExtractor<TestEntityClass>)mock(EntityInfoExtractor.class);

    private CostJournal.Builder costJournal = mock(CostJournal.Builder.class);

    private static final long REGION_ID = 1;
    private static final long BUSINESS_ACCOUNT_ID = 2;
    private static final double BASE_PRICE = 10.0;
    private static final long STORAGE_TIER_ID = 10;
    private static final long COMPUTE_TIER_ID = 11;
    private static final int IOPS_RANGE = 11;
    private static final double IOPS_PRICE_RANGE_1 = 16.0; // price per GB within GB_RANGE
    private static final int GB_RANGE = 11;
    private static final double GB_PRICE_RANGE_1 = 13.0; // price per GB within GB_RANGE
    private static final double IOPS_PRICE = 4.5; // price per GB above the range
    private static final double GB_MONTH_PRICE_10 = 14.0;
    private static final double GB_MONTH_PRICE_20 = 26.0;
    private static final double GB_PRICE = 9.0; // price per GB above the ra

    /**
     * Setup the test.
     */
    @Before
    public void setup() {
         priceTable = buildPriceTable();
        accountPricingData = new AccountPricingData(discountApplicator, priceTable, BUSINESS_ACCOUNT_ID);
    }

    /**
     * Test the cost calculation context.
     */
    @Test
    public void testCostCalculationContext() {
        TestEntityClass testEntity = TestEntityClass.newBuilder(10)
                .setType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setComputeConfig(new EntityInfoExtractor.ComputeConfig(OSType.WINDOWS, Tenancy.DEFAULT,
                        VMBillingType.ONDEMAND, 4, EntityDTO.LicenseModel.AHUB))
                .build(infoExtractor);
        Optional<OnDemandPriceTable> onDemandPriceTable = Optional.of(accountPricingData.getPriceTable()
                .getOnDemandPriceByRegionIdMap().get(REGION_ID));
        Optional<SpotInstancePriceTable> spotInstancePriceTable = Optional.of(accountPricingData
                .getPriceTable().getSpotPriceByZoneOrRegionIdMap().get(REGION_ID));
        CostCalculationContext context = new CostCalculationContext(costJournal, testEntity, REGION_ID,
                accountPricingData, onDemandPriceTable, spotInstancePriceTable);
        assert (context.getEntity().equals(testEntity));

        assert (context.getAccountPricingData().equals(accountPricingData));
        assert (context.getSpotInstancePriceTable().equals(spotInstancePriceTable));

        assert (context.getOnDemandPriceTable().equals(onDemandPriceTable));
    }

    private static PriceTable buildPriceTable() {
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
                                                * CostProtoUtil.HOURS_IN_MONTH)).build())
                                        .build()).build())
                                .putSpotPriceByZoneOrRegionId(REGION_ID, SpotInstancePriceTable.newBuilder()
                                        .putSpotPricesByTierOid(COMPUTE_TIER_ID, SpotPricesForTier
                                                .newBuilder()
                                                .addPriceForGuestOsType(PriceForGuestOsType.newBuilder()
                                                        .setGuestOsType(OSType.LINUX)
                                                        .setPrice(price(Unit.HOURS, BASE_PRICE)))
                                                .build())
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

    private static Price price(Price.Unit unit, double amount) {
        return Price.newBuilder()
                .setUnit(unit)
                .setPriceAmount(CurrencyAmount.newBuilder()
                        .setAmount(amount)
                        .build())
                .build();
    }
}
