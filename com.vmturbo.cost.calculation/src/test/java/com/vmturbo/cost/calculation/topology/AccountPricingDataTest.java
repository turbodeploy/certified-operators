package com.vmturbo.cost.calculation.topology;

import static org.hamcrest.Matchers.closeTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.LicensePriceTuple;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.ComputeTierConfig;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO.ComputeTierPriceList;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry;
import com.vmturbo.platform.sdk.common.PricingDTO.LicensePriceEntry.LicensePrice;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;

/**
 * Tests for AccountPricingData.
 */
public class AccountPricingDataTest {

    private static final double TOLERANCE = 1e-6;

    /**
     *  Verify that AccountPricingData.OS_TO_BASE_OS covers all OSType values.
     */
    @Test
    public void testOsToBaseOsMap() {
        for (OSType os : OSType.values()) {
            assertNotNull(AccountPricingData.OS_TO_BASE_OS.get(os));
        }
    }

    /**
     * Test explicit license pricing with a per-core rate.
     */
    @Test
    public void testExplicitPerCoreLicenseRate() {

        // set up the price table
        final PriceTable priceTable = PriceTable.newBuilder()
                .addOnDemandLicensePrices(LicensePriceEntry.newBuilder()
                        .setOsType(OSType.WINDOWS)
                        .addLicensePrices(LicensePrice.newBuilder()
                                .setIsPerCoreRate(true)
                                .setNumberOfCores(Integer.MAX_VALUE)
                                .setPrice(Price.newBuilder()
                                        .setPriceAmount(CurrencyAmount.newBuilder()
                                                .setAmount(.1)))))
                .build();

        final AccountPricingData accountPricingData = new AccountPricingData(
                DiscountApplicator.noDiscount(),
                priceTable,
                1L, 2, 3);

        final ComputeTierConfig tierConfig = ComputeTierConfig.builder()
                .computeTierOid(123)
                .numCoupons(0)
                .numCores(3)
                .isBurstableCPU(false)
                .build();

        final ComputeTierPriceList computePriceList = ComputeTierPriceList.newBuilder().build();

        final LicensePriceTuple priceTuple = accountPricingData.getLicensePrice(
                tierConfig, OSType.WINDOWS, computePriceList);

        assertThat(priceTuple.getExplicitOnDemandLicensePrice(), closeTo(.3, TOLERANCE));
    }
}
