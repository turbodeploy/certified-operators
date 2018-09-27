package com.vmturbo.common.protobuf;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;

/**
 * Tests for {@link CostProtoUtil}.
 */
public class CostProtoUtilTest {

    @Test
    public void testTimeUnitsInTerm() {
        ReservedInstanceType type = ReservedInstanceType.newBuilder()
                .setTermYears(3)
                .build();

        assertThat(CostProtoUtil.timeUnitsInTerm(type, TimeUnit.DAYS), is(365L * 3));
        assertThat(CostProtoUtil.timeUnitsInTerm(type, TimeUnit.HOURS), is(365L * 3 * 24));
        assertThat(CostProtoUtil.timeUnitsInTerm(type, TimeUnit.MINUTES), is(365L * 3 * 24 * 60));
        assertThat(CostProtoUtil.timeUnitsInTerm(type, TimeUnit.SECONDS), is(365L * 3 * 24 * 60 * 60));
        assertThat(CostProtoUtil.timeUnitsInTerm(type, TimeUnit.MILLISECONDS), is(365L * 3 * 24 * 60 * 60 * 1000));
    }

    @Test
    public void testGetRiCurrencyDefault() {
        final CurrencyAmount currencyAmount = CurrencyAmount.getDefaultInstance();
        final ReservedInstanceBought riBought = ReservedInstanceBought.newBuilder()
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceBoughtCost(ReservedInstanceBoughtCost.newBuilder()
                                .setFixedCost(currencyAmount)
                                .setUsageCostPerHour(currencyAmount)
                                .setRecurringCostPerHour(currencyAmount)))
                .build();
        assertThat(CostProtoUtil.getRiCurrency(riBought), is(currencyAmount.getCurrency()));
    }

    @Test
    public void testGetRiCurrencyNonDefault() {
        final CurrencyAmount currencyAmount = CurrencyAmount.newBuilder()
                .setCurrency(123)
                .build();
        final ReservedInstanceBought riBought = ReservedInstanceBought.newBuilder()
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceBoughtCost(ReservedInstanceBoughtCost.newBuilder()
                                .setFixedCost(currencyAmount)
                                .setUsageCostPerHour(currencyAmount)
                                .setRecurringCostPerHour(currencyAmount)))
                .build();
        assertThat(CostProtoUtil.getRiCurrency(riBought), is(currencyAmount.getCurrency()));
    }

    @Test
    public void testGetRiCurrencyNonDefaultSomeMissing() {
        final CurrencyAmount currencyAmount = CurrencyAmount.newBuilder()
                .setCurrency(123)
                .build();
        final ReservedInstanceBought riBought = ReservedInstanceBought.newBuilder()
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceBoughtCost(ReservedInstanceBoughtCost.newBuilder()
                                // Missing fixed cost.
                                .setUsageCostPerHour(currencyAmount)
                                .setRecurringCostPerHour(currencyAmount)))
                .build();
        assertThat(CostProtoUtil.getRiCurrency(riBought), is(currencyAmount.getCurrency()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetRiCurrencyMultipleCurrencies() {
        final CurrencyAmount currencyAmount = CurrencyAmount.newBuilder()
                .setCurrency(123)
                .build();
        final ReservedInstanceBought riBought = ReservedInstanceBought.newBuilder()
                .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                        .setReservedInstanceBoughtCost(ReservedInstanceBoughtCost.newBuilder()
                                // Using different currency amount for fixed cost.
                                .setFixedCost(CurrencyAmount.getDefaultInstance())
                                .setUsageCostPerHour(currencyAmount)
                                .setRecurringCostPerHour(currencyAmount)))
                .build();
        CostProtoUtil.getRiCurrency(riBought);
    }
}
