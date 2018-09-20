package com.vmturbo.cost.calculation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.vmturbo.cost.calculation.CostJournal.JournalEntry;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;

public class CostJournalTest {

    private EntityInfoExtractor<TestEntityClass> infoExtractor =
            (EntityInfoExtractor<TestEntityClass>)mock(EntityInfoExtractor.class);

    @Test
    public void testJournalEntryCostWithDiscount() {
        final Price price = Price.newBuilder()
                .setUnit(Unit.HOURS)
                .setPriceAmount(CurrencyAmount.newBuilder()
                        .setAmount(100))
                .build();
        final TestEntityClass entity = TestEntityClass.newBuilder(7L)
                .build(infoExtractor);
        final JournalEntry<TestEntityClass> entry = new JournalEntry<>(entity, price, 1);
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(entity)).thenReturn(0.5);
        double cost = entry.calculateHourlyCost(infoExtractor, discountApplicator);
        assertThat(cost, closeTo(50, 0.001));
    }

    @Test
    public void testJournalEntryCostNoDiscount() {
        final Price price = Price.newBuilder()
                .setUnit(Unit.HOURS)
                .setPriceAmount(CurrencyAmount.newBuilder()
                        .setAmount(100))
                .build();
        final TestEntityClass entity = TestEntityClass.newBuilder(7L)
                .build(infoExtractor);
        final JournalEntry<TestEntityClass> entry = new JournalEntry<>(entity, price, 1);
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(entity)).thenReturn(0.0);
        double cost = entry.calculateHourlyCost(infoExtractor, discountApplicator);
        assertThat(cost, closeTo(100, 0.001));
    }
}
