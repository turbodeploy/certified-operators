package com.vmturbo.cost.calculation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.cost.calculation.CostJournal.JournalEntry;
import com.vmturbo.cost.calculation.CostJournal.OnDemandJournalEntry;
import com.vmturbo.cost.calculation.CostJournal.RIJournalEntry;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;

public class CostJournalTest {

    private EntityInfoExtractor<TestEntityClass> infoExtractor =
            (EntityInfoExtractor<TestEntityClass>)mock(EntityInfoExtractor.class);

    @Test
    public void testOnDemandJournalEntryCostWithDiscount() {
        final Price price = Price.newBuilder()
                .setUnit(Unit.HOURS)
                .setPriceAmount(CurrencyAmount.newBuilder()
                        .setAmount(100))
                .build();
        final TestEntityClass entity = TestEntityClass.newBuilder(7L)
                .build(infoExtractor);
        final JournalEntry<TestEntityClass> entry = new OnDemandJournalEntry<>(entity, price, 1);
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(entity)).thenReturn(0.5);
        CurrencyAmount cost = entry.calculateHourlyCost(infoExtractor, discountApplicator);
        assertThat(cost.getAmount(), closeTo(50, 0.001));
        assertThat(cost.getCurrency(), is(price.getPriceAmount().getCurrency()));
    }

    @Test
    public void testOnDemandJournalEntryCostNoDiscount() {
        final Price price = Price.newBuilder()
                .setUnit(Unit.HOURS)
                .setPriceAmount(CurrencyAmount.newBuilder()
                        .setAmount(100))
                .build();
        final TestEntityClass entity = TestEntityClass.newBuilder(7L)
                .build(infoExtractor);
        final JournalEntry<TestEntityClass> entry = new OnDemandJournalEntry<>(entity, price, 1);
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(entity)).thenReturn(0.0);
        CurrencyAmount cost = entry.calculateHourlyCost(infoExtractor, discountApplicator);
        assertThat(cost.getAmount(), closeTo(100, 0.001));
        assertThat(cost.getCurrency(), is(price.getPriceAmount().getCurrency()));
    }

    @Test
    public void testRIJournalEntryCostWithDiscount() {
        final long tierId = 7L;
        final double hourlyCost = 10;
        final int currency = 1;
        final ReservedInstanceData riData = new ReservedInstanceData(
                ReservedInstanceBought.getDefaultInstance(),
                ReservedInstanceSpec.newBuilder()
                    .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                        .setTierId(tierId))
                .build());
        final JournalEntry<TestEntityClass> entry = new RIJournalEntry<>(riData,
                CurrencyAmount.newBuilder()
                    .setAmount(hourlyCost)
                    .setCurrency(currency)
                    .build());
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(tierId)).thenReturn(0.1);
        final CurrencyAmount finalCost = entry.calculateHourlyCost(infoExtractor, discountApplicator);
        assertThat(finalCost.getAmount(), closeTo(9, 0.0001));
        assertThat(finalCost.getCurrency(), is(currency));
    }

    @Test
    public void testRIJournalEntryCostNoDiscount() {
        final long tierId = 7L;
        final double hourlyCost = 10;
        final int currency = 1;
        final ReservedInstanceData riData = new ReservedInstanceData(
                ReservedInstanceBought.getDefaultInstance(),
                ReservedInstanceSpec.newBuilder()
                        .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                                .setTierId(tierId))
                        .build());
        final JournalEntry<TestEntityClass> entry = new RIJournalEntry<>(riData,
                CurrencyAmount.newBuilder()
                        .setAmount(hourlyCost)
                        .setCurrency(currency)
                        .build());
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(tierId)).thenReturn(0.0);
        final CurrencyAmount finalCost = entry.calculateHourlyCost(infoExtractor, discountApplicator);
        assertThat(finalCost.getAmount(), is(hourlyCost));
        assertThat(finalCost.getCurrency(), is(currency));
    }

    @Test
    public void testCostJournal() {
        final Price computePrice = Price.newBuilder()
                .setUnit(Unit.HOURS)
                .setPriceAmount(CurrencyAmount.newBuilder()
                        .setAmount(100))
                .build();
        final Price licensePrice = Price.newBuilder()
                .setUnit(Unit.HOURS)
                .setPriceAmount(CurrencyAmount.newBuilder()
                        .setAmount(10))
                .build();
        final TestEntityClass entity = TestEntityClass.newBuilder(7).build(infoExtractor);
        final TestEntityClass region = TestEntityClass.newBuilder(77).build(infoExtractor);

        final TestEntityClass payee = TestEntityClass.newBuilder(123).build(infoExtractor);
        final ReservedInstanceData riData = new ReservedInstanceData(
                ReservedInstanceBought.getDefaultInstance(),
                ReservedInstanceSpec.newBuilder()
                        .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                                .setTierId(payee.getId()))
                        .build());
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        final CostJournal<TestEntityClass> journal =
            CostJournal.newBuilder(entity, infoExtractor, region, discountApplicator)
                .recordOnDemandCost(CostCategory.COMPUTE, payee, computePrice, 1)
                .recordOnDemandCost(CostCategory.LICENSE, payee, licensePrice, 1)
                .recordRiCost(CostCategory.COMPUTE, riData, CurrencyAmount.newBuilder()
                    .setAmount(25.0)
                    .build())
                .build();

        assertThat(journal.getTotalHourlyCost(), is(135.0));
        assertThat(journal.getEntity(), is(entity));
        assertThat(journal.getHourlyCostForCategory(CostCategory.COMPUTE), is(125.0));
        assertThat(journal.getHourlyCostForCategory(CostCategory.LICENSE), is(10.0));
    }
}
