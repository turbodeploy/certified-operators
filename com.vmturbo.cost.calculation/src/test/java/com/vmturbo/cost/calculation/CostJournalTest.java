package com.vmturbo.cost.calculation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.cost.calculation.CloudCostCalculator.DependentCostLookup;
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
    private static final double VALID_DELTA = 1e-5;
    private static final double TOTAL_PRICE = 100;
    private static final double HOURS_IN_DAY = 24;
    private static final double PRICE_AMOUNT_PER_DAYS_NO_DISCOUNT = TOTAL_PRICE / HOURS_IN_DAY;

    /**
     * Testing cost calculation for an entry with price unit of days and no discount.
     */
    @Test
    public void testDBJournalEntryCostWithNoDiscount() {
        final Price price = createPrice(Unit.DAYS, TOTAL_PRICE);
        final TestEntityClass entity = TestEntityClass.newBuilder(7L)
            .build(infoExtractor);
        final JournalEntry<TestEntityClass> entry = new OnDemandJournalEntry<>(entity, price, 1);
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(entity)).thenReturn(0.0);
        CurrencyAmount cost = entry.calculateHourlyCost(infoExtractor, discountApplicator);
        assertThat(cost.getAmount(), closeTo(PRICE_AMOUNT_PER_DAYS_NO_DISCOUNT, VALID_DELTA));
        assertThat(cost.getCurrency(), is(price.getPriceAmount().getCurrency()));
    }

    @Test
    public void testOnDemandJournalEntryCostWithDiscount() {
        final Price price = createPrice(Unit.HOURS, TOTAL_PRICE);
        final TestEntityClass entity = TestEntityClass.newBuilder(7L)
                .build(infoExtractor);
        final JournalEntry<TestEntityClass> entry = new OnDemandJournalEntry<>(entity, price, 1);
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(entity)).thenReturn(0.5);
        CurrencyAmount cost = entry.calculateHourlyCost(infoExtractor, discountApplicator);
        assertThat(cost.getAmount(), closeTo(50, VALID_DELTA));
        assertThat(cost.getCurrency(), is(price.getPriceAmount().getCurrency()));
    }

    @Test
    public void testOnDemandJournalMonthlyEntryCostWithDiscount() {
        final Price price = createPrice(Unit.MONTH, TOTAL_PRICE * CostProtoUtil.HOURS_IN_MONTH);
        final TestEntityClass entity = TestEntityClass.newBuilder(7L)
                .build(infoExtractor);
        final JournalEntry<TestEntityClass> entry = new OnDemandJournalEntry<>(entity, price, 1);
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(entity)).thenReturn(0.5);
        CurrencyAmount cost = entry.calculateHourlyCost(infoExtractor, discountApplicator);
        assertThat(cost.getAmount(), closeTo(50, VALID_DELTA));
        assertThat(cost.getCurrency(), is(price.getPriceAmount().getCurrency()));
    }

    @Test
    public void testOnDemandJournalGBMonthEntryCostWithDiscount() {
        final Price price = createPrice(Unit.GB_MONTH, TOTAL_PRICE * CostProtoUtil.HOURS_IN_MONTH);
        final TestEntityClass entity = TestEntityClass.newBuilder(7L)
                .build(infoExtractor);
        final JournalEntry<TestEntityClass> entry = new OnDemandJournalEntry<>(entity, price, 1);
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(entity)).thenReturn(0.5);
        CurrencyAmount cost = entry.calculateHourlyCost(infoExtractor, discountApplicator);
        assertThat(cost.getAmount(), closeTo(50, VALID_DELTA));
        assertThat(cost.getCurrency(), is(price.getPriceAmount().getCurrency()));
    }

    @Test
    public void testOnDemandJournalMillionIopsEntryCostWithDiscount() {
        final Price price = createPrice(Unit.MILLION_IOPS, TOTAL_PRICE * CostProtoUtil.HOURS_IN_MONTH);
        final TestEntityClass entity = TestEntityClass.newBuilder(7L)
                .build(infoExtractor);
        final JournalEntry<TestEntityClass> entry = new OnDemandJournalEntry<>(entity, price, 1);
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(entity)).thenReturn(0.5);
        CurrencyAmount cost = entry.calculateHourlyCost(infoExtractor, discountApplicator);
        assertThat(cost.getAmount(), closeTo(50, VALID_DELTA));
        assertThat(cost.getCurrency(), is(price.getPriceAmount().getCurrency()));
    }

    @Test
    public void testOnDemandJournalEntryCostNoDiscount() {
        final Price price = createPrice(Unit.HOURS, TOTAL_PRICE);
        final TestEntityClass entity = TestEntityClass.newBuilder(7L)
                .build(infoExtractor);
        final JournalEntry<TestEntityClass> entry = new OnDemandJournalEntry<>(entity, price, 1);
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(entity)).thenReturn(0.0);
        CurrencyAmount cost = entry.calculateHourlyCost(infoExtractor, discountApplicator);
        assertThat(cost.getAmount(), closeTo(TOTAL_PRICE, VALID_DELTA));
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
                1,
                CurrencyAmount.newBuilder()
                    .setAmount(hourlyCost)
                    .setCurrency(currency)
                    .build());
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(tierId)).thenReturn(0.1);
        final CurrencyAmount finalCost = entry.calculateHourlyCost(infoExtractor, discountApplicator);
        assertThat(finalCost.getAmount(), closeTo(9, VALID_DELTA));
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
                1,
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
        final Price computePrice = createPrice(Unit.HOURS, TOTAL_PRICE);
        final Price licensePrice = createPrice(Unit.HOURS, TOTAL_PRICE / 10);
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
            CostJournal.newBuilder(entity, infoExtractor, region, discountApplicator, e -> null)
                .recordOnDemandCost(CostCategory.ON_DEMAND_COMPUTE, payee, computePrice, 1)
                .recordOnDemandCost(CostCategory.LICENSE, payee, licensePrice, 1)
                .recordRiCost(riData, 1, CurrencyAmount.newBuilder()
                    .setAmount(25.0)
                    .build())
                .build();

        assertThat(journal.getTotalHourlyCost(), is(135.0));
        assertThat(journal.getEntity(), is(entity));
        assertThat(journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE), is(100.0));
        assertThat(journal.getHourlyCostForCategory(CostCategory.RI_COMPUTE), is(25.0));
        assertThat(journal.getHourlyCostForCategory(CostCategory.LICENSE), is(10.0));
    }

    @Test
    public void testCostJournalEntryInheritance() {
        final TestEntityClass entity = TestEntityClass.newBuilder(7).build(infoExtractor);
        final TestEntityClass region = TestEntityClass.newBuilder(77).build(infoExtractor);
        final Price price = createPrice(Unit.HOURS, TOTAL_PRICE);
        final TestEntityClass payee = TestEntityClass.newBuilder(123).build(infoExtractor);
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);

        final TestEntityClass childCostProvider = TestEntityClass.newBuilder(123).build(infoExtractor);
        final CostJournal<TestEntityClass> childCostJournal =
                CostJournal.newBuilder(entity, infoExtractor, region, discountApplicator, e -> null)
                        // One cost category that is also present in the test entity.
                        .recordOnDemandCost(CostCategory.ON_DEMAND_COMPUTE, payee, price, 1)
                        // One cost category that is NOT present in the test entity.
                        .recordOnDemandCost(CostCategory.STORAGE, payee, price, 1)
                        .build();
        final DependentCostLookup<TestEntityClass> dependentCostLookup = e -> {
            assertThat(e, is(childCostProvider));
            return childCostJournal;
        };

        final CostJournal<TestEntityClass> journal =
                CostJournal.newBuilder(entity, infoExtractor, region, discountApplicator, dependentCostLookup)
                        .recordOnDemandCost(CostCategory.ON_DEMAND_COMPUTE, payee, price, 1)
                        .inheritCost(childCostProvider)
                        .build();

        assertThat(journal.getTotalHourlyCost(), is(300.0));
        assertThat(journal.getEntity(), is(entity));
        assertThat(journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE), is(200.0));
        assertThat(journal.getHourlyCostForCategory(CostCategory.STORAGE), is(100.0));

        System.out.println(journal.toString());
    }

    private Price createPrice(Unit timeUnit, double amount){
        return Price.newBuilder()
            .setUnit(timeUnit)
            .setPriceAmount(CurrencyAmount.newBuilder()
                .setAmount(amount))
            .build();
    }
}
