package com.vmturbo.cost.calculation;

import static com.vmturbo.trax.Trax.trax;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import java.util.Optional;

import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.cost.calculation.CloudCostCalculator.DependentCostLookup;
import com.vmturbo.cost.calculation.CostJournal.CostSourceFilter;
import com.vmturbo.cost.calculation.CostJournal.JournalEntry;
import com.vmturbo.cost.calculation.CostJournal.OnDemandJournalEntry;
import com.vmturbo.cost.calculation.CostJournal.RIDiscountJournalEntry;
import com.vmturbo.cost.calculation.CostJournal.RIJournalEntry;
import com.vmturbo.cost.calculation.CostJournal.RateExtractor;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.trax.Trax;
import com.vmturbo.trax.TraxNumber;

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
        final JournalEntry<TestEntityClass> entry = new OnDemandJournalEntry<>(entity, price, trax(1), Optional.of(CostSource.ON_DEMAND_RATE));
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(entity)).thenReturn(trax(0.0));
        RateExtractor rateExtractor = mock(RateExtractor.class);
        TraxNumber cost = entry.calculateHourlyCost(infoExtractor, discountApplicator, rateExtractor);
        assertThat(cost.getValue(), closeTo(PRICE_AMOUNT_PER_DAYS_NO_DISCOUNT, VALID_DELTA));
    }

    @Test
    public void testOnDemandJournalEntryCostWithDiscount() {
        final Price price = createPrice(Unit.HOURS, TOTAL_PRICE);
        final TestEntityClass entity = TestEntityClass.newBuilder(7L)
                .build(infoExtractor);
        final JournalEntry<TestEntityClass> entry = new OnDemandJournalEntry<>(entity, price, trax(1), Optional.of(CostSource.ON_DEMAND_RATE));
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(entity)).thenReturn(trax(0.5));
        //TODO fix this
        RateExtractor rateExtractor = mock(RateExtractor.class);
        TraxNumber cost = entry.calculateHourlyCost(infoExtractor, discountApplicator, rateExtractor);
        assertThat(cost.getValue(), closeTo(50, VALID_DELTA));
    }

    @Test
    public void testOnDemandJournalMonthlyEntryCostWithDiscount() {
        final Price price = createPrice(Unit.MONTH, TOTAL_PRICE * CostProtoUtil.HOURS_IN_MONTH);
        final TestEntityClass entity = TestEntityClass.newBuilder(7L)
                .build(infoExtractor);
        final JournalEntry<TestEntityClass> entry = new OnDemandJournalEntry<>(entity, price, trax(1), Optional.of(CostSource.ON_DEMAND_RATE));
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(entity)).thenReturn(trax(0.5));
        RateExtractor rateExtractor = mock(RateExtractor.class);
        TraxNumber cost = entry.calculateHourlyCost(infoExtractor, discountApplicator,rateExtractor);
        assertThat(cost.getValue(), closeTo(50, VALID_DELTA));
    }

    @Test
    public void testOnDemandJournalGBMonthEntryCostWithDiscount() {
        final Price price = createPrice(Unit.GB_MONTH, TOTAL_PRICE * CostProtoUtil.HOURS_IN_MONTH);
        final TestEntityClass entity = TestEntityClass.newBuilder(7L)
                .build(infoExtractor);
        final JournalEntry<TestEntityClass> entry = new OnDemandJournalEntry<>(entity, price, trax(1), Optional.of(CostSource.ON_DEMAND_RATE));
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(entity)).thenReturn(trax(0.5));
        RateExtractor rateExtractor = mock(RateExtractor.class);
        TraxNumber cost = entry.calculateHourlyCost(infoExtractor, discountApplicator, rateExtractor);
        assertThat(cost.getValue(), closeTo(50, VALID_DELTA));
    }

    @Test
    public void testOnDemandJournalMillionIopsEntryCostWithDiscount() {
        final Price price = createPrice(Unit.MILLION_IOPS, TOTAL_PRICE * CostProtoUtil.HOURS_IN_MONTH);
        final TestEntityClass entity = TestEntityClass.newBuilder(7L)
                .build(infoExtractor);
        final JournalEntry<TestEntityClass> entry = new OnDemandJournalEntry<>(entity, price, trax(1), Optional.of(CostSource.ON_DEMAND_RATE));
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(entity)).thenReturn(trax(0.5));
        RateExtractor rateExtractor = mock(RateExtractor.class);
        TraxNumber cost = entry.calculateHourlyCost(infoExtractor, discountApplicator, rateExtractor);
        assertThat(cost.getValue(), closeTo(50, VALID_DELTA));
    }

    @Test
    public void testOnDemandJournalEntryCostNoDiscount() {
        final Price price = createPrice(Unit.HOURS, TOTAL_PRICE);
        final TestEntityClass entity = TestEntityClass.newBuilder(7L)
                .build(infoExtractor);
        final JournalEntry<TestEntityClass> entry = new OnDemandJournalEntry<>(entity, price, trax(1), Optional.of(CostSource.ON_DEMAND_RATE));
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(entity)).thenReturn(trax(0.0));
        RateExtractor rateExtractor = mock(RateExtractor.class);
        TraxNumber cost = entry.calculateHourlyCost(infoExtractor, discountApplicator, rateExtractor);
        assertThat(cost.getValue(), closeTo(TOTAL_PRICE, VALID_DELTA));
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
                trax(1), trax(hourlyCost), null);
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(tierId)).thenReturn(trax(0.1));
        RateExtractor rateExtractor = mock(RateExtractor.class);
        final TraxNumber finalCost = entry.calculateHourlyCost(infoExtractor, discountApplicator, rateExtractor);
        assertThat(finalCost.getValue(), closeTo(9, VALID_DELTA));
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
                trax(1), trax(hourlyCost), null);
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(tierId)).thenReturn(trax(0.0));
        RateExtractor rateExtractor = mock(RateExtractor.class);
        final TraxNumber finalCost = entry.calculateHourlyCost(infoExtractor, discountApplicator, rateExtractor);
        assertThat(finalCost.getValue(), is(hourlyCost));
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
        when(discountApplicator.getDiscountPercentage(any(TestEntityClass.class))).thenReturn(trax(0));
        when(discountApplicator.getDiscountPercentage(anyLong())).thenReturn(trax(0));
        final CostJournal<TestEntityClass> journal =
                CostJournal.newBuilder(entity, infoExtractor, region, discountApplicator, e -> null)
                        .recordOnDemandCost(CostCategory.ON_DEMAND_COMPUTE, payee, computePrice, trax(1))
                        .recordOnDemandCost(CostCategory.ON_DEMAND_LICENSE, payee, licensePrice, trax(1))
                        .recordRiCost(riData, trax(1), trax(25))
                        .build();

        assertThat(journal.getTotalHourlyCost().getValue(), is(135.0));
        assertThat(journal.getEntity(), is(entity));
        assertThat(journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE).getValue(), is(100.0));
        assertThat(journal.getHourlyCostForCategory(CostCategory.RI_COMPUTE).getValue(), is(25.0));
        assertThat(journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_LICENSE).getValue(), is(10.0));
    }

    /**
     * On Demand rate = 100. RI covered percentage = 25%.
     * OnDemandCompute journal entry should look like:
     * ON_DEMAND_COMPUTE : ON_DEMAND_RATE:TraxNumber{value=100.0}
     * ON_DEMAND_COMPUTE : RI_INVENTORY_DISCOUNT:TraxNumber{value=-25.0}
     * Total On Demand Compute = 100 + (-25) = 75.
     */
   @Test
   public void testFilterByCostCategoryAndCostSource() {
       final Price computePrice = createPrice(Unit.HOURS, TOTAL_PRICE);
       final Price licensePrice = createPrice(Unit.HOURS, TOTAL_PRICE / 10);
       final TestEntityClass entity = TestEntityClass.newBuilder(7L)
               .build(infoExtractor);
       final TestEntityClass region = TestEntityClass.newBuilder(77).build(infoExtractor);
       final TestEntityClass payee = TestEntityClass.newBuilder(123).build(infoExtractor);
       final ReservedInstanceData riData = new ReservedInstanceData(
               ReservedInstanceBought.getDefaultInstance(),
               ReservedInstanceSpec.newBuilder()
                       .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                               .setTierId(payee.getId()))
                       .build());
       final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
       when(discountApplicator.getDiscountPercentage(any(TestEntityClass.class))).thenReturn(trax(0));
       when(discountApplicator.getDiscountPercentage(anyLong())).thenReturn(trax(0));
       final CostJournal<TestEntityClass> journal =
               CostJournal.newBuilder(entity, infoExtractor, region, discountApplicator, e -> null)
                       .recordOnDemandCost(CostCategory.ON_DEMAND_COMPUTE, payee, computePrice, trax(1))
                       .recordOnDemandCost(CostCategory.ON_DEMAND_LICENSE, payee, licensePrice, trax(1))
                       .recordRiCost(riData, trax(1), trax(25))
                       .recordRIDiscount(CostCategory.ON_DEMAND_COMPUTE, riData, trax(0.25))
                       .build();
       CostSourceFilter filter = (costSource -> costSource.equals(CostSource.ON_DEMAND_RATE));
       TraxNumber ans = journal.getHourlyCostFilterEntries(CostCategory.ON_DEMAND_COMPUTE, filter);
       System.out.println(journal.toString());
       System.out.println(ans);
       assertThat(ans.getValue(), is(100.0));
       assertThat(journal.getHourlyCostBySourceAndCategory(CostCategory.ON_DEMAND_COMPUTE,
               CostSource.RI_INVENTORY_DISCOUNT).getValue(), is(-25.0));
   }

    @Test
    public void testCostJournalEntryInheritance() {
        final TestEntityClass entity = TestEntityClass.newBuilder(7).build(infoExtractor);
        final TestEntityClass region = TestEntityClass.newBuilder(77).build(infoExtractor);
        final Price price = createPrice(Unit.HOURS, TOTAL_PRICE);
        final TestEntityClass payee = TestEntityClass.newBuilder(123).build(infoExtractor);
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(any(TestEntityClass.class))).thenReturn(trax(0));

        final TestEntityClass childCostProvider = TestEntityClass.newBuilder(123).build(infoExtractor);
        final CostJournal<TestEntityClass> childCostJournal =
                CostJournal.newBuilder(entity, infoExtractor, region, discountApplicator, e -> null)
                        // One cost category that is also present in the test entity.
                        .recordOnDemandCost(CostCategory.ON_DEMAND_COMPUTE, payee, price, trax(1))
                        // One cost category that is NOT present in the test entity.
                        .recordOnDemandCost(CostCategory.STORAGE, payee, price, trax(1))
                        .build();
        final DependentCostLookup<TestEntityClass> dependentCostLookup = e -> {
            assertThat(e, is(childCostProvider));
            return childCostJournal;
        };

        final CostJournal<TestEntityClass> journal =
                CostJournal.newBuilder(entity, infoExtractor, region, discountApplicator, dependentCostLookup)
                        .recordOnDemandCost(CostCategory.ON_DEMAND_COMPUTE, payee, price, trax(1))
                        .inheritCost(childCostProvider)
                        .build();

        assertThat(journal.getTotalHourlyCost().getValue(), is(300.0));
        assertThat(journal.getEntity(), is(entity));
        assertThat(journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE).getValue(), is(200.0));
        assertThat(journal.getHourlyCostForCategory(CostCategory.STORAGE).getValue(), is(100.0));

        System.out.println(journal.toString());
    }

    @Test
    public void testBuyRIDiscountJournalEntry() {
        final Price computePrice = createPrice(Unit.HOURS, TOTAL_PRICE);
        final Price licensePrice = createPrice(Unit.HOURS, TOTAL_PRICE / 10);
        final TestEntityClass entity = TestEntityClass.newBuilder(7L)
                .build(infoExtractor);
        final TestEntityClass region = TestEntityClass.newBuilder(77).build(infoExtractor);
        final TestEntityClass payee = TestEntityClass.newBuilder(123).build(infoExtractor);
        final ReservedInstanceData riData = new ReservedInstanceData(
                ReservedInstanceBought.getDefaultInstance(),
                ReservedInstanceSpec.newBuilder()
                        .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                                .setTierId(payee.getId()))
                        .build());
        final DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);
        when(discountApplicator.getDiscountPercentage(any(TestEntityClass.class))).thenReturn(trax(0));
        when(discountApplicator.getDiscountPercentage(anyLong())).thenReturn(trax(0));
        final CostJournal<TestEntityClass> journal =
                CostJournal.newBuilder(entity, infoExtractor, region, discountApplicator, e -> null)
                        .recordOnDemandCost(CostCategory.ON_DEMAND_COMPUTE, payee, computePrice, trax(1))
                        .recordOnDemandCost(CostCategory.ON_DEMAND_LICENSE, payee, licensePrice, trax(1))
                        .recordBuyRIDiscount(CostCategory.ON_DEMAND_COMPUTE, riData, trax(0.25))
                        .build();
        CostSourceFilter filter = (costSource -> costSource.equals(CostSource.ON_DEMAND_RATE));
        TraxNumber ans = journal.getHourlyCostFilterEntries(CostCategory.ON_DEMAND_COMPUTE, filter);
        System.out.println(journal.toString());
        System.out.println(ans);
        assertThat(ans.getValue(), is(100.0));
        assertThat(journal.getHourlyCostBySourceAndCategory(CostCategory.ON_DEMAND_COMPUTE,
                CostSource.BUY_RI_DISCOUNT).getValue(), is(-25.0));
    }

    private Price createPrice(Unit timeUnit, double amount){
        return Price.newBuilder()
            .setUnit(timeUnit)
            .setPriceAmount(CurrencyAmount.newBuilder()
                .setAmount(amount))
            .build();
    }
}
