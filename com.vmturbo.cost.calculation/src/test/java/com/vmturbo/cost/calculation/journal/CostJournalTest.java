package com.vmturbo.cost.calculation.journal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.CostProtoUtil;
import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.CostSource;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.cost.calculation.CloudCostCalculator.DependentCostLookup;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.TestEntityClass;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.journal.CostItem.CostSourceLink;
import com.vmturbo.cost.calculation.journal.CostJournal.CostSourceFilter;
import com.vmturbo.cost.calculation.journal.CostJournal.RateExtractor;
import com.vmturbo.cost.calculation.journal.entry.OnDemandJournalEntry;
import com.vmturbo.cost.calculation.journal.entry.QualifiedJournalEntry;
import com.vmturbo.cost.calculation.journal.entry.RIDiscountJournalEntry;
import com.vmturbo.cost.calculation.journal.entry.RIJournalEntry;
import com.vmturbo.cost.calculation.journal.entry.ReservedLicenseJournalEntry;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.trax.Trax;
import com.vmturbo.trax.TraxNumber;

/**
 * Unit test for {@link CostJournal}.
 */
public class CostJournalTest {

    private final EntityInfoExtractor<TestEntityClass> infoExtractor =
            Mockito.mock(EntityInfoExtractor.class);
    private static final double VALID_DELTA = 1e-5;
    private static final double TOTAL_PRICE = 100;
    private static final double TOTAL_RESERVED_LICENSE_PRICE = 0.5;
    private static final double HOURS_IN_DAY = 24;
    private static final double PRICE_AMOUNT_PER_DAYS_NO_DISCOUNT = TOTAL_PRICE / HOURS_IN_DAY;

    private final DiscountApplicator<TestEntityClass> discountApplicator =
            Mockito.mock(DiscountApplicator.class);
    private final RateExtractor rateExtractor = Mockito.mock(RateExtractor.class);


    /**
     * Setup for {@link CostJournalTest}.
     */
    @Before
    public void setup() {
        Mockito.when(discountApplicator.getDiscountPercentage(
                any(TestEntityClass.class))).thenReturn(Trax.trax(0));
        Mockito.when(discountApplicator.getDiscountPercentage(org.mockito.Matchers.anyLong()))
                .thenReturn(Trax.trax(0));
    }

    /**
     * Testing cost calculation for an entry with price unit of days and no discount.
     */
    @Test
    public void testDBJournalEntryCostWithNoDiscount() {
        final Price price = createPrice(Unit.DAYS, TOTAL_PRICE);
        final TestEntityClass entity = TestEntityClass.newBuilder(7L).build(infoExtractor);
        final QualifiedJournalEntry<TestEntityClass> entry =
                new OnDemandJournalEntry<>(entity, price, Trax.trax(1),
                        CostCategory.ON_DEMAND_COMPUTE, Optional.of(CostSource.ON_DEMAND_RATE));
        final RateExtractor rateExtractor = Mockito.mock(RateExtractor.class);
        final TraxNumber cost =
                entry.calculateHourlyCost(infoExtractor, discountApplicator, rateExtractor)
                        .stream()
                        .map(CostItem::cost)
                        .reduce(Trax.trax(0), (t1, t2) -> t1.plus(t2).compute());
        assertThat(cost.getValue(),
                closeTo(PRICE_AMOUNT_PER_DAYS_NO_DISCOUNT, VALID_DELTA));
    }

    /**
     * Test on-demand with discount.
     */
    @Test
    public void testOnDemandJournalEntryCostWithDiscount() {
        final Price price = createPrice(Unit.HOURS, TOTAL_PRICE);
        final TestEntityClass entity = TestEntityClass.newBuilder(7L).build(infoExtractor);
        final QualifiedJournalEntry<TestEntityClass> entry =
                new OnDemandJournalEntry<>(entity, price, Trax.trax(1),
                        CostCategory.ON_DEMAND_COMPUTE, Optional.of(CostSource.ON_DEMAND_RATE));
        Mockito.when(discountApplicator.getDiscountPercentage(entity)).thenReturn(Trax.trax(0.5));
        //TODO fix this
        final TraxNumber cost =
                entry.calculateHourlyCost(infoExtractor, discountApplicator, rateExtractor)
                        .stream()
                        .map(CostItem::cost)
                        .reduce(Trax.trax(0), (t1, t2) -> t1.plus(t2).compute());
        assertThat(cost.getValue(), closeTo(50, VALID_DELTA));
    }

    /**
     * Test on-demand monthly with discount.
     */
    @Test
    public void testOnDemandJournalMonthlyEntryCostWithDiscount() {
        final Price price = createPrice(Unit.MONTH, TOTAL_PRICE * CostProtoUtil.HOURS_IN_MONTH);
        final TestEntityClass entity = TestEntityClass.newBuilder(7L).build(infoExtractor);
        final QualifiedJournalEntry<TestEntityClass> entry =
                new OnDemandJournalEntry<>(entity, price, Trax.trax(1),
                        CostCategory.ON_DEMAND_COMPUTE, Optional.of(CostSource.ON_DEMAND_RATE));
        Mockito.when(discountApplicator.getDiscountPercentage(entity)).thenReturn(Trax.trax(0.5));
        final TraxNumber cost =
                entry.calculateHourlyCost(infoExtractor, discountApplicator, rateExtractor)
                        .stream()
                        .map(CostItem::cost)
                        .reduce(Trax.trax(0), (t1, t2) -> t1.plus(t2).compute());
        assertThat(cost.getValue(), closeTo(50, VALID_DELTA));
    }

    /**
     * Test on-demand monthly with discount. Units in GB.
     */
    @Test
    public void testOnDemandJournalGBMonthEntryCostWithDiscount() {
        final Price price = createPrice(Unit.GB_MONTH, TOTAL_PRICE * CostProtoUtil.HOURS_IN_MONTH);
        final TestEntityClass entity = TestEntityClass.newBuilder(7L).build(infoExtractor);
        final QualifiedJournalEntry<TestEntityClass> entry =
                new OnDemandJournalEntry<>(entity, price, Trax.trax(1),
                        CostCategory.ON_DEMAND_COMPUTE, Optional.of(CostSource.ON_DEMAND_RATE));
        Mockito.when(discountApplicator.getDiscountPercentage(entity)).thenReturn(Trax.trax(0.5));
        final TraxNumber cost =
                entry.calculateHourlyCost(infoExtractor, discountApplicator, rateExtractor)
                        .stream()
                        .map(CostItem::cost)
                        .reduce(Trax.trax(0), (t1, t2) -> t1.plus(t2).compute());
        assertThat(cost.getValue(), closeTo(50, VALID_DELTA));
    }

    /**
     * Test on-demand with discount. Units in million IOPS.
     */
    @Test
    public void testOnDemandJournalMillionIopsEntryCostWithDiscount() {
        final Price price =
                createPrice(Unit.MILLION_IOPS, TOTAL_PRICE * CostProtoUtil.HOURS_IN_MONTH);
        final TestEntityClass entity = TestEntityClass.newBuilder(7L).build(infoExtractor);
        final QualifiedJournalEntry<TestEntityClass> entry =
                new OnDemandJournalEntry<>(entity, price, Trax.trax(1),
                        CostCategory.ON_DEMAND_COMPUTE, Optional.of(CostSource.ON_DEMAND_RATE));
        Mockito.when(discountApplicator.getDiscountPercentage(entity)).thenReturn(Trax.trax(0.5));
        final TraxNumber cost =
                entry.calculateHourlyCost(infoExtractor, discountApplicator, rateExtractor)
                        .stream()
                        .map(CostItem::cost)
                        .reduce(Trax.trax(0), (t1, t2) -> t1.plus(t2).compute());
        assertThat(cost.getValue(), closeTo(50, VALID_DELTA));
    }

    /**
     * Test on-demand no discount.
     */
    @Test
    public void testOnDemandJournalEntryCostNoDiscount() {
        final Price price = createPrice(Unit.HOURS, TOTAL_PRICE);
        final TestEntityClass entity = TestEntityClass.newBuilder(7L).build(infoExtractor);
        final QualifiedJournalEntry<TestEntityClass> entry =
                new OnDemandJournalEntry<>(entity, price, Trax.trax(1),
                        CostCategory.ON_DEMAND_COMPUTE, Optional.of(CostSource.ON_DEMAND_RATE));
        final TraxNumber cost =
                entry.calculateHourlyCost(infoExtractor, discountApplicator, rateExtractor)
                        .stream()
                        .map(CostItem::cost)
                        .reduce(Trax.trax(0), (t1, t2) -> t1.plus(t2).compute());
        assertThat(cost.getValue(), closeTo(TOTAL_PRICE, VALID_DELTA));
    }

    /**
     * Test reserved instance with discount.
     */
    @Test
    public void testRIJournalEntryCostWithDiscount() {
        final long tierId = 7L;
        final double hourlyCost = 10;
        final ReservedInstanceData riData =
                new ReservedInstanceData(ReservedInstanceBought.getDefaultInstance(),
                        ReservedInstanceSpec.newBuilder()
                                .setReservedInstanceSpecInfo(
                                        ReservedInstanceSpecInfo.newBuilder().setTierId(tierId))
                                .build());
        final QualifiedJournalEntry<TestEntityClass> entry =
                new RIJournalEntry<>(riData, Trax.trax(1), Trax.trax(hourlyCost),
                        CostCategory.RI_COMPUTE);
        Mockito.when(discountApplicator.getDiscountPercentage(tierId)).thenReturn(Trax.trax(0.1));
        final TraxNumber finalCost =
                entry.calculateHourlyCost(infoExtractor, discountApplicator, rateExtractor)
                        .stream()
                        .map(CostItem::cost)
                        .reduce(Trax.trax(0), (t1, t2) -> t1.plus(t2).compute());
        assertThat(finalCost.getValue(), closeTo(9, VALID_DELTA));
    }

    /**
     * Test reserved instance no discount.
     */
    @Test
    public void testRIJournalEntryCostNoDiscount() {
        final long tierId = 7L;
        final double hourlyCost = 10;
        final ReservedInstanceData riData =
                new ReservedInstanceData(ReservedInstanceBought.getDefaultInstance(),
                        ReservedInstanceSpec.newBuilder()
                                .setReservedInstanceSpecInfo(
                                        ReservedInstanceSpecInfo.newBuilder().setTierId(tierId))
                                .build());
        final QualifiedJournalEntry<TestEntityClass> entry =
                new RIJournalEntry<>(riData, Trax.trax(1), Trax.trax(hourlyCost),
                        CostCategory.RI_COMPUTE);
        final TraxNumber finalCost =
                entry.calculateHourlyCost(infoExtractor, discountApplicator, rateExtractor)
                        .stream()
                        .map(CostItem::cost)
                        .reduce(Trax.trax(0), (t1, t2) -> t1.plus(t2).compute());
        assertThat(finalCost.getValue(), Matchers.is(hourlyCost));
    }

    /**
     * Test for {@link CostJournal}.
     */
    @Test
    public void testCostJournal() {
        final Price computePrice = createPrice(Unit.HOURS, TOTAL_PRICE);
        final Price licensePrice = createPrice(Unit.HOURS, TOTAL_PRICE / 10);
        final TestEntityClass entity = TestEntityClass.newBuilder(7).build(infoExtractor);
        final TestEntityClass region = TestEntityClass.newBuilder(77).build(infoExtractor);

        final TestEntityClass payee = TestEntityClass.newBuilder(123).build(infoExtractor);
        final ReservedInstanceData riData =
                new ReservedInstanceData(ReservedInstanceBought.getDefaultInstance(),
                        ReservedInstanceSpec.newBuilder()
                                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                                        .setTierId(payee.getId()))
                                .build());
        final CostJournal<TestEntityClass> journal =
                CostJournal.newBuilder(entity, infoExtractor, region, discountApplicator, e -> null)
                        .recordOnDemandCost(CostCategory.ON_DEMAND_COMPUTE, payee, computePrice,
                                Trax.trax(1))
                        .recordOnDemandCost(CostCategory.ON_DEMAND_LICENSE, payee, licensePrice,
                                Trax.trax(1))
                        .recordRiCost(riData, Trax.trax(1), Trax.trax(25))
                        .build();

        assertThat(journal.getTotalHourlyCost().getValue(), Matchers.is(135.0));
        assertThat(journal.getEntity(), Matchers.is(entity));
        assertThat(
                journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE).getValue(),
                Matchers.is(100.0));
        assertThat(
                journal.getHourlyCostForCategory(CostCategory.RI_COMPUTE).getValue(),
                Matchers.is(25.0));
        assertThat(
                journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_LICENSE).getValue(),
                Matchers.is(10.0));
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
        final TestEntityClass entity = TestEntityClass.newBuilder(7L).build(infoExtractor);
        final TestEntityClass region = TestEntityClass.newBuilder(77).build(infoExtractor);
        final TestEntityClass payee = TestEntityClass.newBuilder(123).build(infoExtractor);
        final ReservedInstanceData riData =
                new ReservedInstanceData(ReservedInstanceBought.getDefaultInstance(),
                        ReservedInstanceSpec.newBuilder()
                                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                                        .setTierId(payee.getId()))
                                .build());
        final CostJournal<TestEntityClass> journal =
                CostJournal.newBuilder(entity, infoExtractor, region, discountApplicator, e -> null)
                        .recordOnDemandCost(CostCategory.ON_DEMAND_COMPUTE, payee, computePrice,
                                Trax.trax(1))
                        .recordOnDemandCost(CostCategory.ON_DEMAND_LICENSE, payee, licensePrice,
                                Trax.trax(1))
                        .recordRiCost(riData, Trax.trax(1), Trax.trax(25))
                        .recordRIDiscount(CostCategory.ON_DEMAND_COMPUTE, riData, Trax.trax(0.25))
                        .build();
        final CostSourceFilter filter =
                (costSource -> costSource.equals(CostSource.ON_DEMAND_RATE));
        final TraxNumber ans =
                journal.getHourlyCostFilterEntries(CostCategory.ON_DEMAND_COMPUTE, filter);
        assertThat(ans.getValue(), Matchers.is(100.0));
        assertThat(
                journal.getFilteredCategoryCostsBySource(CostCategory.ON_DEMAND_COMPUTE, CostSourceFilter.INCLUDE_ALL)
                        .get(CostSource.RI_INVENTORY_DISCOUNT).getValue(), Matchers.is(-25.0));
        System.out.println(journal.toString());
        System.out.println(ans);
    }

    /**
     * Test inheritance journal entry.
     */
    @Test
    public void testCostJournalEntryInheritance() {
        final TestEntityClass entity = TestEntityClass.newBuilder(7).build(infoExtractor);
        final TestEntityClass region = TestEntityClass.newBuilder(77).build(infoExtractor);
        final Price price = createPrice(Unit.HOURS, TOTAL_PRICE);
        final TestEntityClass payee = TestEntityClass.newBuilder(123).build(infoExtractor);

        final TestEntityClass childCostProvider =
                TestEntityClass.newBuilder(123).build(infoExtractor);
        final CostJournal<TestEntityClass> childCostJournal =
                CostJournal.newBuilder(entity, infoExtractor, region, discountApplicator, e -> null)
                        // One cost category that is also present in the test entity.
                        .recordOnDemandCost(CostCategory.ON_DEMAND_COMPUTE, payee, price,
                                Trax.trax(1))
                        // One cost category that is NOT present in the test entity.
                        .recordOnDemandCost(CostCategory.STORAGE, payee, price, Trax.trax(1))
                        .build();
        final DependentCostLookup<TestEntityClass> dependentCostLookup = e -> {
            assertThat(e, Matchers.is(childCostProvider));
            return childCostJournal;
        };

        final CostJournal<TestEntityClass> journal =
                CostJournal.newBuilder(entity, infoExtractor, region, discountApplicator,
                        dependentCostLookup)
                        .recordOnDemandCost(CostCategory.ON_DEMAND_COMPUTE, payee, price,
                                Trax.trax(1))
                        .inheritCost(childCostProvider)
                        .build();

        assertThat(journal.getTotalHourlyCost().getValue(), Matchers.is(300.0));
        assertThat(journal.getEntity(), Matchers.is(entity));
        assertThat(
                journal.getHourlyCostForCategory(CostCategory.ON_DEMAND_COMPUTE).getValue(),
                Matchers.is(200.0));
        assertThat(journal.getHourlyCostForCategory(CostCategory.STORAGE).getValue(),
                Matchers.is(100.0));
        System.out.println(journal.toString());
    }

    /**
     * Test buy reserved instance discount.
     */
    @Test
    public void testBuyRIDiscountJournalEntry() {
        final Price computePrice = createPrice(Unit.HOURS, TOTAL_PRICE);
        final Price licensePrice = createPrice(Unit.HOURS, TOTAL_PRICE / 10);
        final TestEntityClass entity = TestEntityClass.newBuilder(7L).build(infoExtractor);
        final TestEntityClass region = TestEntityClass.newBuilder(77).build(infoExtractor);
        final TestEntityClass payee = TestEntityClass.newBuilder(123).build(infoExtractor);
        final ReservedInstanceData riData =
                new ReservedInstanceData(ReservedInstanceBought.getDefaultInstance(),
                        ReservedInstanceSpec.newBuilder()
                                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                                        .setTierId(payee.getId()))
                                .build());
        final CostJournal<TestEntityClass> journal =
                CostJournal.newBuilder(entity, infoExtractor, region, discountApplicator, e -> null)
                        .recordOnDemandCost(CostCategory.ON_DEMAND_COMPUTE, payee, computePrice,
                                Trax.trax(1))
                        .recordOnDemandCost(CostCategory.ON_DEMAND_LICENSE, payee, licensePrice,
                                Trax.trax(1))
                        .recordBuyRIDiscount(CostCategory.ON_DEMAND_COMPUTE, riData,
                                Trax.trax(0.25))
                        .build();
        final CostSourceFilter filter =
                (costSource -> costSource.equals(CostSource.ON_DEMAND_RATE));
        final TraxNumber ans =
                journal.getHourlyCostFilterEntries(CostCategory.ON_DEMAND_COMPUTE, filter);
        assertThat(ans.getValue(), Matchers.is(100.0));
        assertThat(
                journal.getFilteredCategoryCostsBySource(CostCategory.ON_DEMAND_COMPUTE, CostSourceFilter.INCLUDE_ALL)
                        .get(CostSource.BUY_RI_DISCOUNT).getValue(), Matchers.is(-25.0));
        System.out.println(journal.toString());
        System.out.println(ans);
    }

    /**
     * Reserved License Price = 0.5. 25% RI coverage. RI License price should be 0.5*0.25 = 0.125.
     */
    @Test
    public void testReservedLicenseJournal() {
        final long tierId = 7L;
        final TraxNumber riBoughtPercentage = Trax.trax(0.25);
        final TestEntityClass region = TestEntityClass.newBuilder(77).build(infoExtractor);
        final Price price = createPrice(Unit.HOURS, TOTAL_RESERVED_LICENSE_PRICE);
        final TestEntityClass entity = TestEntityClass.newBuilder(7L).build(infoExtractor);
        final ReservedInstanceData riData =
                new ReservedInstanceData(ReservedInstanceBought.getDefaultInstance(),
                        ReservedInstanceSpec.newBuilder()
                                .setReservedInstanceSpecInfo(
                                        ReservedInstanceSpecInfo.newBuilder().setTierId(tierId))
                                .build());
        final QualifiedJournalEntry<TestEntityClass> reservedLicenseEntry =
                new ReservedLicenseJournalEntry<>(price, riData, riBoughtPercentage,
                        CostCategory.RESERVED_LICENSE, Optional.of(CostSource.BUY_RI_DISCOUNT));
        final TraxNumber reservedLicensePrice =
                reservedLicenseEntry.calculateHourlyCost(infoExtractor, discountApplicator, rateExtractor)
                        .stream()
                        .map(CostItem::cost)
                        .reduce(Trax.trax(0), (t1, t2) -> t1.plus(t2).compute());
        assertThat(reservedLicensePrice.getValue(), Matchers.is(0.125));
        final CostJournal<TestEntityClass> journal =
                CostJournal.newBuilder(entity, infoExtractor, region, discountApplicator, e -> null)
                        .recordReservedLicenseCost(CostCategory.RESERVED_LICENSE, riData,
                                riBoughtPercentage, price, true)
                        .build();
        Map<CostSource, TraxNumber> filteredCategoryCostsBySource = journal.getFilteredCategoryCostsBySource(CostCategory.RESERVED_LICENSE, CostSourceFilter.INCLUDE_ALL);
        assertThat(
                filteredCategoryCostsBySource
                        .get(CostSource.BUY_RI_DISCOUNT).getValue(), Matchers.is(0.125));
        System.out.println(journal);
    }

    /**
     * Tests if discounted RI compute costs are being calculated correctly.
     */
    @Test
    public void testRIDiscountedRate() {
        final long tierId = 7L;
        final float discountPercent = 0.2f;
        final TraxNumber riBoughtPercentage = Trax.trax(1.0);
        final ReservedInstanceData riData =
                new ReservedInstanceData(ReservedInstanceBought.getDefaultInstance(),
                        ReservedInstanceSpec.newBuilder()
                                .setReservedInstanceSpecInfo(
                                        ReservedInstanceSpecInfo.newBuilder().setTierId(tierId))
                                .build());

        final TestEntityClass entity = TestEntityClass.newBuilder(7L).build(infoExtractor);
        final QualifiedJournalEntry<TestEntityClass> entry =
                new RIDiscountJournalEntry<>(riData, riBoughtPercentage,
                        CostCategory.ON_DEMAND_COMPUTE, CostSource.ON_DEMAND_RATE, false);
        Mockito.when(discountApplicator.getDiscountPercentage(entity))
                .thenReturn(Trax.trax(discountPercent));
        Mockito.when(discountApplicator.getDiscountPercentage(org.mockito.Matchers.anyLong()))
                .thenReturn(Trax.trax(discountPercent));
        Mockito.when(rateExtractor.lookupCostWithFilter(eq(CostCategory.ON_DEMAND_COMPUTE), any()))
                .thenReturn(Collections.singleton(CostItem.builder()
                        .cost(Trax.trax(TOTAL_PRICE))
                        .costSourceLink(CostSourceLink.of(CostSource.ON_DEMAND_RATE))
                        .build()));
        final double discountedCost = -TOTAL_PRICE;
        final TraxNumber cost =
                entry.calculateHourlyCost(infoExtractor, discountApplicator, rateExtractor)
                        .stream()
                        .map(CostItem::cost)
                        .reduce(Trax.trax(0), (t1, t2) -> t1.plus(t2).compute());
        assertThat(cost.getValue(), closeTo(discountedCost, VALID_DELTA));
    }

    /**
     * Test the cost calculation when an entity has both an uptime discount and buy RI discount.
     */
    @Test
    public void testBuyRIDiscountAndUptime() {

        final Price computePrice = createPrice(Unit.HOURS, TOTAL_PRICE);
        final TestEntityClass entity = TestEntityClass.newBuilder(7L).build(infoExtractor);
        final TestEntityClass region = TestEntityClass.newBuilder(77).build(infoExtractor);
        final TestEntityClass payee = TestEntityClass.newBuilder(123).build(infoExtractor);
        final ReservedInstanceData riData =
                new ReservedInstanceData(ReservedInstanceBought.getDefaultInstance(),
                        ReservedInstanceSpec.newBuilder()
                                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                                        .setTierId(payee.getId()))
                                .build());
        final CostJournal<TestEntityClass> journal =
                CostJournal.newBuilder(entity, infoExtractor, region, discountApplicator, e -> null)
                        .recordOnDemandCost(CostCategory.ON_DEMAND_COMPUTE, payee, computePrice,
                                Trax.trax(1))
                        .recordBuyRIDiscount(CostCategory.ON_DEMAND_COMPUTE, riData,
                                Trax.trax(.50))
                        // Uptime = 75% so 25% discount
                        .addUptimeDiscountToAllCategories(Trax.trax(.25))
                        .build();

        final TraxNumber computeCostMinusBuyRIDiscount =
                journal.getHourlyCostFilterEntries(
                        CostCategory.ON_DEMAND_COMPUTE,
                        CostSourceFilter.EXCLUDE_BUY_RI_DISCOUNT_FILTER);
        final TraxNumber uptimeDiscount = journal.getFilteredCategoryCostsBySource(
                CostCategory.ON_DEMAND_COMPUTE,
                CostSourceFilter.INCLUDE_ALL).get(CostSource.ENTITY_UPTIME_DISCOUNT);
        final TraxNumber onDemandRateExcludingUptime = journal.getFilteredCategoryCostsBySource(
                CostCategory.ON_DEMAND_COMPUTE,
                CostSourceFilter.EXCLUDE_UPTIME).get(CostSource.ON_DEMAND_RATE);
        final TraxNumber buyRIDiscount =
                journal.getFilteredCategoryCostsBySource(
                        CostCategory.ON_DEMAND_COMPUTE,
                        CostSourceFilter.INCLUDE_ALL).get(CostSource.BUY_RI_DISCOUNT);
        final TraxNumber buyRIDiscountExcludingUptime =
                journal.getFilteredCategoryCostsBySource(
                        CostCategory.ON_DEMAND_COMPUTE,
                        CostSourceFilter.EXCLUDE_UPTIME).get(CostSource.BUY_RI_DISCOUNT);

        assertThat(onDemandRateExcludingUptime.getValue(), closeTo(TOTAL_PRICE, VALID_DELTA));
        assertThat(uptimeDiscount.getValue(), closeTo(TOTAL_PRICE * -0.25, VALID_DELTA));
        assertThat(computeCostMinusBuyRIDiscount.getValue(), closeTo(TOTAL_PRICE * .75, VALID_DELTA));
        assertThat(buyRIDiscount.getValue(), closeTo(TOTAL_PRICE * .75 * -.5, VALID_DELTA));
        assertThat(buyRIDiscountExcludingUptime.getValue(), closeTo(TOTAL_PRICE * -.5, VALID_DELTA));
    }

    /**
     * Tests a reserved license from a BUY RI recommendation, along with entity uptime. The intent is to
     * verify the entity uptime discount on the BUY RI reserved license is also filtered when the
     * BUY RI cost source is filtered.
     */
    @Test
    public void testBuyRILicenseWithEntityUptime() {

        final TestEntityClass entity = TestEntityClass.newBuilder(7L).build(infoExtractor);
        final TestEntityClass region = TestEntityClass.newBuilder(77).build(infoExtractor);
        final TestEntityClass payee = TestEntityClass.newBuilder(123).build(infoExtractor);
        final ReservedInstanceData riData =
                new ReservedInstanceData(ReservedInstanceBought.getDefaultInstance(),
                        ReservedInstanceSpec.newBuilder()
                                .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                                        .setTierId(payee.getId()))
                                .build());
        final CostJournal<TestEntityClass> journal =
                CostJournal.newBuilder(entity, infoExtractor, region, discountApplicator, e -> null)
                        .recordReservedLicenseCost(CostCategory.RESERVED_LICENSE, riData,
                                Trax.trax(1), createPrice(Unit.HOURS, 2.0), true)
                        // Uptime = 75% so 25% discount
                        .addUptimeDiscountToAllCategories(Trax.trax(.25))
                        .build();


        final TraxNumber licenseCost = journal.getHourlyCostForCategory(CostCategory.RESERVED_LICENSE);
        final TraxNumber licenseCostMinusBuyRI =
                journal.getHourlyCostFilterEntries(
                        CostCategory.RESERVED_LICENSE,
                        CostSourceFilter.EXCLUDE_BUY_RI_DISCOUNT_FILTER);
        final TraxNumber licenseCostMinusUptime = journal.getFilteredCategoryCostsBySource(
                CostCategory.RESERVED_LICENSE,
                CostSourceFilter.EXCLUDE_UPTIME
        ).get(CostSource.BUY_RI_DISCOUNT);

        // $2 with a 25% discount = $1.5
        assertThat(licenseCost.getValue(), closeTo(1.5, VALID_DELTA));
        // ignoring uptime, should be discounted for the full price
        assertThat(licenseCostMinusUptime.getValue(), closeTo(2.0, VALID_DELTA));
        // ignoring the buy RI discount, the license cost should be 0
        assertThat(licenseCostMinusBuyRI.getValue(), closeTo(0, VALID_DELTA));
    }

    private Price createPrice(final Unit timeUnit, final double amount) {
        return Price.newBuilder()
                .setUnit(timeUnit)
                .setPriceAmount(CurrencyAmount.newBuilder().setAmount(amount))
                .build();
    }



}
