package com.vmturbo.cost.calculation;

import static com.vmturbo.trax.Trax.trax;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.common.protobuf.cost.Cost.CostCategory;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCost;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.trax.Trax.TraxTopicConfiguration.Verbosity;
import com.vmturbo.cost.calculation.CloudCostCalculator.DependentCostLookup;
import com.vmturbo.cost.calculation.ReservedInstanceApplicator.ReservedInstanceApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.ComputeConfig;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.ComputeTierConfig;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.LicenseModel;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualMachineData.VMBillingType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.platform.sdk.common.PricingDTO.Price;
import com.vmturbo.platform.sdk.common.PricingDTO.Price.Unit;
import com.vmturbo.trax.Trax;
import com.vmturbo.trax.TraxConfiguration;
import com.vmturbo.trax.TraxConfiguration.TraxContext;
import com.vmturbo.trax.TraxNumber;

public class ReservedInstanceApplicatorTest {

    private CloudCostData cloudCostData = mock(CloudCostData.class);

    private EntityInfoExtractor<TestEntityClass> infoExtractor = mock(EntityInfoExtractor.class);

    private DiscountApplicator<TestEntityClass> discountApplicator = mock(DiscountApplicator.class);

    private DependentCostLookup<TestEntityClass> dependentCostLookup = mock(DependentCostLookup.class);

    private CostJournal.Builder<TestEntityClass> costJournal = mock(CostJournal.Builder.class);

    private ReservedInstanceApplicatorFactory<TestEntityClass> applicatorFactory =
            ReservedInstanceApplicator.newFactory();

    private final ComputeConfig computeConfig = new ComputeConfig(OSType.WINDOWS, Tenancy.DEFAULT,
            VMBillingType.RESERVED, 4, LicenseModel.LICENSE_INCLUDED);

    private static final int ENTITY_ID = 7;

    private static final int REGION_ID = 8;

    private static final int COMPUTE_TIER_ID = 17;

    private static final long RI_ID = 77;

    private static final double FULL_COVERAGE_PERCENTAGE = 1.0;
    private static final double NO_COVERAGE_PERCENTAGE = 0.0;

    // the number of cores is set to 0 because it doesn't affect this test
    private static final int DEFAULT_CORE_NUM = 0;

    private static final long RI_SPEC_ID = 777;

    private static final int TOTAL_COUPONS_REQUIRED = 10;

    private TestEntityClass computeTier;

    private static final boolean BURSTABLE_CPU = false;

    private static final ReservedInstanceBought RI_BOUGHT = ReservedInstanceBought.newBuilder()
            .setId(RI_ID)
            .setReservedInstanceBoughtInfo(ReservedInstanceBoughtInfo.newBuilder()
                    .setReservedInstanceSpec(RI_SPEC_ID)
                    .setNumBought(1)
                    .setReservedInstanceBoughtCost(ReservedInstanceBoughtCost.newBuilder()
                            .setFixedCost(CurrencyAmount.newBuilder()
                                    .setAmount(365 * 10 * 24))
                            .setRecurringCostPerHour(CurrencyAmount.newBuilder()
                                    .setAmount(10))
                            .setUsageCostPerHour(CurrencyAmount.newBuilder()
                                    .setAmount(10)))
                    .setReservedInstanceBoughtCoupons(ReservedInstanceBoughtCoupons.newBuilder()
                            .setNumberOfCoupons(TOTAL_COUPONS_REQUIRED)))
            .build();

    private static final ReservedInstanceSpec RI_SPEC = ReservedInstanceSpec.newBuilder()
            .setId(RI_SPEC_ID)
            .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                    .setType(ReservedInstanceType.newBuilder()
                            .setTermYears(1)))
            .build();

    // Mock Price which is easier to work with
    private static final Price price = Price.newBuilder().setUnit(Unit.HOURS)
            .setPriceAmount(CurrencyAmount.newBuilder().setAmount(10)).build();

    /**
     * Setup trax configuration.
     */
    @BeforeClass
    public static void setupTraxConfiguration() {
        // Use trax to make it easier to debug wrong values.
        TraxConfiguration.configureTopics(ReservedInstanceApplicatorTest.class.getName(), Verbosity.TRACE);
    }

    /**
     * Clean up Trax configuration.
     */
    @AfterClass
    public static void clearTraxConfiguration() {
        TraxConfiguration.clearAllConfiguration();
    }

    private TraxContext traxContext;

    /**
     * Create a {@link TraxContext}.
     */
    @Before
    public void setup() {
        traxContext = Trax.track(getClass().getName());
    }

    /**
     * Clean up the {@link TraxContext}.
     */
    @After
    public void teardown() {
        traxContext.close();
    }

    @Test
    public void testApplyRi() {
        Map<Long, EntityReservedInstanceCoverage> topologyRiCoverage = new HashMap<>();
        final TestEntityClass entity = TestEntityClass.newBuilder(ENTITY_ID)
                .build(infoExtractor);
        topologyRiCoverage.put(entity.getId(), EntityReservedInstanceCoverage.newBuilder()
                .putCouponsCoveredByRi(RI_ID, 5.0)
                .build());
        final TestEntityClass computeTier = TestEntityClass.newBuilder(COMPUTE_TIER_ID)
                .setComputeTierConfig(new ComputeTierConfig(TOTAL_COUPONS_REQUIRED, DEFAULT_CORE_NUM, BURSTABLE_CPU))
                .build(infoExtractor);
        final ReservedInstanceApplicator<TestEntityClass> applicator =
            applicatorFactory.newReservedInstanceApplicator(costJournal, infoExtractor,
                    cloudCostData, topologyRiCoverage);

        when(costJournal.getEntity()).thenReturn(entity);

        final ReservedInstanceData riData = new ReservedInstanceData(RI_BOUGHT, RI_SPEC);

        when(cloudCostData.getExistingRiBoughtData(RI_ID)).thenReturn(Optional.of(riData));

        TraxNumber coveredPercentage = applicator.recordRICoverage(computeTier, price, true);
        assertThat(coveredPercentage.getValue(), closeTo(0.5, 0.0001));

        // 10 + 10 + (3650 / (1 * 365 * 24)) = 30 <- hourly cost per instance
        // 30 / 10 = 3 <- hourly cost per coupon
        // 3 * 5 = 15 <- hourly cost for 5 coupons, which is how much apply to the entity.
        verify(costJournal).recordRiCost(eq(riData), argThat(traxEq(5.0)), argThat(traxApprox(15.0, 0.001)));
    }

    @Test
    public void testApplyRiCoverageMoreThanRequired() {
        Map<Long, EntityReservedInstanceCoverage> topologyRiCoverage = new HashMap<>();
        final TestEntityClass entity = TestEntityClass.newBuilder(ENTITY_ID)
                .build(infoExtractor);
        topologyRiCoverage.put(entity.getId(), EntityReservedInstanceCoverage.newBuilder()
                .putCouponsCoveredByRi(RI_ID, 100)
                .build());
        final TestEntityClass computeTier = TestEntityClass.newBuilder(COMPUTE_TIER_ID)
                .setComputeTierConfig(new ComputeTierConfig(TOTAL_COUPONS_REQUIRED, DEFAULT_CORE_NUM, BURSTABLE_CPU))
                .build(infoExtractor);
        final ReservedInstanceApplicator<TestEntityClass> applicator =
            applicatorFactory.newReservedInstanceApplicator(costJournal, infoExtractor,
                cloudCostData, topologyRiCoverage);
        when(costJournal.getEntity()).thenReturn(entity);

        final ReservedInstanceData riData = new ReservedInstanceData(RI_BOUGHT, RI_SPEC);

        when(cloudCostData.getExistingRiBoughtData(RI_ID)).thenReturn(Optional.of(riData));

        TraxNumber coveredPercentage = applicator.recordRICoverage(computeTier, price, true);
        assertThat(coveredPercentage.getValue(), is(FULL_COVERAGE_PERCENTAGE));

        final ArgumentCaptor<TraxNumber> amountCaptor = ArgumentCaptor.forClass(TraxNumber.class);
        verify(costJournal).recordRiCost(eq(riData), argThat(traxEq(100.0)), amountCaptor.capture());
        TraxNumber amount = amountCaptor.getValue();
        // 10 + 10 + (3650 / (1 * 365 * 24)) = 30 <- hourly cost per instance
        // 30 / 10 = 3 <- hourly cost per coupon
        // 3 * 10 = 30 <- hourly cost for 10 coupons, which is how much apply to the entity.
        //     The RI Bought only has 10 coupons, so we should only get the cost for those 10
        //     even though the coverage says there are 100.
        assertThat(amount.getValue(), closeTo(30.0, 0.001));
    }

    @Test
    public void testApplyRiNoCoverage() {
        Map<Long, EntityReservedInstanceCoverage> topologyRiCoverage = new HashMap<>();
        final ReservedInstanceApplicator<TestEntityClass> applicator =
                applicatorFactory.newReservedInstanceApplicator(costJournal, infoExtractor,
                        cloudCostData, topologyRiCoverage);
        final TestEntityClass entity = TestEntityClass.newBuilder(ENTITY_ID)
                .build(infoExtractor);
        final TestEntityClass computeTier = TestEntityClass.newBuilder(COMPUTE_TIER_ID)
                .setComputeTierConfig(new ComputeTierConfig(TOTAL_COUPONS_REQUIRED, DEFAULT_CORE_NUM, BURSTABLE_CPU))
                .build(infoExtractor);
        when(costJournal.getEntity()).thenReturn(entity);
        TraxNumber coveredPercentage = applicator.recordRICoverage(computeTier, price, true);
        assertThat(coveredPercentage.getValue(), is(NO_COVERAGE_PERCENTAGE));
        verify(costJournal, never()).recordRiCost(any(), any(), any());
    }

    @Test
    public void testApplyRiNoRequiredNoCoverage() {
        Map<Long, EntityReservedInstanceCoverage> topologyRiCoverage = new HashMap<>();
        final ReservedInstanceApplicator<TestEntityClass> applicator =
                applicatorFactory.newReservedInstanceApplicator(costJournal, infoExtractor,
                        cloudCostData, topologyRiCoverage);
        final TestEntityClass entity = TestEntityClass.newBuilder(ENTITY_ID)
                .build(infoExtractor);
        final TestEntityClass computeTier = TestEntityClass.newBuilder(COMPUTE_TIER_ID)
                // 0 coupons required - this would mainly happens if the coupon number is not set
                // in the topology.
                .setComputeTierConfig(new ComputeTierConfig(0, DEFAULT_CORE_NUM, BURSTABLE_CPU))
                .build(infoExtractor);
        when(costJournal.getEntity()).thenReturn(entity);

        TraxNumber coveredPercentage = applicator.recordRICoverage(computeTier, price, true);
        assertThat(coveredPercentage.getValue(), is(NO_COVERAGE_PERCENTAGE));
        verify(costJournal, never()).recordRiCost(any(), any(), any());
    }

    @Test
    public void testApplyRiNoRiData() {
        Map<Long, EntityReservedInstanceCoverage> topologyRiCoverage = new HashMap<>();
        final TestEntityClass entity = TestEntityClass.newBuilder(ENTITY_ID)
                .build(infoExtractor);
        topologyRiCoverage.put(entity.getId(), EntityReservedInstanceCoverage.newBuilder()
                .putCouponsCoveredByRi(RI_ID, 5.0)
                .build());
        final ReservedInstanceApplicator<TestEntityClass> applicator =
                applicatorFactory.newReservedInstanceApplicator(costJournal, infoExtractor,
                        cloudCostData, topologyRiCoverage);
        final TestEntityClass computeTier = TestEntityClass.newBuilder(COMPUTE_TIER_ID)
                .setComputeTierConfig(new ComputeTierConfig(TOTAL_COUPONS_REQUIRED, DEFAULT_CORE_NUM, BURSTABLE_CPU))
                .build(infoExtractor);
        when(costJournal.getEntity()).thenReturn(entity);
        when(cloudCostData.getExistingRiBoughtData(RI_ID)).thenReturn(Optional.empty());
        TraxNumber coveredPercentage = applicator.recordRICoverage(computeTier, price, true);
        assertThat(coveredPercentage.getValue(), is(NO_COVERAGE_PERCENTAGE));
        verify(costJournal, never()).recordRiCost(any(), any(), any());
    }

    @Test
    public void testRiJournal() {
        Map<Long, EntityReservedInstanceCoverage> topologyRiCoverage = new HashMap<>();
        final TestEntityClass entity = TestEntityClass.newBuilder(ENTITY_ID)
                .build(infoExtractor);
        topologyRiCoverage.put(entity.getId(), EntityReservedInstanceCoverage.newBuilder()
                .putCouponsCoveredByRi(RI_ID, 2.5)
                .build());
        final TestEntityClass region = TestEntityClass.newBuilder(REGION_ID).build(infoExtractor);
        CostJournal.Builder<TestEntityClass> journalBuilder = CostJournal.newBuilder(entity, infoExtractor, region,
                discountApplicator, dependentCostLookup );
        final ReservedInstanceApplicator<TestEntityClass> applicator =
                applicatorFactory.newReservedInstanceApplicator(journalBuilder, infoExtractor,
                        cloudCostData, topologyRiCoverage);
        final TestEntityClass computeTier = TestEntityClass.newBuilder(COMPUTE_TIER_ID)
                .setComputeTierConfig(new ComputeTierConfig(TOTAL_COUPONS_REQUIRED, DEFAULT_CORE_NUM, BURSTABLE_CPU))
                .build(infoExtractor);

        final ReservedInstanceData riData = new ReservedInstanceData(RI_BOUGHT, RI_SPEC);

        when(cloudCostData.getExistingRiBoughtData(RI_ID)).thenReturn(Optional.of(riData));

        when(discountApplicator.getDiscountPercentage(0L)).thenReturn(trax(0));

        applicator.recordRICoverage(computeTier, price, true);
        CostJournal<TestEntityClass> journal = journalBuilder.build();
        journal.getCategories();
        TraxNumber totalCostIncludingRI = journal.getHourlyCostForCategory(CostCategory.RI_COMPUTE);
        assert (totalCostIncludingRI.getValue() == 7.5);
    }

    /**
     * A class for comparing {@link TraxNumber}s for exact value equality.
     */
    public static class TraxEqualityMatcher extends TypeSafeMatcher<TraxNumber> {
        private final TraxNumber number;
        private String reason;

        private TraxEqualityMatcher(@Nonnull final TraxNumber number) {
            this.number = number;
        }

        @Override
        protected boolean matchesSafely(TraxNumber other) {
            if (!number.valueEquals(other)) {
                reason = String.format("value to equal %f but was %f",
                    number.getValue(), other.getValue());
                return false;
            }

            return true;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(reason);
        }
    }

    /**
     * A class for comparing {@link TraxNumber}s for approximate value equality.
     */
    public static class TraxApproximateEqualityMatcher extends TypeSafeMatcher<TraxNumber> {
        private final TraxNumber number;
        private final double acceptableDelta;
        private String reason;

        private TraxApproximateEqualityMatcher(@Nonnull final TraxNumber number,
                                               final double acceptableDelta) {
            this.number = number;
            this.acceptableDelta = acceptableDelta;
        }

        @Override
        protected boolean matchesSafely(TraxNumber other) {
            final double absDifference = Math.abs(number.getValue() - other.getValue());
            if (absDifference > acceptableDelta) {
                reason = String.format("value to to be within %f of %f but %f is %f off.",
                    acceptableDelta, number.getValue(), other.getValue(), absDifference);
                return false;
            }

            return true;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(reason);
        }
    }

    /**
     * Compare two TraxNumbers for exact value equality.
     *
     * @param number The number for later comparison.
     * @return A {@link TraxEqualityMatcher}.
     */
    public static TraxEqualityMatcher traxEq(@Nonnull final TraxNumber number) {
        return new TraxEqualityMatcher(number);
    }

    /**
     * Create an equality matcher that a {@link TraxNumber} should be exactly equal to.
     *
     * @param value The value the {@link TraxNumber} should be equal to.
     * @return A {@link TraxEqualityMatcher}.
     */
    public static TraxEqualityMatcher traxEq(final double value) {
        return new TraxEqualityMatcher(trax(value));
    }

    /**
     * Compare two TraxNumbers for approximate value equality.
     *
     * @param number The number for later comparison.
     * @param delta Acceptable delta for the difference between values.
     * @return A {@link TraxEqualityMatcher}.
     */
    public static TraxApproximateEqualityMatcher traxApprox(@Nonnull final TraxNumber number,
                                                            final double delta) {
        return new TraxApproximateEqualityMatcher(number, delta);
    }

    /**
     * Create an equality matcher that a {@link TraxNumber} should be approximately equal to.
     *
     * @param value The value the {@link TraxNumber} should be equal to.
     * @param delta Acceptable delta for the difference between values.
     * @return A {@link TraxEqualityMatcher}.
     */
    public static TraxApproximateEqualityMatcher traxApprox(final double value,
                                                            final double delta) {
        return new TraxApproximateEqualityMatcher(trax(value), delta);
    }
}
