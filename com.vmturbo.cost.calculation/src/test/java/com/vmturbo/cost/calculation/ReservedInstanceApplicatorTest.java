package com.vmturbo.cost.calculation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
import com.vmturbo.cost.calculation.ReservedInstanceApplicator.ReservedInstanceApplicatorFactory;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor;
import com.vmturbo.cost.calculation.integration.EntityInfoExtractor.ComputeTierConfig;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;

public class ReservedInstanceApplicatorTest {

    private CloudCostData cloudCostData = mock(CloudCostData.class);

    private EntityInfoExtractor<TestEntityClass> infoExtractor = mock(EntityInfoExtractor.class);

    private CostJournal.Builder<TestEntityClass> costJournal = mock(CostJournal.Builder.class);

    private ReservedInstanceApplicatorFactory<TestEntityClass> applicatorFactory =
            ReservedInstanceApplicator.newFactory();

    private static final int ENTITY_ID = 7;

    private static final int COMPUTE_TIER_ID = 17;

    private static final long RI_ID = 77;

    private static final double FULL_COVERAGE_PERCENTAGE = 1.0;
    private static final double NO_COVERAGE_PERCENTAGE = 0.0;

    // the number of cores is set to 0 because it doesn't affect this test
    private static final int DEFAULT_CORE_NUM = 0;

    private static final long RI_SPEC_ID = 777;

    private static final int TOTAL_COUPONS_REQUIRED = 10;

    private TestEntityClass computeTier;

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

    @Test
    public void testApplyRi() {
        Map<Long, EntityReservedInstanceCoverage> topologyRiCoverage = new HashMap<>();
        final TestEntityClass entity = TestEntityClass.newBuilder(ENTITY_ID)
                .build(infoExtractor);
        topologyRiCoverage.put(entity.getId(), EntityReservedInstanceCoverage.newBuilder()
                .putCouponsCoveredByRi(RI_ID, 5.0)
                .build());
        final TestEntityClass computeTier = TestEntityClass.newBuilder(COMPUTE_TIER_ID)
                .setComputeTierConfig(new ComputeTierConfig(TOTAL_COUPONS_REQUIRED, DEFAULT_CORE_NUM))
                .build(infoExtractor);
        final ReservedInstanceApplicator<TestEntityClass> applicator =
            applicatorFactory.newReservedInstanceApplicator(costJournal, infoExtractor,
                    cloudCostData, topologyRiCoverage);

        when(costJournal.getEntity()).thenReturn(entity);

        final ReservedInstanceData riData = new ReservedInstanceData(RI_BOUGHT, RI_SPEC);

        when(cloudCostData.getExistingRiBoughtData(RI_ID)).thenReturn(Optional.of(riData));

        double coveredPercentage = applicator.recordRICoverage(computeTier);
        assertThat(coveredPercentage, closeTo(0.5, 0.0001));

        final ArgumentCaptor<CurrencyAmount> amountCaptor = ArgumentCaptor.forClass(CurrencyAmount.class);
        verify(costJournal).recordRiCost( eq(riData), eq(5.0), amountCaptor.capture());
        CurrencyAmount amount = amountCaptor.getValue();
        // 10 + 10 + (3650 / (1 * 365 * 24)) = 30 <- hourly cost per instance
        // 30 / 10 = 3 <- hourly cost per coupon
        // 3 * 5 = 15 <- hourly cost for 5 coupons, which is how much apply to the entity.
        assertThat(amount.getAmount(), closeTo(15.0, 0.001));
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
                .setComputeTierConfig(new ComputeTierConfig(TOTAL_COUPONS_REQUIRED, DEFAULT_CORE_NUM))
                .build(infoExtractor);
        final ReservedInstanceApplicator<TestEntityClass> applicator =
                applicatorFactory.newReservedInstanceApplicator(costJournal, infoExtractor,
                        cloudCostData, topologyRiCoverage);
        when(costJournal.getEntity()).thenReturn(entity);

        final ReservedInstanceData riData = new ReservedInstanceData(RI_BOUGHT, RI_SPEC);

        when(cloudCostData.getExistingRiBoughtData(RI_ID)).thenReturn(Optional.of(riData));

        double coveredPercentage = applicator.recordRICoverage(computeTier);
        assertThat(coveredPercentage, is(FULL_COVERAGE_PERCENTAGE));

        final ArgumentCaptor<CurrencyAmount> amountCaptor = ArgumentCaptor.forClass(CurrencyAmount.class);
        verify(costJournal).recordRiCost(eq(riData), eq(100.0), amountCaptor.capture());
        CurrencyAmount amount = amountCaptor.getValue();
        // 10 + 10 + (3650 / (1 * 365 * 24)) = 30 <- hourly cost per instance
        // 30 / 10 = 3 <- hourly cost per coupon
        // 3 * 10 = 30 <- hourly cost for 10 coupons, which is how much apply to the entity.
        //     The RI Bought only has 10 coupons, so we should only get the cost for those 10
        //     even though the coverage says there are 100.
        assertThat(amount.getAmount(), closeTo(30.0, 0.001));
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
                .setComputeTierConfig(new ComputeTierConfig(TOTAL_COUPONS_REQUIRED, DEFAULT_CORE_NUM))
                .build(infoExtractor);
        when(costJournal.getEntity()).thenReturn(entity);
        double coveredPercentage = applicator.recordRICoverage(computeTier);
        assertThat(coveredPercentage, is(NO_COVERAGE_PERCENTAGE));
        verify(costJournal, never()).recordRiCost(any(), anyLong(), any());
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
                .setComputeTierConfig(new ComputeTierConfig(0, DEFAULT_CORE_NUM))
                .build(infoExtractor);
        when(costJournal.getEntity()).thenReturn(entity);

        double coveredPercentage = applicator.recordRICoverage(computeTier);
        assertThat(coveredPercentage, is(NO_COVERAGE_PERCENTAGE));
        verify(costJournal, never()).recordRiCost(any(), anyLong(), any());
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
                .setComputeTierConfig(new ComputeTierConfig(TOTAL_COUPONS_REQUIRED, DEFAULT_CORE_NUM))
                .build(infoExtractor);

        when(costJournal.getEntity()).thenReturn(entity);
        when(cloudCostData.getExistingRiBoughtData(RI_ID)).thenReturn(Optional.empty());

        double coveredPercentage = applicator.recordRICoverage(computeTier);
        assertThat(coveredPercentage, is(NO_COVERAGE_PERCENTAGE));
        verify(costJournal, never()).recordRiCost(any(), anyLong(), any());
    }
}
