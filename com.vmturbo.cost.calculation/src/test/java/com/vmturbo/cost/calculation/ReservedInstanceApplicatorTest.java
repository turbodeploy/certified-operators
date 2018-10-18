package com.vmturbo.cost.calculation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    private static final long RI_ID = 77;

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
                            .setNumberOfCoupons(10)))
            .build();

    private static final ReservedInstanceSpec RI_SPEC = ReservedInstanceSpec.newBuilder()
            .setId(RI_SPEC_ID)
            .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder()
                    .setType(ReservedInstanceType.newBuilder()
                            .setTermYears(1)))
            .build();

    @Test
    public void testApplyRi() {
        final ReservedInstanceApplicator<TestEntityClass> applicator =
            applicatorFactory.newReservedInstanceApplicator(costJournal, infoExtractor, cloudCostData);

        final TestEntityClass entity = TestEntityClass.newBuilder(7)
                .build(infoExtractor);
        final TestEntityClass computeTier = TestEntityClass.newBuilder(17)
                .setComputeTierConfig(new ComputeTierConfig(TOTAL_COUPONS_REQUIRED))
                .build(infoExtractor);

        when(costJournal.getEntity()).thenReturn(entity);
        when(cloudCostData.getRiCoverageForEntity(entity.getId()))
            .thenReturn(Optional.of(EntityReservedInstanceCoverage.newBuilder()
                .putCouponsCoveredByRi(RI_ID, 5.0)
                .build()));

        final ReservedInstanceData riData = new ReservedInstanceData(RI_BOUGHT, RI_SPEC);

        when(cloudCostData.getRiBoughtData(RI_ID)).thenReturn(Optional.of(riData));

        double coveredPercentage = applicator.recordRICoverage(computeTier);
        assertThat(coveredPercentage, closeTo(0.5, 0.0001));

        final ArgumentCaptor<CurrencyAmount> amountCaptor = ArgumentCaptor.forClass(CurrencyAmount.class);
        verify(costJournal).recordRiCost(eq(CostCategory.COMPUTE), eq(riData), amountCaptor.capture());
        CurrencyAmount amount = amountCaptor.getValue();
        // 10 + 10 + (3650 / (1 * 365 * 24)) = 30 <- hourly cost per instance
        // 30 / 10 = 3 <- hourly cost per coupon
        // 3 * 5 = 15 <- hourly cost for 5 coupons, which is how much apply to the entity.
        assertThat(amount.getAmount(), closeTo(15.0, 0.001));
    }

    @Test
    public void testApplyRiCoverageMoreThanRequired() {
        final ReservedInstanceApplicator<TestEntityClass> applicator =
                applicatorFactory.newReservedInstanceApplicator(costJournal, infoExtractor, cloudCostData);

        final TestEntityClass entity = TestEntityClass.newBuilder(7)
                .build(infoExtractor);
        final TestEntityClass computeTier = TestEntityClass.newBuilder(17)
                .setComputeTierConfig(new ComputeTierConfig(10))
                .build(infoExtractor);

        when(costJournal.getEntity()).thenReturn(entity);
        when(cloudCostData.getRiCoverageForEntity(entity.getId()))
            .thenReturn(Optional.of(EntityReservedInstanceCoverage.newBuilder()
                // More covered coupons than coupons required.
                .putCouponsCoveredByRi(RI_ID, 100)
                .build()));

        final ReservedInstanceData riData = new ReservedInstanceData(RI_BOUGHT, RI_SPEC);

        when(cloudCostData.getRiBoughtData(RI_ID)).thenReturn(Optional.of(riData));

        double coveredPercentage = applicator.recordRICoverage(computeTier);
        assertThat(coveredPercentage, is(1.0));

        final ArgumentCaptor<CurrencyAmount> amountCaptor = ArgumentCaptor.forClass(CurrencyAmount.class);
        verify(costJournal).recordRiCost(eq(CostCategory.COMPUTE), eq(riData), amountCaptor.capture());
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
        final ReservedInstanceApplicator<TestEntityClass> applicator =
                applicatorFactory.newReservedInstanceApplicator(costJournal, infoExtractor, cloudCostData);
        final TestEntityClass entity = TestEntityClass.newBuilder(7)
                .build(infoExtractor);
        final TestEntityClass computeTier = TestEntityClass.newBuilder(17)
                .setComputeTierConfig(new ComputeTierConfig(10))
                .build(infoExtractor);
        when(costJournal.getEntity()).thenReturn(entity);
        when(cloudCostData.getRiCoverageForEntity(entity.getId())).thenReturn(Optional.empty());

        double coveredPercentage = applicator.recordRICoverage(computeTier);
        assertThat(coveredPercentage, is(0.0));
        verify(costJournal, never()).recordRiCost(any(), any(), any());
    }

    @Test
    public void testApplyRiNoRequiredNoCoverage() {
        final ReservedInstanceApplicator<TestEntityClass> applicator =
                applicatorFactory.newReservedInstanceApplicator(costJournal, infoExtractor, cloudCostData);
        final TestEntityClass entity = TestEntityClass.newBuilder(7)
                .build(infoExtractor);
        final TestEntityClass computeTier = TestEntityClass.newBuilder(17)
                // 0 coupons required - this would mainly happens if the coupon number is not set
                // in the topology.
                .setComputeTierConfig(new ComputeTierConfig(0))
                .build(infoExtractor);
        when(costJournal.getEntity()).thenReturn(entity);
        when(cloudCostData.getRiCoverageForEntity(entity.getId())).thenReturn(Optional.empty());

        double coveredPercentage = applicator.recordRICoverage(computeTier);
        assertThat(coveredPercentage, is(0.0));
        verify(costJournal, never()).recordRiCost(any(), any(), any());
    }

    @Test
    public void testApplyRiNoRiData() {
        final ReservedInstanceApplicator<TestEntityClass> applicator =
                applicatorFactory.newReservedInstanceApplicator(costJournal, infoExtractor, cloudCostData);
        final TestEntityClass entity = TestEntityClass.newBuilder(7)
                .build(infoExtractor);
        final TestEntityClass computeTier = TestEntityClass.newBuilder(17)
                .setComputeTierConfig(new ComputeTierConfig(10))
                .build(infoExtractor);

        when(costJournal.getEntity()).thenReturn(entity);
        when(cloudCostData.getRiCoverageForEntity(entity.getId()))
            .thenReturn(Optional.of(EntityReservedInstanceCoverage.newBuilder()
                    .putCouponsCoveredByRi(RI_ID, 5.0)
                    .build()));
        when(cloudCostData.getRiBoughtData(RI_ID)).thenReturn(Optional.empty());

        double coveredPercentage = applicator.recordRICoverage(computeTier);
        assertThat(coveredPercentage, is(0.0));
        verify(costJournal, never()).recordRiCost(any(), any(), any());
    }
}
