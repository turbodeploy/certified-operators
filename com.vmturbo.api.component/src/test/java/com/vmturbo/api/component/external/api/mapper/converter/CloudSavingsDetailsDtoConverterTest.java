package com.vmturbo.api.component.external.api.mapper.converter;

import org.junit.Assert;
import org.junit.Test;

import com.vmturbo.api.dto.action.CloudResizeActionDetailsApiDTO;
import com.vmturbo.api.dto.entity.EntityUptimeApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails;
import com.vmturbo.common.protobuf.action.ActionDTO.CloudSavingsDetails.TierCostDetails;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentCoverage;
import com.vmturbo.common.protobuf.cost.EntityUptime.EntityUptimeDTO;
import com.vmturbo.platform.sdk.common.CommonCost.CurrencyAmount;

/**
 * Unit test for {@link CloudSavingsDetailsDtoConverter}.
 */
public class CloudSavingsDetailsDtoConverterTest {

    private static final double RI_COVERAGE_CAPACITY_BEFORE = 10001.1;
    private static final double RI_COVERAGE_USED_BEFORE = 10002.2;
    private static final double ON_DEMAND_COST_BEFORE = 10003.3;
    private static final double ON_DEMAND_RATE_BEFORE = 10004.4;
    private static final double RI_COVERAGE_CAPACITY_AFTER = 10005.5;
    private static final double RI_COVERAGE_USED_AFTER = 10006.6;
    private static final double ON_DEMAND_COST_AFTER = 10007.7;
    private static final double ON_DEMAND_RATE_AFTER = 10008.8;
    private static final Long TOTAL_DURATION_MS = 10009L;
    private static final Long UPTIME_DURATION_MS = 100010L;
    private static final Long CREATION_TIME_MS = 10011L;
    private static final double UPTIME_PERCENTAGE = 10012.12;
    private static final double DELTA = 0.001;

    /**
     * Test for {@link CloudSavingsDetailsDtoConverter#convert(CloudSavingsDetails)}.
     */
    @Test
    public void testConvert() {
        final CloudSavingsDetails protobufDto =
                CloudSavingsDetails.newBuilder()
                        .setSourceTierCostDetails(TierCostDetails.newBuilder()
                                .setCloudCommitmentCoverage(CloudCommitmentCoverage.newBuilder()
                                        .setCapacity(CloudCommitmentAmount.newBuilder()
                                                .setCoupons(RI_COVERAGE_CAPACITY_BEFORE))
                                        .setUsed(CloudCommitmentAmount.newBuilder()
                                                .setCoupons(RI_COVERAGE_USED_BEFORE)))
                                .setOnDemandCost(CurrencyAmount.newBuilder()
                                        .setAmount(ON_DEMAND_COST_BEFORE))
                                .setOnDemandRate(CurrencyAmount.newBuilder()
                                        .setAmount(ON_DEMAND_RATE_BEFORE)))
                        .setProjectedTierCostDetails(
                                TierCostDetails.newBuilder()
                                        .setCloudCommitmentCoverage(
                                                CloudCommitmentCoverage.newBuilder()
                                                        .setCapacity(
                                                                CloudCommitmentAmount.newBuilder()
                                                                        .setCoupons(
                                                                                RI_COVERAGE_CAPACITY_AFTER))
                                                        .setUsed(CloudCommitmentAmount.newBuilder()
                                                                .setCoupons(
                                                                        RI_COVERAGE_USED_AFTER)))
                                        .setOnDemandCost(CurrencyAmount.newBuilder()
                                                .setAmount(ON_DEMAND_COST_AFTER))
                                        .setOnDemandRate(CurrencyAmount.newBuilder()
                                                .setAmount(ON_DEMAND_RATE_AFTER)))
                        .setEntityUptime(EntityUptimeDTO.newBuilder()
                                .setTotalDurationMs(TOTAL_DURATION_MS)
                                .setUptimeDurationMs(UPTIME_DURATION_MS)
                                .setCreationTimeMs(CREATION_TIME_MS)
                                .setUptimePercentage(UPTIME_PERCENTAGE))
                        .build();

        final CloudResizeActionDetailsApiDTO dto = new CloudSavingsDetailsDtoConverter(
                new EntityUptimeDtoConverter()).convert(protobufDto);

        Assert.assertEquals(ON_DEMAND_COST_AFTER, dto.getOnDemandCostAfter(), DELTA);
        Assert.assertEquals(ON_DEMAND_COST_BEFORE, dto.getOnDemandCostBefore(), DELTA);

        Assert.assertEquals(ON_DEMAND_RATE_AFTER, dto.getOnDemandRateAfter(), DELTA);
        Assert.assertEquals(ON_DEMAND_RATE_BEFORE, dto.getOnDemandRateBefore(), DELTA);

        final StatApiDTO riCoverageBefore = dto.getRiCoverageBefore();
        Assert.assertEquals(RI_COVERAGE_CAPACITY_BEFORE, riCoverageBefore.getCapacity().getAvg(),
                DELTA);
        Assert.assertEquals(RI_COVERAGE_USED_BEFORE, riCoverageBefore.getValue(), DELTA);

        final StatApiDTO riCoverageAfter = dto.getRiCoverageAfter();
        Assert.assertEquals(RI_COVERAGE_CAPACITY_AFTER, riCoverageAfter.getCapacity().getAvg(),
                DELTA);
        Assert.assertEquals(RI_COVERAGE_USED_AFTER, riCoverageAfter.getValue(), DELTA);

        final EntityUptimeApiDTO entityUptime = dto.getEntityUptime();
        Assert.assertEquals(TOTAL_DURATION_MS, entityUptime.getTotalDurationInMilliseconds());
        Assert.assertEquals(UPTIME_DURATION_MS, entityUptime.getUptimeDurationInMilliseconds());
        Assert.assertEquals(CREATION_TIME_MS, entityUptime.getCreationTimestamp());
        Assert.assertEquals(UPTIME_PERCENTAGE, entityUptime.getUptimePercentage(), DELTA);
    }
}
