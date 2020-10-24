package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.demand.BoundedDuration;
import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.DemandTransformationJournal.DemandTransformationResult;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.data.TimeSeries;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class AnalysisDemandCreatorTest {

    private final TimeInterval analysisWindow = TimeInterval.builder()
            .startTime(Instant.ofEpochSecond(0))
            .endTime(Instant.ofEpochSecond(Duration.ofHours(3).getSeconds()))
            .build();

    private final BoundedDuration analysisBucket = BoundedDuration.builder()
            .amount(1)
            .unit(ChronoUnit.HOURS)
            .build();

    @Nonnull
    public void testAnalysisDemandCreation() {


        final TimeInterval firstHour = TimeInterval.builder()
                .startTime(analysisWindow.startTime())
                .endTime(analysisWindow.startTime().plus(1, ChronoUnit.HOURS))
                .build();
        final AggregateCloudTierDemand aggregateDemand = AggregateCloudTierDemand.builder()
                .cloudTierInfo(ScopedCloudTierInfo.builder()
                        .accountOid(1)
                        .regionOid(2)
                        .serviceProviderOid(3)
                        .cloudTierDemand(ComputeTierDemand.builder()
                                .cloudTierOid(4)
                                .osType(OSType.RHEL)
                                .tenancy(Tenancy.DEFAULT)
                                .build())
                        .build())
                .classification(DemandClassification.of(AllocatedDemandClassification.ALLOCATED))
                .build();

        final AggregateDemandSegment firstDemandSegment = AggregateDemandSegment.builder()
                .timeInterval(firstHour)
                .putAggregateCloudTierDemand(aggregateDemand.cloudTierInfo(), aggregateDemand)
                .build();
        final TimeInterval secondHour = TimeInterval.builder()
                .startTime(analysisWindow.startTime().plus(1, ChronoUnit.HOURS))
                .endTime(analysisWindow.startTime().plus(2, ChronoUnit.HOURS))
                .build();
        final AggregateDemandSegment secondDemandSegment = AggregateDemandSegment.builder()
                .timeInterval(secondHour)
                .putAggregateCloudTierDemand(aggregateDemand.cloudTierInfo(), aggregateDemand)
                .build();
        final TimeInterval thirdHour = TimeInterval.builder()
                .startTime(analysisWindow.startTime().plus(2, ChronoUnit.HOURS))
                .endTime(analysisWindow.startTime().plus(3, ChronoUnit.HOURS))
                .build();
        final AggregateDemandSegment thirdDemandSegment = AggregateDemandSegment.builder()
                .timeInterval(thirdHour)
                .putAggregateCloudTierDemand(aggregateDemand.cloudTierInfo(), aggregateDemand)
                .build();
        final DemandTransformationResult transformationResult = DemandTransformationResult.builder()
                .transformationStats(DemandTransformationStatistics.EMPTY_STATS)
                .putAggregateDemandByBucket(firstHour, firstDemandSegment)
                .putAggregateDemandByBucket(secondHour, secondDemandSegment)
                .putAggregateDemandByBucket(thirdHour, thirdDemandSegment)
                .build();

        // invoke the creator
        final AggregateAnalysisDemand aggregateAnalysisDemand = AnalysisDemandCreator.createAnalysisDemand(
                transformationResult, analysisWindow, analysisBucket);

        // expected result
        final TimeSeries<TimeInterval> expectedTimeInterval = TimeSeries.newTimeline(
                firstHour, secondHour, thirdHour);
        final TimeSeries<AggregateDemandSegment> expectedAggregateDemand = TimeSeries.newTimeSeries(
                firstDemandSegment, secondDemandSegment, thirdDemandSegment);

        // assertions
        assertThat(aggregateAnalysisDemand.analysisTimeline(), equalTo(expectedTimeInterval));
        assertThat(aggregateAnalysisDemand.aggregateDemandSeries(), equalTo(expectedAggregateDemand));
    }

}
