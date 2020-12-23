package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.demand.BoundedDuration;
import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand.EntityInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateDemandCollector.AggregateDemandCollectorFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.DemandTransformationJournal.DemandTransformationResult;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.ClassifiedEntitySelection;
import com.vmturbo.cloud.common.data.ImmutableTimeSeries;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.data.TimeSeries;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class AggregateDemandCollectorTest {

    private final TimeInterval analysisWindow = TimeInterval.builder()
            .startTime(Instant.ofEpochSecond(Duration.ofDays(1).getSeconds()))
            .endTime(Instant.ofEpochSecond(Duration.ofDays(2).getSeconds()))
            .build();

    private final BoundedDuration analysisInterval = BoundedDuration.builder()
            .amount(1)
            .unit(ChronoUnit.HOURS)
            .build();

    private DemandTransformationJournal transformationJournal;

    private final AggregateDemandCollectorFactory collectorFactory =
            new AggregateDemandCollectorFactory();

    private AggregateDemandCollector aggregateDemandCollector;

    @Before
    public void setup() {

        transformationJournal = DemandTransformationJournal.newJournal();

        aggregateDemandCollector = collectorFactory.newCollector(
                transformationJournal,
                analysisWindow,
                analysisInterval);
    }


    /**
     * Tests demand aggregation with demand overlap and filtering through demand transformation.
     */
    @Test
    public void testDemandAggregation() {

        final DemandClassification classification = DemandClassification.of(AllocatedDemandClassification.ALLOCATED);
        final TimeSeries<TimeInterval> demandTimeline = ImmutableTimeSeries.of(
                TimeInterval.builder()
                        .startTime(analysisWindow.startTime())
                        .endTime(analysisWindow.startTime().plus(90, ChronoUnit.MINUTES))
                        .build(),
                TimeInterval.builder()
                        .startTime(analysisWindow.startTime().plus(120, ChronoUnit.MINUTES))
                        .endTime(analysisWindow.startTime().plus(195, ChronoUnit.MINUTES))
                        .build(),
                TimeInterval.builder()
                        .startTime(analysisWindow.startTime().plus(225, ChronoUnit.MINUTES))
                        .endTime(analysisWindow.startTime().plus(240, ChronoUnit.MINUTES))
                        .build());
        final ClassifiedEntitySelection classifiedEntitySelection = ClassifiedEntitySelection.builder()
                .entityOid(1L)
                .accountOid(2L)
                .regionOid(3L)
                .serviceProviderOid(4L)
                .classification(classification)
                .demandTimeline(demandTimeline)
                .cloudTierDemand(ComputeTierDemand.builder()
                        .cloudTierOid(5L)
                        .osType(OSType.RHEL)
                        .tenancy(Tenancy.DEFAULT)
                        .build())
                .isRecommendationCandidate(true)
                .isSuspended(true)
                .isTerminated(false)
                .build();


        // Invoke SUT
        final AggregateDemandCollector collector = collectorFactory.newCollector(
                transformationJournal,
                analysisWindow,
                analysisInterval);
        collector.collectEntitySelection(classifiedEntitySelection);

        // check the journal results
        final DemandTransformationResult transformationResult = transformationJournal.getTransformationResult();
        final Map<TimeInterval, AggregateDemandSegment> demandByBucket =
                transformationResult.aggregateDemandByBucket();
        // check the bucket results

        final EntityInfo entityInfo = EntityInfo.builder()
                .entityOid(classifiedEntitySelection.entityOid())
                .isSuspended(classifiedEntitySelection.isSuspended())
                .isTerminated(classifiedEntitySelection.isTerminated())
                .build();
        final ScopedCloudTierInfo cloudTierInfo = ScopedCloudTierInfo.builder()
                .accountOid(classifiedEntitySelection.accountOid())
                .regionOid(classifiedEntitySelection.regionOid())
                .serviceProviderOid(classifiedEntitySelection.serviceProviderOid())
                .cloudTierDemand(classifiedEntitySelection.cloudTierDemand())
                .build();
        final AggregateCloudTierDemand.Builder aggregateDemandBuilder = AggregateCloudTierDemand.builder()
                .cloudTierInfo(cloudTierInfo)
                .classification(classification)
                .isRecommendationCandidate(classifiedEntitySelection.isRecommendationCandidate());

        // check the first hour
        final TimeInterval firstHour = TimeInterval.builder()
                .startTime(analysisWindow.startTime())
                .endTime(analysisWindow.startTime().plus(1, ChronoUnit.HOURS))
                .build();
        final AggregateDemandSegment firstDemandSet = AggregateDemandSegment.builder()
                .timeInterval(firstHour)
                .putAggregateCloudTierDemand(
                        cloudTierInfo,
                        aggregateDemandBuilder
                                .demandByEntity(ImmutableMap.of(entityInfo, 1.0))
                                .build())
                .build();
        assertThat(demandByBucket, hasKey(firstHour));
        assertThat(demandByBucket.get(firstHour), equalTo(firstDemandSet));

        // check the second hour
        final TimeInterval secondHour = TimeInterval.builder()
                .startTime(analysisWindow.startTime().plus(1, ChronoUnit.HOURS))
                .endTime(analysisWindow.startTime().plus(2, ChronoUnit.HOURS))
                .build();
        final AggregateDemandSegment secondDemandSet = AggregateDemandSegment.builder()
                .timeInterval(secondHour)
                .putAggregateCloudTierDemand(
                        cloudTierInfo,
                        aggregateDemandBuilder
                                .demandByEntity(ImmutableMap.of(entityInfo, .5))
                                .build())
                .build();
        assertThat(demandByBucket, hasKey(secondHour));
        assertThat(demandByBucket.get(secondHour), equalTo(secondDemandSet));

        // check the third hour
        final TimeInterval thirdHour = TimeInterval.builder()
                .startTime(analysisWindow.startTime().plus(2, ChronoUnit.HOURS))
                .endTime(analysisWindow.startTime().plus(3, ChronoUnit.HOURS))
                .build();
        final AggregateDemandSegment thirdDemandSet = AggregateDemandSegment.builder()
                .timeInterval(thirdHour)
                .putAggregateCloudTierDemand(
                        cloudTierInfo,
                        aggregateDemandBuilder
                                .demandByEntity(ImmutableMap.of(entityInfo, 1.0))
                                .build())
                .build();
        assertThat(demandByBucket, hasKey(thirdHour));
        assertThat(demandByBucket.get(thirdHour), equalTo(thirdDemandSet));

        // check the fourth hour
        final TimeInterval fourthHour = TimeInterval.builder()
                .startTime(analysisWindow.startTime().plus(3, ChronoUnit.HOURS))
                .endTime(analysisWindow.startTime().plus(4, ChronoUnit.HOURS))
                .build();
        final AggregateDemandSegment fourthDemandSet = AggregateDemandSegment.builder()
                .timeInterval(fourthHour)
                .putAggregateCloudTierDemand(
                        cloudTierInfo,
                        aggregateDemandBuilder
                                .demandByEntity(ImmutableMap.of(entityInfo, .5))
                                .build())
                .build();
        assertThat(demandByBucket, hasKey(fourthHour));
        assertThat(demandByBucket.get(fourthHour), equalTo(fourthDemandSet));

    }
}
