package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.demand.BoundedDuration;
import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableTimeInterval;
import com.vmturbo.cloud.commitment.analysis.demand.TimeInterval;
import com.vmturbo.cloud.commitment.analysis.demand.TimeSeries;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand.EntityInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateDemandCollector.AggregateDemandCollectorFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.DemandTransformationJournal.DemandTransformationResult;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.ClassifiedEntitySelection;
import com.vmturbo.cloud.common.topology.ComputeTierFamilyResolver.ComputeTierFamilyResolverFactory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class AggregateDemandCollectorTest {

    private final TimeInterval analysisWindow = ImmutableTimeInterval.builder()
            .startTime(Instant.ofEpochSecond(Duration.ofDays(1).getSeconds()))
            .endTime(Instant.ofEpochSecond(Duration.ofDays(2).getSeconds()))
            .build();

    private final BoundedDuration analysisInterval = BoundedDuration.builder()
            .amount(1)
            .unit(ChronoUnit.HOURS)
            .build();

    private DemandTransformationJournal transformationJournal;

    private CloudTopology<TopologyEntityDTO> cloudTierTopology = mock(CloudTopology.class);

    private final ComputeTierFamilyResolverFactory computeTierFamilyResolverFactory =
            new ComputeTierFamilyResolverFactory();

    private final AggregateDemandCollectorFactory collectorFactory =
            new AggregateDemandCollectorFactory(computeTierFamilyResolverFactory);

    private AggregateDemandCollector aggregateDemandCollector;

    @Before
    public void setup() {

        transformationJournal = DemandTransformationJournal.newJournal();

        aggregateDemandCollector = collectorFactory.newCollector(
                transformationJournal,
                cloudTierTopology,
                analysisWindow,
                analysisInterval);
    }


    /**
     * Tests demand aggregation with demand overlap and filtering through demand transformation.
     */
    @Test
    public void testDemandAggregation() {

        final DemandClassification classification = DemandClassification.of(AllocatedDemandClassification.ALLOCATED);
        final TimeSeries<TimeInterval> demandTimeline = TimeSeries.newTimeline(Lists.newArrayList(
                ImmutableTimeInterval.builder()
                        .startTime(analysisWindow.startTime())
                        .endTime(analysisWindow.startTime().plus(90, ChronoUnit.MINUTES))
                        .build(),
                ImmutableTimeInterval.builder()
                        .startTime(analysisWindow.startTime().plus(120, ChronoUnit.MINUTES))
                        .endTime(analysisWindow.startTime().plus(195, ChronoUnit.MINUTES))
                        .build(),
                ImmutableTimeInterval.builder()
                        .startTime(analysisWindow.startTime().plus(225, ChronoUnit.MINUTES))
                        .endTime(analysisWindow.startTime().plus(240, ChronoUnit.MINUTES))
                        .build()));
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
                cloudTierTopology,
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
        final AggregateCloudTierDemand.Builder aggregateDemandBuilder = AggregateCloudTierDemand.builder()
                .accountOid(classifiedEntitySelection.accountOid())
                .regionOid(classifiedEntitySelection.regionOid())
                .serviceProviderOid(classifiedEntitySelection.serviceProviderOid())
                .cloudTierDemand(classifiedEntitySelection.cloudTierDemand())
                .classification(classification)
                .isRecommendationCandidate(classifiedEntitySelection.isRecommendationCandidate());

        // check the first hour
        final TimeInterval firstHour = ImmutableTimeInterval.builder()
                .startTime(analysisWindow.startTime())
                .endTime(analysisWindow.startTime().plus(1, ChronoUnit.HOURS))
                .build();
        final AggregateDemandSegment firstDemandSet = AggregateDemandSegment.builder()
                .timeInterval(firstHour)
                .addAggregateCloudTierDemand(aggregateDemandBuilder
                        .demandByEntity(ImmutableMap.of(entityInfo, 1.0))
                        .build())
                .build();
        assertThat(demandByBucket, hasKey(firstHour));
        assertThat(demandByBucket.get(firstHour), equalTo(firstDemandSet));

        // check the second hour
        final TimeInterval secondHour = ImmutableTimeInterval.builder()
                .startTime(analysisWindow.startTime().plus(1, ChronoUnit.HOURS))
                .endTime(analysisWindow.startTime().plus(2, ChronoUnit.HOURS))
                .build();
        final AggregateDemandSegment secondDemandSet = AggregateDemandSegment.builder()
                .timeInterval(secondHour)
                .addAggregateCloudTierDemand(aggregateDemandBuilder
                        .demandByEntity(ImmutableMap.of(entityInfo, .5))
                        .build())
                .build();
        assertThat(demandByBucket, hasKey(secondHour));
        assertThat(demandByBucket.get(secondHour), equalTo(secondDemandSet));

        // check the third hour
        final TimeInterval thirdHour = ImmutableTimeInterval.builder()
                .startTime(analysisWindow.startTime().plus(2, ChronoUnit.HOURS))
                .endTime(analysisWindow.startTime().plus(3, ChronoUnit.HOURS))
                .build();
        final AggregateDemandSegment thirdDemandSet = AggregateDemandSegment.builder()
                .timeInterval(thirdHour)
                .addAggregateCloudTierDemand(aggregateDemandBuilder
                        .demandByEntity(ImmutableMap.of(entityInfo, 1.0))
                        .build())
                .build();
        assertThat(demandByBucket, hasKey(thirdHour));
        assertThat(demandByBucket.get(thirdHour), equalTo(thirdDemandSet));

        // check the fourth hour
        final TimeInterval fourthHour = ImmutableTimeInterval.builder()
                .startTime(analysisWindow.startTime().plus(3, ChronoUnit.HOURS))
                .endTime(analysisWindow.startTime().plus(4, ChronoUnit.HOURS))
                .build();
        final AggregateDemandSegment fourthDemandSet = AggregateDemandSegment.builder()
                .timeInterval(fourthHour)
                .addAggregateCloudTierDemand(aggregateDemandBuilder
                        .demandByEntity(ImmutableMap.of(entityInfo, .5))
                        .build())
                .build();
        assertThat(demandByBucket, hasKey(fourthHour));
        assertThat(demandByBucket.get(fourthHour), equalTo(fourthDemandSet));

    }
}
