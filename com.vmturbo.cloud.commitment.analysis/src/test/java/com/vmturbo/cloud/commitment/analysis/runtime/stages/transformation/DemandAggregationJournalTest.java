package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertThat;

import java.time.Duration;
import java.time.Instant;

import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.TestUtils;
import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.data.DoubleStatistics;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate.DemandTimeSeries;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand.EntityInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.DemandTransformationJournal.DemandTransformationResult;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.data.TimeSeries;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class DemandAggregationJournalTest {

    private DemandTransformationJournal demandTransformationJournal;

    @Before
    public void setup() {
        demandTransformationJournal = DemandTransformationJournal.newJournal();
    }

    @Test
    public void testRecordingDemand() {

        final TimeInterval bucketA = TimeInterval.builder()
                .startTime(Instant.ofEpochSecond(60 * 60))
                .endTime(Instant.ofEpochSecond(60 * 60 * 2))
                .build();
        final TimeInterval bucketB = TimeInterval.builder()
                .startTime(Instant.ofEpochSecond(60 * 60 * 2))
                .endTime(Instant.ofEpochSecond(60 * 60 * 3))
                .build();
        final AggregateCloudTierDemand demandA = AggregateCloudTierDemand.builder()
                .cloudTierInfo(ScopedCloudTierInfo.builder()
                        .cloudTierDemand(ComputeTierDemand.builder()
                                .cloudTierOid(1)
                                .osType(OSType.RHEL)
                                .tenancy(Tenancy.DEFAULT)
                                .build())
                        .accountOid(2)
                        .regionOid(3)
                        .serviceProviderOid(4)
                        .build())
                .classification(DemandClassification.of(AllocatedDemandClassification.ALLOCATED))
                .build();
        final AggregateCloudTierDemand demandB = AggregateCloudTierDemand.builder()
                .from(demandA)
                .cloudTierInfo(ScopedCloudTierInfo.builder()
                        .from(demandA.cloudTierInfo())
                        .accountOid(5)
                        .build())
                .build();

        // setup entity info
        final EntityInfo entityInfoA = EntityInfo.builder()
                .entityOid(6)
                .isSuspended(false)
                .isTerminated(false)
                .build();
        final EntityInfo entityInfoB = EntityInfo.builder()
                .entityOid(7)
                .isSuspended(false)
                .isTerminated(false)
                .build();
        final EntityInfo entityInfoC = EntityInfo.builder()
                .entityOid(8)
                .isSuspended(false)
                .isTerminated(false)
                .build();

        // record the demand
        demandTransformationJournal.recordAggregateDemand(bucketA, demandA, entityInfoA, 1.0);
        demandTransformationJournal.recordAggregateDemand(bucketA, demandA, entityInfoA, .5);
        demandTransformationJournal.recordAggregateDemand(bucketA, demandB, entityInfoB, 1.0);
        demandTransformationJournal.recordAggregateDemand(bucketA, demandB, entityInfoC, 1.0);
        demandTransformationJournal.recordAggregateDemand(bucketB, demandA, entityInfoA, 1.0);


        // get transformation result
        final DemandTransformationResult transformationResult =
                demandTransformationJournal.getTransformationResult();

        // setup expected result
        final AggregateDemandSegment expectedDemandSetA = AggregateDemandSegment.builder()
                .timeInterval(bucketA)
                .putAggregateCloudTierDemand(
                        demandA.cloudTierInfo(),
                        AggregateCloudTierDemand.builder()
                                .from(demandA)
                                .putDemandByEntity(entityInfoA, 1.5)
                                .build())
                .putAggregateCloudTierDemand(
                        demandB.cloudTierInfo(),
                        AggregateCloudTierDemand.builder()
                                .from(demandB)
                                .putDemandByEntity(entityInfoB, 1.0)
                                .putDemandByEntity(entityInfoC, 1.0)
                                .build())
                .build();
        final AggregateDemandSegment expectedDemandSetB = AggregateDemandSegment.builder()
                .timeInterval(bucketB)
                .putAggregateCloudTierDemand(
                        demandA.cloudTierInfo(),
                        AggregateCloudTierDemand.builder()
                                .from(demandA)
                                .putDemandByEntity(entityInfoA, 1.0)
                                .build())
                .build();


        // ASSERTIONS
        assertThat(transformationResult.aggregateDemandByBucket(), hasEntry(bucketA, expectedDemandSetA));
        assertThat(transformationResult.aggregateDemandByBucket(), hasEntry(bucketB, expectedDemandSetB));

        // Stats assertions
        final DemandTransformationStatistics transformationStats = transformationResult.transformationStats();
        assertThat(transformationStats.uniqueAggregateDemand(), equalTo(2L));
        assertThat(transformationStats.aggregatedEntitiesCount(), equalTo(3L));

        // bucket stats assertions
        assertThat(transformationStats.demandStatsByBucket(), hasKey(bucketA));
        final DoubleStatistics bucketAStats = transformationStats.demandStatsByBucket().get(bucketA);
        assertThat(bucketAStats.average(), closeTo(.875, TestUtils.ERROR_LIMIT));
        assertThat(bucketAStats.count(), equalTo(4L));
        assertThat(bucketAStats.max(), closeTo(1.0, TestUtils.ERROR_LIMIT));
        assertThat(bucketAStats.min(), closeTo(.5, TestUtils.ERROR_LIMIT));
        assertThat(bucketAStats.sum(), closeTo(3.5, TestUtils.ERROR_LIMIT));

        assertThat(transformationStats.demandStatsByBucket(), hasKey(bucketB));
        final DoubleStatistics bucketBStats = transformationStats.demandStatsByBucket().get(bucketB);
        assertThat(bucketBStats.average(), closeTo(1.0, TestUtils.ERROR_LIMIT));
        assertThat(bucketBStats.count(), equalTo(1L));
        assertThat(bucketBStats.max(), closeTo(1.0, TestUtils.ERROR_LIMIT));
        assertThat(bucketBStats.min(), closeTo(1.0, TestUtils.ERROR_LIMIT));
        assertThat(bucketBStats.sum(), closeTo(1.0, TestUtils.ERROR_LIMIT));

        // aggregate demand stats assertions
        assertThat(transformationStats.demandStatsByTierScope(), hasKey(demandA));
        final DoubleStatistics demandAStats = transformationStats.demandStatsByTierScope().get(demandA);
        assertThat(demandAStats.average(), closeTo(2.5 / 3, TestUtils.ERROR_LIMIT));
        assertThat(demandAStats.count(), equalTo(3L));
        assertThat(demandAStats.max(), closeTo(1.0, TestUtils.ERROR_LIMIT));
        assertThat(demandAStats.min(), closeTo(.5, TestUtils.ERROR_LIMIT));
        assertThat(demandAStats.sum(), closeTo(2.5, TestUtils.ERROR_LIMIT));

        assertThat(transformationStats.demandStatsByTierScope(), hasKey(demandB));
        final DoubleStatistics demandBStats = transformationStats.demandStatsByTierScope().get(demandB);
        assertThat(demandBStats.average(), closeTo(1.0, TestUtils.ERROR_LIMIT));
        assertThat(demandBStats.count(), equalTo(2L));
        assertThat(demandBStats.max(), closeTo(1.0, TestUtils.ERROR_LIMIT));
        assertThat(demandBStats.min(), closeTo(1.0, TestUtils.ERROR_LIMIT));
        assertThat(demandBStats.sum(), closeTo(2.0, TestUtils.ERROR_LIMIT));
    }

    @Test
    public void testSkippedEntities() {

        // setup entity aggregates
        ClassifiedEntityDemandAggregate entityAggregateA = ClassifiedEntityDemandAggregate.builder()
                .entityOid(1)
                .accountOid(2)
                .regionOid(3)
                .serviceProviderOid(4)
                .build();
        ClassifiedEntityDemandAggregate entityAggregateB = ClassifiedEntityDemandAggregate.builder()
                .from(entityAggregateA)
                .entityOid(5)
                .build();


        // record skipped entities
        demandTransformationJournal.recordSkippedEntity(entityAggregateA);
        demandTransformationJournal.recordSkippedEntity(entityAggregateB);

        // get aggregation result
        final DemandTransformationResult transformationResult = demandTransformationJournal.getTransformationResult();

        assertThat(transformationResult.transformationStats().skippedEntitiesCount(), equalTo(2L));
    }

    @Test
    public void testSkippedDemand() {

        final ClassifiedEntityDemandAggregate entityAggregate = ClassifiedEntityDemandAggregate.builder()
                .entityOid(1)
                .accountOid(2)
                .regionOid(3)
                .serviceProviderOid(4)
                .build();

        final DemandClassification demandClassification = DemandClassification.of(
                AllocatedDemandClassification.FLEXIBLY_ALLOCATED);

        final DemandTimeSeries seriesA = DemandTimeSeries.builder()
                .cloudTierDemand(ComputeTierDemand.builder()
                        .cloudTierOid(5)
                        .osType(OSType.RHEL)
                        .tenancy(Tenancy.DEFAULT)
                        .build())
                .demandIntervals(TimeSeries.newTimeline(
                        TimeInterval.builder()
                                .startTime(Instant.ofEpochSecond(0))
                                .endTime(Instant.ofEpochSecond(10))
                                .build()))
                .build();
        final DemandTimeSeries seriesB = DemandTimeSeries.builder()
                .cloudTierDemand(ComputeTierDemand.builder()
                        .cloudTierOid(6)
                        .osType(OSType.RHEL)
                        .tenancy(Tenancy.DEFAULT)
                        .build())
                .demandIntervals(TimeSeries.newTimeline(
                        TimeInterval.builder()
                                .startTime(Instant.ofEpochSecond(20))
                                .endTime(Instant.ofEpochSecond(40))
                                .build()))
                .build();

        demandTransformationJournal.recordSkippedDemand(
                entityAggregate,
                demandClassification,
                Sets.newHashSet(seriesA, seriesB));

        final DemandTransformationResult transformationResult = demandTransformationJournal.getTransformationResult();

        final DemandTransformationStatistics transformationStats = transformationResult.transformationStats();
        assertThat(transformationStats.skippedDemandDuration(), equalTo(Duration.ofSeconds(30)));
    }
}
