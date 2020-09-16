package com.vmturbo.cloud.commitment.analysis.runtime.stages.classification;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Lists;

import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.EntityCloudTierMapping;
import com.vmturbo.cloud.commitment.analysis.demand.EntityComputeTierAllocation;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableEntityComputeTierAllocation;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableTimeInterval;
import com.vmturbo.cloud.commitment.analysis.demand.TimeSeries;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.AllocatedDemandClassifier.AllocatedDemandClassifierFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate.DemandTimeSeries;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

public class AllocatedDemandClassifierTest {

    private final AllocatedDemandClassifierFactory allocatedDemandClassifierFactory =
            new AllocatedDemandClassifierFactory();


    private final CloudTierFamilyMatcher cloudTierFamilyMatcher = mock(CloudTierFamilyMatcher.class);

    @Test
    public void testClassifications() {

        final EntityComputeTierAllocation allocatedMapping = ImmutableEntityComputeTierAllocation.builder()
                .entityOid(1L)
                .accountOid(2L)
                .regionOid(3L)
                .serviceProviderOid(4L)
                .timeInterval(ImmutableTimeInterval.builder()
                        .startTime(Instant.now().minusSeconds(60))
                        .endTime(Instant.now())
                        .build())
                .cloudTierDemand(ComputeTierDemand.builder()
                        .cloudTierOid(5L)
                        .osType(OSType.RHEL)
                        .tenancy(Tenancy.DEFAULT)
                        .build())
                .build();

        // This demand should be classified as ephemeral
        final EntityComputeTierAllocation ephemeralMapping = ImmutableEntityComputeTierAllocation.copyOf(allocatedMapping)
                .withTimeInterval(ImmutableTimeInterval.builder()
                        // Have a gap between allocatedMapping and this one. Duration will be less
                        // than the target minimum
                        .startTime(allocatedMapping.timeInterval().startTime().minusSeconds(20))
                        .endTime(allocatedMapping.timeInterval().startTime().minusSeconds(10))
                        .build());

        // Coming prior to an ephemeral demand and allocated demand, this demand should be classified as
        // allocated
        final EntityComputeTierAllocation priorAllocation = ImmutableEntityComputeTierAllocation.copyOf((allocatedMapping))
                .withTimeInterval(ImmutableTimeInterval.builder()
                        .startTime(ephemeralMapping.timeInterval().startTime().minusSeconds(300))
                        .endTime(ephemeralMapping.timeInterval().startTime().minusSeconds(100))
                        .build());


        final EntityComputeTierAllocation flexiblyAllocatedA = ImmutableEntityComputeTierAllocation.copyOf((allocatedMapping))
                .withTimeInterval(ImmutableTimeInterval.builder()
                        .startTime(priorAllocation.timeInterval().startTime().minusSeconds(300))
                        .endTime(priorAllocation.timeInterval().startTime().minusSeconds(100))
                        .build())
                .withCloudTierDemand(ComputeTierDemand.builder().from(allocatedMapping.cloudTierDemand())
                        .cloudTierOid(allocatedMapping.cloudTierDemand().cloudTierOid() + 1)
                        .build());

        final EntityComputeTierAllocation flexiblyAllocatedB = ImmutableEntityComputeTierAllocation.copyOf((allocatedMapping))
                .withTimeInterval(ImmutableTimeInterval.builder()
                        .startTime(flexiblyAllocatedA.timeInterval().startTime().minusSeconds(300))
                        .endTime(flexiblyAllocatedA.timeInterval().startTime().minusSeconds(100))
                        .build())
                .withCloudTierDemand(ComputeTierDemand.builder().from(flexiblyAllocatedA.cloudTierDemand())
                        .cloudTierOid(flexiblyAllocatedA.cloudTierDemand().cloudTierOid() + 1)
                        .build());

        final EntityComputeTierAllocation staleAllocated = ImmutableEntityComputeTierAllocation.copyOf((allocatedMapping))
                .withTimeInterval(ImmutableTimeInterval.builder()
                        .startTime(flexiblyAllocatedB.timeInterval().startTime().minusSeconds(300))
                        .endTime(flexiblyAllocatedB.timeInterval().startTime().minusSeconds(100))
                        .build())
                .withCloudTierDemand(ComputeTierDemand.builder()
                        .from(allocatedMapping.cloudTierDemand())
                        .cloudTierOid(flexiblyAllocatedB.cloudTierDemand().cloudTierOid() + 1)
                        .build());

        // This demand matches allocated, but it comes prior to a stale classification and should be
        // classified as stale.
        final EntityComputeTierAllocation oldAllocation = ImmutableEntityComputeTierAllocation.copyOf((allocatedMapping))
                .withTimeInterval(ImmutableTimeInterval.builder()
                        .startTime(staleAllocated.timeInterval().startTime().minusSeconds(300))
                        .endTime(staleAllocated.timeInterval().startTime().minusSeconds(100))
                        .build());


        // setup the cloud tier family matcher
        when(cloudTierFamilyMatcher.match(eq(flexiblyAllocatedA), eq(allocatedMapping))).thenReturn(true);
        when(cloudTierFamilyMatcher.match(eq(flexiblyAllocatedB), eq(allocatedMapping))).thenReturn(true);
        when(cloudTierFamilyMatcher.match(eq(staleAllocated), eq(allocatedMapping))).thenReturn(false);

        // build the time series of demand
        final TimeSeries<EntityCloudTierMapping> entityDemandSeries = TimeSeries.newTimeSeries(
                Lists.newArrayList(allocatedMapping, ephemeralMapping, priorAllocation,
                        flexiblyAllocatedA, flexiblyAllocatedB, staleAllocated, oldAllocation));

        // invoke the demand classifier
        final AllocatedDemandClassifier allocatedDemandClassifier = allocatedDemandClassifierFactory.newClassifier(
                cloudTierFamilyMatcher, Duration.ofSeconds(20).toMillis());
        final ClassifiedCloudTierDemand actualClassifications =
                allocatedDemandClassifier.classifyEntityDemand(entityDemandSeries);

        // setup the expected output
        // Allocated
        final DemandTimeSeries allocatedDemandTimeSeries = DemandTimeSeries
                .builder()
                .cloudTierDemand(allocatedMapping.cloudTierDemand())
                .demandIntervals(TimeSeries.newTimeline(
                        Lists.newArrayList(
                                allocatedMapping.timeInterval(),
                                priorAllocation.timeInterval())))
                .build();

        // Ephemeral
        final DemandTimeSeries ephemeralDemandTimeSeries = DemandTimeSeries.builder()
                .cloudTierDemand(ephemeralMapping.cloudTierDemand())
                .demandIntervals(TimeSeries.newTimeline(
                        Lists.newArrayList(
                                ephemeralMapping.timeInterval())))
                .build();

        // Flexibly allocated
        final DemandTimeSeries flexibleDemandTimeSeriesA = DemandTimeSeries.builder()
                .cloudTierDemand(flexiblyAllocatedA.cloudTierDemand())
                .demandIntervals(TimeSeries.newTimeline(
                        Lists.newArrayList(
                                flexiblyAllocatedA.timeInterval())))
                .build();
        final DemandTimeSeries flexibleDemandTimeSeriesB = DemandTimeSeries.builder()
                .cloudTierDemand(flexiblyAllocatedB.cloudTierDemand())
                .demandIntervals(TimeSeries.newTimeline(
                        Lists.newArrayList(
                                flexiblyAllocatedB.timeInterval())))
                .build();

        // Stale
        final DemandTimeSeries staleDemandTimeSeries = DemandTimeSeries.builder()
                .cloudTierDemand(staleAllocated.cloudTierDemand())
                .demandIntervals(TimeSeries.newTimeline(
                        Lists.newArrayList(
                                staleAllocated.timeInterval())))
                .build();

        final DemandTimeSeries oldDemandTimeSeries = DemandTimeSeries.builder()
                .cloudTierDemand(oldAllocation.cloudTierDemand())
                .demandIntervals(TimeSeries.newTimeline(
                        Lists.newArrayList(
                                oldAllocation.timeInterval())))
                .build();

        // Asertions
        final Map<DemandClassification, Set<DemandTimeSeries>> classifiedDemand =
                actualClassifications.classifiedDemand();

        assertThat(actualClassifications.allocatedDemand(), equalTo(Optional.of(
                DemandTimeSeries
                        .builder()
                        .cloudTierDemand(allocatedMapping.cloudTierDemand())
                        .demandIntervals(TimeSeries.singletonTimeline(allocatedMapping.timeInterval()))
                        .build())));

        final DemandClassification allocatedClassification =
                DemandClassification.of(AllocatedDemandClassification.ALLOCATED);
        assertTrue(classifiedDemand.containsKey(allocatedClassification));
        assertThat(classifiedDemand.get(allocatedClassification), containsInAnyOrder(allocatedDemandTimeSeries));

        final DemandClassification ephemeralClassification =
                DemandClassification.of(AllocatedDemandClassification.EPHEMERAL);
        assertTrue(classifiedDemand.containsKey(ephemeralClassification));
        assertThat(classifiedDemand.get(ephemeralClassification), containsInAnyOrder(ephemeralDemandTimeSeries));

        final DemandClassification flexibleClassification =
                DemandClassification.of(AllocatedDemandClassification.FLEXIBLY_ALLOCATED);
        assertTrue(classifiedDemand.containsKey(flexibleClassification));
        assertThat(classifiedDemand.get(flexibleClassification),
                containsInAnyOrder(flexibleDemandTimeSeriesA, flexibleDemandTimeSeriesB));

        final DemandClassification staleClassification =
                DemandClassification.of(AllocatedDemandClassification.STALE_ALLOCATED);
        assertTrue(classifiedDemand.containsKey(staleClassification));
        assertThat(classifiedDemand.get(staleClassification),
                containsInAnyOrder(staleDemandTimeSeries, oldDemandTimeSeries));
    }
}
