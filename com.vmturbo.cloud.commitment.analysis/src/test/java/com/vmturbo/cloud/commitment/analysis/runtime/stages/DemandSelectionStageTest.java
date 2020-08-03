package com.vmturbo.cloud.commitment.analysis.runtime.stages;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.collect.Lists;

import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.demand.EntityCloudTierMapping;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableEntityCloudTierMapping;
import com.vmturbo.cloud.commitment.analysis.persistence.CloudCommitmentDemandReader;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.DemandSelectionStage.DemandSelectionFactory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentInventory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.RecommendationSettings;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandClassification;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandClassification.ClassifiedDemandSelection;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandScope;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.CloudTierType;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.DemandSegment;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandType;

/**
 * Testing the demand selection stage.
 */
public class DemandSelectionStageTest {

    private final long id = 123L;

    private final CloudCommitmentAnalysisContext analysisContext = mock(CloudCommitmentAnalysisContext.class);

    private final CloudCommitmentDemandReader demandReader = mock(CloudCommitmentDemandReader.class);

    private final DemandSelectionFactory demandSelectionFactory = new DemandSelectionFactory(demandReader);

    /**
     * Testing execution with demand trimming.
     */
    @Test
    public void testExecutionWithDemandTrimming() {

        // setup analysis config for stage construction
        final Instant lookbackStartTime = Instant.now().minus(10, ChronoUnit.DAYS);
        final DemandSegment demandSegmentA = DemandSegment.newBuilder()
                .setDemandType(HistoricalDemandType.ALLOCATION)
                .setScope(DemandScope.newBuilder()
                        .addAccountOid(1L)
                        .build())
                .build();
        final DemandSegment demandSegmentB = DemandSegment.newBuilder()
                .setDemandType(HistoricalDemandType.ALLOCATION)
                .setScope(DemandScope.newBuilder()
                        .addRegionOid(2L)
                        .build())
                .build();
        final HistoricalDemandSelection demandSelection = HistoricalDemandSelection.newBuilder()
                .setCloudTierType(CloudTierType.COMPUTE_TIER)
                .addDemandSegment(demandSegmentA)
                .addDemandSegment(demandSegmentB)
                .setLogDetailedSummary(true)
                // make sure demand selection isn't using this value. It should be using the normalized
                // value from the config
                .setLookBackStartTime(lookbackStartTime.plusSeconds(32312).toEpochMilli())
                .build();

        final CloudCommitmentAnalysisConfig analysisConfig = CloudCommitmentAnalysisConfig.newBuilder()
                .setDemandSelection(demandSelection)
                .setDemandClassification(DemandClassification.newBuilder()
                        .setDemandSelection(ClassifiedDemandSelection.newBuilder()))
                .setCloudCommitmentInventory(CloudCommitmentInventory.newBuilder())
                .setPurchaseProfile(CommitmentPurchaseProfile.newBuilder()
                        .setRecommendationSettings(RecommendationSettings.newBuilder()))
                .build();

        // setup the lookback time in the context
        when(analysisContext.getAnalysisStartTime()).thenReturn(Optional.of(lookbackStartTime));

        // construct the stage
        final AnalysisStage demandSelectionStage = demandSelectionFactory.createStage(
                id, analysisConfig, analysisContext);


        // set demand reader response
        final EntityCloudTierMapping entityCloudTierMappingA = ImmutableEntityCloudTierMapping.builder()
                .startTime(lookbackStartTime.minus(3, ChronoUnit.DAYS))
                .endTime(lookbackStartTime.plus(1, ChronoUnit.DAYS))
                .entityOid(1L)
                .accountOid(2L)
                .regionOid(3L)
                .serviceProviderOid(4L)
                .cloudTierType(CloudTierType.COMPUTE_TIER)
                .cloudTierDemand(new Object())
                .build();

        final EntityCloudTierMapping entityCloudTierMappingB = ImmutableEntityCloudTierMapping.builder()
                .startTime(lookbackStartTime.plus(1, ChronoUnit.DAYS))
                .endTime(Instant.now())
                .entityOid(1L)
                .accountOid(2L)
                .regionOid(3L)
                .serviceProviderOid(4L)
                .cloudTierType(CloudTierType.COMPUTE_TIER)
                .cloudTierDemand(new Object())
                .build();

        when(demandReader.getDemand(
                eq(CloudTierType.COMPUTE_TIER),
                eq(Lists.newArrayList(demandSegmentA, demandSegmentB)),
                eq(lookbackStartTime))).thenReturn(Stream.of(entityCloudTierMappingA, entityCloudTierMappingB));

        // invoke the stage
        final AnalysisStage.StageResult<Set<EntityCloudTierMapping<?>>> stageResult =
                demandSelectionStage.execute(null);


        // setup expected output
        final EntityCloudTierMapping expectedMappingA = ImmutableEntityCloudTierMapping.copyOf(entityCloudTierMappingA)
                .withStartTime(lookbackStartTime);
        final EntityCloudTierMapping expectedMappingB = entityCloudTierMappingB;


        assertThat(stageResult.output(), hasSize(2));
        assertThat(stageResult.output(), containsInAnyOrder(expectedMappingA, expectedMappingB));

    }
}
