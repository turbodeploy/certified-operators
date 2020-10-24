package com.vmturbo.cloud.commitment.analysis.runtime.stages.retrieval;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.EntityCloudTierMapping;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableEntityCloudTierMapping;
import com.vmturbo.cloud.commitment.analysis.persistence.CloudCommitmentDemandReader;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.retrieval.DemandRetrievalStage.DemandRetrievalFactory;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.topology.MinimalCloudTopology;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandSelection;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentInventory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.RecommendationSettings;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandScope;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandSelection;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.CloudTierType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Testing the demand selection stage.
 */
public class DemandRetrievalStageTest {

    private final long id = 123L;

    private final CloudCommitmentAnalysisContext analysisContext = mock(CloudCommitmentAnalysisContext.class);

    private final CloudCommitmentDemandReader demandReader = mock(CloudCommitmentDemandReader.class);

    private final DemandRetrievalFactory demandRetrievalFactory = new DemandRetrievalFactory(demandReader);

    private final MinimalCloudTopology cloudTopology = mock(MinimalCloudTopology.class);

    @Before
    public void setup() {
        when(analysisContext.getSourceCloudTopology()).thenReturn(cloudTopology);
        when(cloudTopology.getBillingFamilyForAccount(anyLong())).thenReturn(Optional.empty());
    }

    /**
     * Testing execution with demand trimming and allocated demand retrieval
     */
    @Test
    public void testExecutionWithDemandTrimming() throws Exception {

        // setup analysis config for stage construction
        final Instant lookbackStartTime = Instant.now().minus(10, ChronoUnit.DAYS);
        final AllocatedDemandSelection allocatedSelection = AllocatedDemandSelection.newBuilder()
                .setIncludeFlexibleDemand(true)
                .setDemandSelection(DemandSelection.newBuilder()
                        .setScope(DemandScope.newBuilder()
                                .addAccountOid(1L)))
                .build();
        final HistoricalDemandSelection demandSelection = HistoricalDemandSelection.newBuilder()
                .setCloudTierType(CloudTierType.COMPUTE_TIER)
                .setAllocatedSelection(allocatedSelection)
                .setLogDetailedSummary(true)
                // make sure demand selection isn't using this value. It should be using the normalized
                // value from the config
                .setLookBackStartTime(lookbackStartTime.plusSeconds(32312).toEpochMilli())
                .build();

        final CloudCommitmentAnalysisConfig analysisConfig = CloudCommitmentAnalysisConfig.newBuilder()
                .setDemandSelection(demandSelection)

                .setCloudCommitmentInventory(CloudCommitmentInventory.newBuilder())
                .setPurchaseProfile(CommitmentPurchaseProfile.newBuilder()
                        .setRecommendationSettings(RecommendationSettings.newBuilder()))
                .build();

        // setup the lookback time in the context
        when(analysisContext.getAnalysisWindow()).thenReturn(Optional.of(
                TimeInterval.builder()
                        .startTime(lookbackStartTime)
                        .endTime(Instant.now())
                        .build()));

        // construct the stage
        final AnalysisStage<Void, EntityCloudTierDemandSet> demandRetrievalStage = demandRetrievalFactory.createStage(
                id, analysisConfig, analysisContext);


        // set demand reader response
        final EntityCloudTierMapping entityCloudTierMappingA = ImmutableEntityCloudTierMapping.builder()
                .timeInterval(TimeInterval.builder()
                        .startTime(lookbackStartTime.minus(3, ChronoUnit.DAYS))
                        .endTime(lookbackStartTime.plus(1, ChronoUnit.DAYS))
                        .build())
                .entityOid(1L)
                .accountOid(2L)
                .regionOid(3L)
                .serviceProviderOid(4L)
                .cloudTierDemand(ComputeTierDemand.builder()
                        .cloudTierOid(5L)
                        .osType(OSType.LINUX)
                        .tenancy(Tenancy.DEFAULT)
                        .build())
                .build();

        final EntityCloudTierMapping entityCloudTierMappingB = ImmutableEntityCloudTierMapping.builder()
                .timeInterval(TimeInterval.builder()
                        .startTime(lookbackStartTime.plus(1, ChronoUnit.DAYS))
                        .endTime(Instant.now())
                        .build())
                .entityOid(1L)
                .accountOid(2L)
                .regionOid(3L)
                .serviceProviderOid(4L)
                .cloudTierDemand(ComputeTierDemand.builder()
                        .cloudTierOid(5L)
                        .osType(OSType.LINUX)
                        .tenancy(Tenancy.DEFAULT)
                        .build())
                .build();

        when(demandReader.getAllocationDemand(
                eq(CloudTierType.COMPUTE_TIER),
                eq(allocatedSelection.getDemandSelection().getScope()),
                eq(lookbackStartTime))).thenReturn(Stream.of(entityCloudTierMappingA, entityCloudTierMappingB));

        // invoke the stage
        final AnalysisStage.StageResult<EntityCloudTierDemandSet> stageResult =
                demandRetrievalStage.execute(null);


        // setup expected output
        final EntityCloudTierMapping expectedMappingA = ImmutableEntityCloudTierMapping.copyOf(entityCloudTierMappingA)
                .withTimeInterval(entityCloudTierMappingA.timeInterval()
                        .toBuilder()
                        .startTime(lookbackStartTime)
                        .build());
        final EntityCloudTierMapping expectedMappingB = entityCloudTierMappingB;


        assertThat(stageResult.output().allocatedDemand(), hasSize(2));
        assertThat(stageResult.output().allocatedDemand(), containsInAnyOrder(expectedMappingA, expectedMappingB));

    }
}
