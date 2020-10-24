package com.vmturbo.cloud.commitment.analysis.runtime.stages;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.TestUtils;
import com.vmturbo.cloud.commitment.analysis.demand.BoundedDuration;
import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.inventory.CloudCommitmentBoughtResolver;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage.StageResult;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.data.AnalysisTopology;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateAnalysisDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateDemandSegment;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AnalysisDemandCreator;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.DemandTransformationJournal.DemandTransformationResult;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.DemandTransformationStatistics;
import com.vmturbo.cloud.common.commitment.CloudCommitmentData;
import com.vmturbo.cloud.common.commitment.ReservedInstanceData;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentInventory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentInventory.CloudCommitment;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentInventory.CloudCommitmentCapacity;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo.ReservedInstanceBoughtCoupons;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Class to test the Cloud Commitment InventoryResolverStage.
 */
public class CloudCommitmentInventoryResolverStageTest {
    private CloudCommitmentInventoryResolverStage cloudCommitmentInventoryResolverStage;

    private final long id = 111111L;

    private CloudCommitmentBoughtResolver cloudCommitmentBoughtResolver;

    private final CloudCommitmentAnalysisContext analysisContext = mock(CloudCommitmentAnalysisContext.class);

    private CloudCommitmentAnalysisConfig cloudCommitmentAnalysisConfig;

    private final CloudCommitmentInventory cloudCommitmentInventory = CloudCommitmentInventory.newBuilder().addCloudCommitment(
            CloudCommitment.newBuilder().setType(CloudCommitmentType.RESERVED_INSTANCE).setCapacity(
                    CloudCommitmentCapacity.newBuilder().build())).build();

    private final TimeInterval analysisWindow = TimeInterval.builder()
            .startTime(Instant.ofEpochSecond(0))
            .endTime(Instant.ofEpochSecond(Duration.ofHours(3).getSeconds()))
            .build();

    private final BoundedDuration analysisBucket = BoundedDuration.builder()
            .amount(1)
            .unit(ChronoUnit.HOURS)
            .build();

    private final ReservedInstanceSpec spec1 = ReservedInstanceSpec.newBuilder()
            .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder().setTierId(10L).setOs(
                    OSType.LINUX).setPlatformFlexible(true).build()).build();

    private final ReservedInstanceSpec spec2 = ReservedInstanceSpec.newBuilder()
            .setReservedInstanceSpecInfo(ReservedInstanceSpecInfo.newBuilder().setTierId(20L).setOs(
                    OSType.WINDOWS).setPlatformFlexible(true).build()).build();

    /**
     * Setup the test.
     */
    @Before
    public void setup() {
        final CloudCommitmentAnalysisConfig.Builder analysisConfigBuilder = TestUtils.createBaseConfig().toBuilder();
        analysisConfigBuilder.setCloudCommitmentInventory(cloudCommitmentInventory);
        cloudCommitmentAnalysisConfig = analysisConfigBuilder.build();
        cloudCommitmentBoughtResolver = mock(CloudCommitmentBoughtResolver.class);
        cloudCommitmentInventoryResolverStage = new CloudCommitmentInventoryResolverStage(id, cloudCommitmentAnalysisConfig,
                analysisContext, cloudCommitmentBoughtResolver);
        List<CloudCommitmentData> cloudCommitmentDataList = new ArrayList<>();

        ReservedInstanceData cloudCommitmentData1 = ReservedInstanceData.builder().commitment(
                ReservedInstanceBought.newBuilder().setReservedInstanceBoughtInfo(
                        ReservedInstanceBoughtInfo.newBuilder().setBusinessAccountId(111L).setNumBought(4).setReservedInstanceSpec(45L)
                                .setDisplayName("cloudCommitmentBoughtData1").setReservedInstanceBoughtCoupons(
                                ReservedInstanceBoughtCoupons.newBuilder().setNumberOfCoupons(4).setNumberOfCouponsUsed(2).build())
                                .build()).setId(10L).build()).spec(spec1).build();
        cloudCommitmentDataList.add(cloudCommitmentData1);

        ReservedInstanceData cloudCommitmentData2 = ReservedInstanceData.builder().commitment(
                ReservedInstanceBought.newBuilder().setReservedInstanceBoughtInfo(
                        ReservedInstanceBoughtInfo.newBuilder().setBusinessAccountId(111L).setNumBought(8).setReservedInstanceSpec(45L)
                                .setDisplayName("cloudCommitmentBoughtData2").setReservedInstanceBoughtCoupons(
                                ReservedInstanceBoughtCoupons.newBuilder().setNumberOfCoupons(8).setNumberOfCouponsUsed(2).build())
                                .build()).setId(20L).build()).spec(spec2).build();
        cloudCommitmentDataList.add(cloudCommitmentData2);

        when(cloudCommitmentBoughtResolver.getCloudCommitment(cloudCommitmentInventory)).thenReturn(cloudCommitmentDataList);
    }

    /**
     * Test inventory retrieval.
     */
    @Test
    public void testInventoryRetrieval() {
        AggregateAnalysisDemand aggregateAnalysisDemand = AnalysisDemandCreator.createAnalysisDemand(createDemandTransformationResult(),
               analysisWindow, analysisBucket);
        StageResult<AnalysisTopology> output = cloudCommitmentInventoryResolverStage.execute(aggregateAnalysisDemand);
        Map<Long, CloudCommitmentData> cloudCommitmentDataMap = output.output().cloudCommitmentsByOid();

        assert (cloudCommitmentDataMap.get(10L).spec().equals(spec1));
        assert (cloudCommitmentDataMap.get(20L).spec().equals(spec2));

        ReservedInstanceData riData1 = cloudCommitmentDataMap.get(10L).asReservedInstance();
        assert (riData1.commitment().getReservedInstanceBoughtInfo().getNumBought() == 4);

        ReservedInstanceData riData2 = cloudCommitmentDataMap.get(20L).asReservedInstance();
        assert (riData2.commitment().getReservedInstanceBoughtInfo().getNumBought() == 8);
    }

    public DemandTransformationResult createDemandTransformationResult() {
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
        return transformationResult;
    }
}
