package com.vmturbo.cloud.commitment.analysis.runtime.stages;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.TestUtils;
import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.AnalysisStage;
import com.vmturbo.cloud.commitment.analysis.runtime.CloudCommitmentAnalysisContext;
import com.vmturbo.cloud.commitment.analysis.runtime.data.CloudTierCoverageDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand.EntityInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateDemandSegment;
import com.vmturbo.cloud.commitment.analysis.spec.CCARecommendationSpecMatcherStage.CCARecommendationSpecMatcherStageFactory;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecMatcher;
import com.vmturbo.cloud.commitment.analysis.spec.CommitmentSpecDemand;
import com.vmturbo.cloud.commitment.analysis.spec.CommitmentSpecDemandSet;
import com.vmturbo.cloud.commitment.analysis.spec.ImmutableReservedInstanceSpecData;
import com.vmturbo.cloud.commitment.analysis.spec.ReservedInstanceSpecData;
import com.vmturbo.cloud.commitment.analysis.spec.ReservedInstanceSpecMatcher;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.RecommendationSettings;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.ReservedInstancePurchaseProfile;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpecInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.PaymentOption;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Class for testing the CCARecommendationSpecMatcherStage.
 */
public class CCARecommendationSpecMatcherStageTest {

    private final CloudCommitmentAnalysisContext analysisContext = mock(CloudCommitmentAnalysisContext.class);

    private ImmutableMap<Long, ReservedInstanceType> purchaseConstraints;

    private static final int id = 11;

    private static final long REGION_AWS = 111;

    private static final long REGION_AZURE = 11L;

    private static final long Account_Aws = 123L;

    private static final long awsServiceProviderId = 55555L;

    private static final long computeTierOid = 12L;

    private static final CloudTierCoverageDemand cloudTierCoverageDemand = mock(CloudTierCoverageDemand.class);

    private static final AggregateDemandSegment classifiedDemandSegment = mock(AggregateDemandSegment.class);

    private static final ComputeTierDemand computeTierDemand = ComputeTierDemand.builder()
            .cloudTierOid(computeTierOid).osType(OSType.LINUX).tenancy(Tenancy.DEFAULT).build();

    private static final AggregateCloudTierDemand cloudTierDemand = AggregateCloudTierDemand.builder()
            .putDemandByEntity(EntityInfo.builder().entityOid(12L).build(), .5)
            .putDemandByEntity(EntityInfo.builder().entityOid(13L).build(), .5)
            .putDemandByEntity(EntityInfo.builder().entityOid(14L).build(), .5)
            .putDemandByEntity(EntityInfo.builder().entityOid(15L).build(), .5)
            .accountOid(Account_Aws).regionOid(REGION_AWS).serviceProviderOid(awsServiceProviderId)
            .cloudTierDemand(computeTierDemand)
            .classification(DemandClassification.of(AllocatedDemandClassification.ALLOCATED)).build();

    private static final CloudCommitmentSpecMatcher reservedInstanceSpecMatcher = mock(ReservedInstanceSpecMatcher.class);

    private static final TopologyEntityDTO computeTier = TopologyEntityDTO.newBuilder().setEnvironmentType(EnvironmentType.CLOUD)
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setOid(computeTierOid)
            .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setComputeTier(
                    ComputeTierInfo.newBuilder().setNumCoupons(8).build()).build())
            .setDisplayName("tier1")
            .build();

    Optional<ReservedInstanceSpecData> riSpecData = Optional.of(ImmutableReservedInstanceSpecData.builder()
            .spec(ReservedInstanceSpec.newBuilder().setReservedInstanceSpecInfo(
                    ReservedInstanceSpecInfo.newBuilder().setOs(OSType.LINUX).setTenancy(Tenancy.DEFAULT)
                            .setRegionId(REGION_AWS).setTierId(12L).build()).build()).cloudTier(computeTier).build());


    /**
     * Setuo the test.
     */
    @Before
    public void setup() {
        ReservedInstanceType awsConstraints = ReservedInstanceType.newBuilder().setOfferingClass(
                OfferingClass.STANDARD)
                .setPaymentOption(PaymentOption.PARTIAL_UPFRONT).setTermYears(1).build();

        ReservedInstanceType azureConstraints = ReservedInstanceType.newBuilder().setOfferingClass(OfferingClass.CONVERTIBLE)
                .setPaymentOption(PaymentOption.ALL_UPFRONT).setTermYears(3).build();

        purchaseConstraints =
                ImmutableMap.of(REGION_AWS, awsConstraints,
                        REGION_AZURE, azureConstraints);
        when(analysisContext.getCloudCommitmentSpecMatcher()).thenReturn(reservedInstanceSpecMatcher);
    }

    /**
     * Test the execution of the CCARecommendationSpecMatcherStage.
     */
    @Test
    public void testExecution() throws Exception {

        final CloudCommitmentAnalysisConfig analysisConfig = TestUtils.createBaseConfig().toBuilder()
                .setPurchaseProfile(CommitmentPurchaseProfile.newBuilder()
                        .setRiPurchaseProfile(ReservedInstancePurchaseProfile.newBuilder().putAllRiTypeByRegionOid(purchaseConstraints).build())
                        .setRecommendationSettings(RecommendationSettings.newBuilder()))
                .build();

        when(classifiedDemandSegment.aggregateCloudTierDemand()).thenReturn(Collections.singleton(cloudTierDemand));
        when(cloudTierCoverageDemand.demandSegment()).thenReturn(classifiedDemandSegment);

        CCARecommendationSpecMatcherStageFactory ccaRecommendationSpecMatcherStageFactory = new CCARecommendationSpecMatcherStageFactory();
        when(reservedInstanceSpecMatcher.matchDemandToSpecs(cloudTierDemand)).thenReturn(riSpecData);
        final AnalysisStage ccaRecommendationSpecMatcherStage = ccaRecommendationSpecMatcherStageFactory
                .createStage(id, analysisConfig, analysisContext);

        final AnalysisStage.StageResult<CommitmentSpecDemandSet> result = ccaRecommendationSpecMatcherStage.execute(cloudTierCoverageDemand);
        CommitmentSpecDemandSet output = result.output();

        assert (!output.commitmentSpecDemand().isEmpty());

        Optional<CommitmentSpecDemand> commitmentSpecDemand = output.commitmentSpecDemand().stream().findFirst();
        assert (commitmentSpecDemand.isPresent());
        Set<AggregateCloudTierDemand> aggregateCloudTierDemand = commitmentSpecDemand.get().aggregateCloudTierDemandSet();
        assert (!aggregateCloudTierDemand.isEmpty());
        aggregateCloudTierDemand.iterator().next();
        assert (aggregateCloudTierDemand.iterator().next().equals(cloudTierDemand));
    }
}
