package com.vmturbo.cloud.commitment.analysis;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.ClassifiedDemandScope;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentInventory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.RecommendationSettings;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.ReservedInstancePurchaseProfile;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandClassification;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandClassification.ClassifiedDemandSelection;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandScope;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.CloudTierType;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.DemandSegment;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandType;

public class TestUtils {

    private TestUtils() {}

    public static CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig createBaseConfig() {
        return CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig.newBuilder()
                .setDemandSelection(HistoricalDemandSelection.newBuilder()
                        .setCloudTierType(CloudTierType.COMPUTE_TIER)
                        .addDemandSegment(DemandSegment.newBuilder()
                                .setScope(DemandScope.newBuilder())
                                .setDemandType(HistoricalDemandType.ALLOCATION)
                                .build())
                        .setLookBackStartTime(Instant.now().minus(30, ChronoUnit.DAYS).toEpochMilli())
                        .setLogDetailedSummary(true))
                .setDemandClassification(DemandClassification.newBuilder()
                        .setDemandSelection(ClassifiedDemandSelection.newBuilder()
                                .addScope(ClassifiedDemandScope.newBuilder()
                                        .setScope(DemandScope.newBuilder())
                                        .addAllocatedDemandClassification(AllocatedDemandClassification.ALLOCATED))))
                .setCloudCommitmentInventory(CloudCommitmentInventory.newBuilder())
                .setPurchaseProfile(CommitmentPurchaseProfile.newBuilder()
                        .addScope(ClassifiedDemandScope.newBuilder()
                                .setScope(DemandScope.newBuilder())
                                .addAllocatedDemandClassification(AllocatedDemandClassification.ALLOCATED))
                        .setRecommendationSettings(RecommendationSettings.newBuilder())
                        .setRiPurchaseProfile(ReservedInstancePurchaseProfile.newBuilder()))
                .build();
    }
}
