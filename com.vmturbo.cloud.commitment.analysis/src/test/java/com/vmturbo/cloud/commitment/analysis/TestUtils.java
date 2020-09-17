package com.vmturbo.cloud.commitment.analysis;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandSelection;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentInventory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.ReservedInstancePurchaseProfile;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandClassificationSettings;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandScope;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandSelection;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.CloudTierType;

/**
 * Utility class for cca tests.
 */
public class TestUtils {

    /**
     * The error limit allowed in comparing doubles.
     */
    public static final double ERROR_LIMIT = .000001;

    private TestUtils() {}

    /**
     * Create a base cca config.
     *
     * @return The cloud commitment analysis config.
     */
    public static CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig createBaseConfig() {
        return CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig.newBuilder()
                .setDemandSelection(HistoricalDemandSelection.newBuilder()
                        .setCloudTierType(CloudTierType.COMPUTE_TIER)
                        .setAllocatedSelection(AllocatedDemandSelection.newBuilder()
                                .setDemandSelection(DemandSelection.newBuilder()
                                        .setScope(DemandScope.newBuilder())))
                        .setLookBackStartTime(Instant.now().minus(30, ChronoUnit.DAYS).toEpochMilli())
                        .setLogDetailedSummary(true))
                .setDemandClassificationSettings(DemandClassificationSettings.newBuilder().build().newBuilder()
                        .setLogDetailedSummary(true))
                .setCloudCommitmentInventory(CloudCommitmentInventory.newBuilder())
                .setPurchaseProfile(CommitmentPurchaseProfile.newBuilder()
                        .setAllocatedSelection(AllocatedDemandSelection.newBuilder()
                                .setDemandSelection(DemandSelection.newBuilder()
                                        .setScope(DemandScope.newBuilder())))
                        .setRiPurchaseProfile(ReservedInstancePurchaseProfile.newBuilder()))
                .build();
    }
}
