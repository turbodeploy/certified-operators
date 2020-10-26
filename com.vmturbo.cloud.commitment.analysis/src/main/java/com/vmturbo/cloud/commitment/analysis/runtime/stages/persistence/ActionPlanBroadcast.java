package com.vmturbo.cloud.commitment.analysis.runtime.stages.persistence;

import java.util.List;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.CloudCommitmentRecommendation;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;

/**
 * An interface for publishing cloud commitment recommendations on a pub/sub channel.
 */
public interface ActionPlanBroadcast {

    /**
     * Sends a notification containing the provided {@code recommendations}.
     * @param analysisInfo Analysis info.
     * @param recommendations The recommendations to broadcast.
     * @throws Exception An unrecoverable exception during the broadcast.
     */
    void sendNotification(@Nonnull CloudCommitmentAnalysisInfo analysisInfo,
                          @Nonnull List<CloudCommitmentRecommendation> recommendations) throws Exception;
}
