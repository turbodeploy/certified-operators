package com.vmturbo.cloud.commitment.analysis.runtime.stages.persistence;

import java.util.Collection;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation.CloudCommitmentRecommendation;
import com.vmturbo.cloud.common.topology.MinimalCloudTopology;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;

/**
 * An interface for persisting {@link CloudCommitmentRecommendation} instances to a database.
 */
public interface CloudCommitmentRecommendationStore {

    /**
     * Persists {@code recommendations} to the underlying database store.
     * @param analysisInfo The analysis info.
     * @param recommendations The recommendations to persist.
     * @param cloudTopology The cloud topology, provided for any required translation of referenced
     *                      entities within the recommendations (e.g. to the cloud tier's name).
     */
    void persistRecommendations(@Nonnull CloudCommitmentAnalysisInfo analysisInfo,
                                @Nonnull Collection<CloudCommitmentRecommendation> recommendations,
                                @Nonnull MinimalCloudTopology<MinimalEntity> cloudTopology);
}
