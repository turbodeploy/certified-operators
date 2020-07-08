package com.vmturbo.market.runner.cost;

import java.util.List;

import com.vmturbo.common.protobuf.cost.Cost;

/**
 * Defines the interface for interacting with the cost component's migrade workload cloud commitment analysis service.
 */
public interface MigratedWorkloadCloudCommitmentAnalysisService {
    /**
     * Starts a migrated workload cloud commitment analysis (Buy RI analysis).
     * @param topologyContextId         The plan topology ID
     * @param workloadPlacementList     A list of workload placements (VM, compute tier, and region)
     */
    void startAnalysis(long topologyContextId, List<Cost.MigratedWorkloadCloudCommitmentAnalysisRequest.MigratedWorkloadPlacement> workloadPlacementList);
}
