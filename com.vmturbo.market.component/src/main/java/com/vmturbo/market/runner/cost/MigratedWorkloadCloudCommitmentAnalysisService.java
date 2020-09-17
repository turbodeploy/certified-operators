package com.vmturbo.market.runner.cost;

import java.util.List;
import java.util.Optional;

import com.vmturbo.common.protobuf.cost.Cost;

/**
 * Defines the interface for interacting with the cost component's migrade workload cloud commitment analysis service.
 */
public interface MigratedWorkloadCloudCommitmentAnalysisService {
    /**
     * Starts a migrated workload cloud commitment analysis (Buy RI analysis).
     * @param businessAccountOid        The business account for which to buy RIs
     * @param topologyContextId         The plan topology ID
     * @param workloadPlacementList     A list of workload placements (VM, compute tier, and region)
     */
    void startAnalysis(long topologyContextId, Optional<Long> businessAccountOid, List<Cost.MigratedWorkloadCloudCommitmentAnalysisRequest.MigratedWorkloadPlacement> workloadPlacementList);
}
