package com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm;

import java.util.List;

import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.cost.Cost.MigratedWorkloadCloudCommitmentAnalysisRequest.MigratedWorkloadPlacement;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.RIProviderSetting;
import com.vmturbo.common.protobuf.search.CloudType;

/**
 * Strategy interface for implementing a migrated workflow cloud commitment (Buy RI) analysis. The purpose for
 * implementing a strategy design pattern is that the initial implementation of the algorithm will mirror that
 * in classic, but we eventually want to integrate the migrate to public cloud Buy RI analysis with the algorithm
 * currently under development. Defining a strategy with a common interface will allow us to swap out the implementation
 * in the future through a Spring Config change.
 */
public interface MigratedWorkloadCloudCommitmentAlgorithmStrategy {
    /**
     * Performs the analysis of our input data and generates Buy RI recommendations.
     *
     * @param migratedWorkloads        The workloads that are being migrated as part of a migrate to cloud plan
     * @param masterBusinessAccountOid The master business account for which to buy RIs
     * @param cloudType                The cloud service provider
     * @param riProviderSetting        The RI provider settings specifying the type of RIs to buy
     * @param topologyContextId        The topology context with which to associate the actions
     * @return A list of Buy RI actions for these workloads
     */
    List<Action> analyze(List<MigratedWorkloadPlacement> migratedWorkloads,
                         Long masterBusinessAccountOid,
                         CloudType cloudType,
                         RIProviderSetting riProviderSetting,
                         Long topologyContextId);
}
