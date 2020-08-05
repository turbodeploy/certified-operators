package com.vmturbo.common.protobuf.utils;

/**
 * Represents features provided by probes.
 */
public enum ProbeFeature {

    /**
     * Discovery feature (includes all discovery types, like incremental, performance and so on).
     */
    DISCOVERY,

    /**
     * SupplyChain feature.
     */
    SUPPLY_CHAIN,

    /**
     * ActionExecution feature.
     */
    ACTION_EXECUTION,

    /**
     * ActionAudit feature.
     */
    ACTION_AUDIT,

    /**
     * ActionApproval feature.
     */
    ACTION_APPROVAL,

    /**
     * PlanExport feature.
     */
    PLAN_EXPORT
}
