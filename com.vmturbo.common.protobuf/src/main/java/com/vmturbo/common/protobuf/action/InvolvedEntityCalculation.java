package com.vmturbo.common.protobuf.action;

/**
 * Depending on the context, involved entities need to be handled differently. See
 * {@link InvolvedEntityCalculation#INCLUDE_SOURCE_PROVIDERS_WITH_RISKS} for an examples.
 */
public enum InvolvedEntityCalculation {

    /**
     * If an the entity is involved with the action in anyway, we included it. This includes
     * but not limited to: target, source, destination, resource, compute tier, workload tier,
     * region, master account, etc.
     *
     * <p/> Does NOT include entities involved through actions merged into an atomic action.
     * Involving an entity in this calculation means that the action will show up on
     * that entity's page in the UI.
     */
    INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES,

    /**
     * If an the entity is involved with the action in anyway, we included it. This includes
     * but not limited to: target, source, destination, resource, compute tier, workload tier,
     * region, master account, etc.
     *
     * <p/> DOES include entities involved through actions merged into an atomic action.
     * Involving an entity in this calculation allows details of those entity's to be returned
     * in API queries for the action (ie the display name of an entity involved in a
     * merged action).
     */
    INCLUDE_ALL_MERGED_INVOLVED_ENTITIES,

    /**
     * When expanding the involved entities to find related actions, it does not make sense
     * to include all the actions that contain one of the entities. After expanding, it matters
     * how the entity participates in the action. Here, we look for include involved entities in
     * an action that would affect the involved entities before expansion. We achieve this by
     * looking for involved entities in target, source, and resource of an action.
     *
     * <p>In the below examples we the involved entity is a BApp. We expand it to include
     * all the entities below it, including PM_Host that hosts the VM_Host that hosts the BApp.</p>
     * <ul>
     * <li> For move VM_other from PM_Host to PM_Other, we should include the action. This action
     * relates to the BApp because the BApp might be at risk due to the congestion on PM_Host.
     * If BApp was hosted on PM_Other instead, then BApp is unaffected by the move
     * action, because the Market will recommend actions that are safe for the destination.</li>
     * <li>For move VM_other from PM_Host to PM_Other due to compliance, we should not include the action.
     * Even though the BApp is hosted on PM_Host, the compliance issue has nothing to do with
     * the BApp. However, if BApp was hosted on VM_other, then the compliance issue puts the
     * BApp at risk.</li>
     * </ul>
     */
    INCLUDE_SOURCE_PROVIDERS_WITH_RISKS
}
