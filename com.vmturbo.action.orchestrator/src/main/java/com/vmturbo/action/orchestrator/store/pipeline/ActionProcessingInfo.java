package com.vmturbo.action.orchestrator.store.pipeline;

/**
 * Information about processed actions. The output of an {@link ActionPipeline}.
 */
public class ActionProcessingInfo {
    private final int actionStoreSize;

    /**
     * Create a new {@link ActionProcessingInfo}.
     *
     * @param actionStoreSize The size of the actionStore after processing the action plan.
     */
    public ActionProcessingInfo(final int actionStoreSize) {
        this.actionStoreSize = actionStoreSize;
    }

    /**
     * Get the size of the action store after processing the action plan.
     *
     * @return the size of the action store after processing the action plan.
     */
    public int getActionStoreSize() {
        return actionStoreSize;
    }
}
