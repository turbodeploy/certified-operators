package com.vmturbo.action.orchestrator.store;

import javax.annotation.Nonnull;

/**
 * An interface for factories that create action stores.
 */
public interface IActionStoreFactory {
    /**
     * Create a new {@link ActionStore} instance. The type of store created may be determined
     * by the topologyContextId.
     *
     * @param topologyContextId The id of the topology context that this store will be associated with.
     *                          The store created may be determined by consideration of this context.
     * @return A new {@link ActionStore} instance.
     */
    @Nonnull
    ActionStore newStore(long topologyContextId);

    /**
     * Get the name of the type of context this ID belongs to. For example "plan" or "live".
     *
     * @param topologyContextId The id of the topology context whose type name should be retrieved.
     * @return "plan" or "live" depending on if the ID belongs to a plan or live context.
     */
    @Nonnull
    String getContextTypeName(long topologyContextId);
}
