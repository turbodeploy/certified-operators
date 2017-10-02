package com.vmturbo.action.orchestrator.store;

import java.util.List;

/**
 * An interface for loading action stores that were previously persisted.
 */
public interface IActionStoreLoader {
    /**
     * Load previously persisted action stores.
     *
     * @return The list of previously persisted action stores.
     */
    List<ActionStore> loadActionStores();
}
