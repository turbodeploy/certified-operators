package com.vmturbo.cost.component.savings;

import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.cost.component.savings.DataInjectionMonitor.ScriptEvent;

/**
 * Script event injection support.
 */
public interface ScenarioDataInjector {
    /**
     * Get the script event class.
     *
     * @return the script event class
     */
    Class getScriptEventClass();

    /**
     * Handle script events.
     *
     * @param scriptEvents list of script events
     * @param uuidMap UUID map for getting OIDs from entity display names
     * @param purgePreviousTestState true if the stats and entity for the entities in the UUID list
     * @param actionChains action chains (output parameter)
     * @param billRecordsByEntity bill records (output parameter)
     */
    void handleScriptEvents(@Nonnull List<ScriptEvent> scriptEvents,
            @Nonnull Map<String, Long> uuidMap,
            @Nonnull AtomicBoolean purgePreviousTestState,
            @Nonnull Map<Long, NavigableSet<ActionSpec>> actionChains,
            @Nonnull Map<Long, Set<BillingRecord>> billRecordsByEntity);
}
