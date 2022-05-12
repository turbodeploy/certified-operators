package com.vmturbo.cost.component.savings;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.cost.component.savings.DataInjectionMonitor.ScriptEvent;

/**
 * Translator for injected test data script events.
 */
public class BillingDataInjector implements ScenarioDataInjector {
    /**
     * Logger.
     */
    private static final Logger logger = LogManager.getLogger();

    /**
     * Event format passed between the data generator and the event injector.
     */
    public static class BillingScriptEvent extends ScriptEvent {
        double sourceOnDemandRate;
        double destinationOnDemandRate;
        boolean purgeState;
        boolean state;

        /**
         * Return string representation of event.
         *
         * @return string representation of event
         */
        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("BillingScriptEvent{");
            sb.append("sourceOnDemandRate=").append(sourceOnDemandRate);
            sb.append(", destinationOnDemandRate=").append(destinationOnDemandRate);
            sb.append(", purgeState=").append(purgeState);
            sb.append(", state=").append(state);
            sb.append(", timestamp=").append(timestamp);
            sb.append(", eventType='").append(eventType).append('\'');
            sb.append(", uuid='").append(uuid).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }

    /**
     * Get the script event class.
     *
     * @return the script event class
     */
    @Override
    public Class getScriptEventClass() {
        return BillingScriptEvent[].class;
    }

    /**
     * Generate the action chains and bill records from the scenario script.
     * Also get the purgeState flag from the STOP event.
     *
     * @param scriptEvents list of script events
     * @param uuidMap UUID map for getting OIDs from entity display names
     * @param purgePreviousTestState true if the stats and entity for the entities in the UUID list
     * @param actionChains action chains (output parameter)
     * @param billRecordsByEntity bill records (output parameter)
     */
    @Override
    public void handleScriptEvents(@Nonnull List<ScriptEvent> scriptEvents,
            @Nonnull Map<String, Long> uuidMap,
            @Nonnull AtomicBoolean purgePreviousTestState,
            @Nonnull Map<Long, NavigableSet<ActionSpec>> actionChains,
            @Nonnull Map<Long, Set<BillingRecord>> billRecordsByEntity) {
        if (scriptEvents.isEmpty()) {
            return;
        }
        // Get the purgeState flag from the STOP event, and get the scenario end time.
        long startTime = Long.MAX_VALUE;
        long endTime = Long.MIN_VALUE;
        for (ScriptEvent event : scriptEvents) {
            if ("STOP".equals(event.eventType)) {
                BillingScriptEvent billingScriptEvent = (BillingScriptEvent)event;
                purgePreviousTestState.set(billingScriptEvent.purgeState);
            }
            startTime = Math.min(startTime, event.timestamp);
            endTime = Math.max(endTime, event.timestamp);
        }

        // Generate the action chains and bill records from the scenario script.
        List<BillingScriptEvent> eventList = scriptEvents.stream()
                .filter(x -> x instanceof BillingScriptEvent)
                .map(BillingScriptEvent.class::cast)
                .collect(Collectors.toList());
        actionChains.putAll(ScenarioGenerator.generateActionChains(eventList, uuidMap));
        if (!actionChains.isEmpty()) {
            billRecordsByEntity.putAll(ScenarioGenerator.generateBillRecords(eventList, uuidMap,
                    TimeUtil.millisToLocalDateTime(startTime, Clock.systemUTC()),
                    TimeUtil.millisToLocalDateTime(endTime, Clock.systemUTC())));
        }
    }
}
