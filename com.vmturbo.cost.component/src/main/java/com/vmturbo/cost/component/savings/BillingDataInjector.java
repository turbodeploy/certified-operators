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

import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
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
     * Describes Commodity in list of commodities involved in scale actions, if relevant.
     */
    public static class Commodity {
        String commType;
        float sourceCapacity;
        double sourceRate;
        float destinationCapacity;
        double destinationRate;

        Commodity(final String commType, final float sourceCapacity, final double sourceRate,
                  final float destinationCapacity, final double destinationRate) {
            this.commType = commType;
            this.sourceCapacity = sourceCapacity;
            this.sourceRate = sourceRate;
            this.destinationCapacity = destinationCapacity;
            this.destinationRate = destinationRate;
        }

        /**
         * Return string representation of commodity.
         *
         * @return string representation of commodity.
         */
        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Commodity{");
            sb.append("Type=").append(commType);
            sb.append(", sourceCapacity=").append(sourceCapacity);
            sb.append(", sourceRate=").append(sourceRate);
            sb.append(", destinationCapacity=").append(destinationCapacity);
            sb.append(", destinationRate=").append(destinationRate);
            sb.append('}');
            return sb.toString();
        }
    }

    /**
     * Event format passed between the data generator and the event injector.
     */
    public static class BillingScriptEvent extends ScriptEvent {
        // on-demand rates hold the compute rates for VMs and the compute portion rates for DBs.
        double sourceOnDemandRate;
        double destinationOnDemandRate;
        boolean purgeState;
        boolean state;
        double expectedCloudCommitment;
        List<Commodity> commodities;
        String sourceType;
        String destinationType;
        int sourceNumVCores;
        int destinationNumVCores;
        double sourceLicenseRate;
        double destinationLicenseRate;

        /**
         * Return string representation of event.
         *
         * @return string representation of event
         */
        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("BillingScriptEvent{");
            sb.append("sourceOnDemandRate=").append(this.sourceOnDemandRate);
            sb.append(", destinationOnDemandRate=").append(this.destinationOnDemandRate);
            sb.append(", commodities=").append(this.commodities);
            sb.append(", purgeState=").append(this.purgeState);
            sb.append(", state=").append(this.state);
            sb.append(", timestamp=").append(this.timestamp);
            sb.append(", eventType='").append(this.eventType).append('\'');
            sb.append(", uuid='").append(uuid).append('\'');
            sb.append(", expectedCloudCommitment='").append(this.expectedCloudCommitment).append('\'');
            sb.append(", sourceType='").append(sourceType).append('\'');
            sb.append(", destinationType='").append(destinationType).append('\'');
            sb.append(", sourceNumVCores='").append(sourceNumVCores).append('\'');
            sb.append(", destinationNumVCores='").append(destinationNumVCores).append('\'');
            sb.append(", sourceLicenseRate='").append(sourceLicenseRate).append('\'');
            sb.append(", destinationLicenseRate='").append(destinationLicenseRate).append('\'');
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
            @Nonnull Map<Long, NavigableSet<ExecutedActionsChangeWindow>> actionChains,
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
