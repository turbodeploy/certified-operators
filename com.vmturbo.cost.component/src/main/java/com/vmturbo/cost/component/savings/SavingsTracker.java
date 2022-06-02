package com.vmturbo.cost.component.savings;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.SetUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.cost.component.savings.calculator.Calculator;
import com.vmturbo.cost.component.savings.calculator.SavingsValues;

/**
 * Processes a chunk of entity stats for a set of given time periods.
 */
public class SavingsTracker implements ScenarioDataHandler {
    private final Logger logger = LogManager.getLogger();

    /**
     * For billing record queries.
     */
    private final BillingRecordStore billingRecordStore;

    /**
     * Action chain interface.
     */
    private final ActionChainStore actionChainStore;

    /**
     * Stats writing interface.
     */
    private final SavingsStore savingsStore;

    /**
     * Supported provider types.
     */
    private final Set<Integer> supportedProviderTypes;

    /**
     * Clock.
     */
    private final Clock clock;

    /**
     * Bill-based savings calculator.
     */
    private final Calculator calculator;

    /**
     * Creates a new tracker.
     *
     * @param billingRecordStore Store for billing records.
     * @param actionChainStore Action chain store.
     * @param savingsStore Writer for final stats.
     * @param supportedProviderTypes Provider types wer are interested in.
     */
    public SavingsTracker(@Nonnull final BillingRecordStore billingRecordStore,
            @Nonnull ActionChainStore actionChainStore,
            @Nonnull final SavingsStore savingsStore,
            @Nonnull final Set<Integer> supportedProviderTypes,
            long deleteActionRetentionMs,
            @Nonnull Clock clock) {
        this.billingRecordStore = billingRecordStore;
        this.actionChainStore = actionChainStore;
        this.savingsStore = savingsStore;
        this.supportedProviderTypes = supportedProviderTypes;
        this.clock = clock;
        this.calculator = new Calculator(deleteActionRetentionMs, clock);
    }

    /**
     * Process given list of entity states. A chunk of states are processed at a time.
     *
     * @param entityIds OIDs of entities to be processed.
     * @param savingsTimes Contains timing related info used for query, stores responses as well.
     * @param chunkCounter Counter for current chunk, for logging.
     * @throws EntitySavingsException Thrown on DB error.
     */
    void processStates(@Nonnull final Set<Long> entityIds,
            @Nonnull final SavingsTimes savingsTimes, @Nonnull final AtomicInteger chunkCounter)
            throws EntitySavingsException {
        long previousLastUpdated = savingsTimes.getPreviousLastUpdatedTime();
        long lastUpdatedEndTime = savingsTimes.getLastUpdatedEndTime();
        logger.trace("{}: Processing chunk of {} states with last updated >= {} && < {}...",
                () -> chunkCounter, entityIds::size, () -> previousLastUpdated,
                () -> lastUpdatedEndTime);

        // Get billing records in this time range, mapped by entity id.
        final Map<Long, Set<BillingRecord>> billingRecords = new HashMap<>();

        // For this set of billing records, see if we have any last_updated times that are newer.
        final AtomicLong newLastUpdated = new AtomicLong(savingsTimes.getCurrentLastUpdatedTime());
        billingRecordStore.getUpdatedBillRecords(previousLastUpdated, lastUpdatedEndTime, entityIds)
                .filter(record -> record.isValid(supportedProviderTypes))
                .forEach(record -> {
                    if (record.getLastUpdated() != null
                            && record.getLastUpdated() > newLastUpdated.get()) {
                        newLastUpdated.set(record.getLastUpdated());
                    }
                    billingRecords.computeIfAbsent(record.getEntityId(), e -> new HashSet<>())
                            .add(record);
                });
        savingsTimes.setCurrentLastUpdatedTime(newLastUpdated.get());

        // Get map of entity id to sorted list of actions for it, starting with first executed.
        final Map<Long, NavigableSet<ExecutedActionsChangeWindow>> actionChains = actionChainStore
                .getActionChains(entityIds);

        // Get the timestamp of the day (beginning of the day) that was last processed.
        // Need this date for delete action savings calculation.
        long lastProcessedDate = savingsTimes.getLastRollupTimes().getLastTimeByDay();
        final Set<Long> statTimes = processStates(entityIds, billingRecords, actionChains,
                lastProcessedDate, LocalDateTime.now(clock));

        // Save off the day stats timestamps for all stats written this time, used for rollups.
        savingsTimes.addAllDayStatsTimes(statTimes);
    }

    private Set<Long> processStates(@Nonnull final Set<Long> entityOids,
            Map<Long, Set<BillingRecord>> billingRecords,
            Map<Long, NavigableSet<ExecutedActionsChangeWindow>> actionChains,
            long lastProcessedDate, LocalDateTime periodEndTime) throws EntitySavingsException {
        final List<SavingsValues> allSavingsValues = new ArrayList<>();
        entityOids.forEach(entityId -> {
            Set<BillingRecord> entityBillingRecords = billingRecords.getOrDefault(entityId,
                    Collections.emptySet());
            NavigableSet<ExecutedActionsChangeWindow> entityActionChain = actionChains.get(entityId);
            if (SetUtils.emptyIfNull(entityActionChain).isEmpty()) {
                return;
            }
            final List<SavingsValues> values = calculator.calculate(entityId, entityBillingRecords,
                    entityActionChain, lastProcessedDate, periodEndTime);
            logger.trace("{} savings values for entity {}, {} bill records, {} actions.",
                    values::size, () -> entityId, entityBillingRecords::size,
                    entityActionChain::size);
            allSavingsValues.addAll(values);
            logger.trace("Savings stats for entity {}:\n{}\n{}", () -> entityId,
                    SavingsValues::toCsvHeader,
                    () -> values.stream()
                            .sorted(Comparator.comparing(SavingsValues::getTimestamp))
                            .map(SavingsValues::toCsv)
                            .collect(Collectors.joining("\n")));

        });

        // Once we are done processing all the states for this period, we write stats.
        return savingsStore.writeDailyStats(allSavingsValues);
    }

    /**
     * Process given list of entity states. This can only be invoked when the
     * ENABLE_SAVINGS_TEST_INPUT feature flag is enabled.
     *
     * @param participatingUuids list of UUIDs involved in the injected scenario
     * @param startTime starting time of the injected scenario
     * @param endTime ending time of the injected scenario
     * @param actionChains action chain
     * @param billRecordsByEntity bill records of each entity
     * @throws EntitySavingsException Errors with generating or writing stats
     */
    @Override
    public void processStates(@Nonnull Set<Long> participatingUuids,
            @Nonnull LocalDateTime startTime, @Nonnull LocalDateTime endTime,
            @Nonnull final Map<Long, NavigableSet<ExecutedActionsChangeWindow>> actionChains,
            @Nonnull final Map<Long, Set<BillingRecord>> billRecordsByEntity)
            throws EntitySavingsException {
        logger.info("Scenario generator invoked for the period of {} to {} on UUIDs: {}",
                startTime, endTime, participatingUuids);
        final Map<Long, NavigableSet<ExecutedActionsChangeWindow>> actions = new HashMap<>(actionChains);
        final Map<Long, Set<BillingRecord>> billRecords = new HashMap<>(billRecordsByEntity);
        if (actions.isEmpty()) {
            logger.info("No actions are defined in the scenario. Get action and bill data from the database.");
            // If no action chains are passed in, we will use the data in the database.
            // Get billing records in this time range, mapped by entity id.
            billingRecordStore.getBillRecords(startTime, endTime, participatingUuids)
                    .filter(record -> record.isValid(supportedProviderTypes))
                    .forEach(record -> billRecords.computeIfAbsent(record.getEntityId(), e -> new HashSet<>())
                            .add(record));

            // Get map of entity id to sorted list of actions for it, starting with first executed.
            actions.putAll(actionChainStore.getActionChains(participatingUuids));
        }

        processStates(participatingUuids, billRecords, actions,
                TimeUtil.localTimeToMillis(startTime.truncatedTo(ChronoUnit.DAYS).minusDays(1),
                        Clock.systemUTC()), endTime);
    }

    /**
     * Purge state for the indicated UUIDs in preparation for processing injected data.  This can
     * only be invoked when the ENABLE_SAVINGS_TEST_INPUT feature flag is enabled.
     *
     * @param uuids UUIDs to purge.
     */
    @Override
    public void purgeState(Set<Long> uuids) {
        logger.debug("Purge state for UUIDs in preparation for data injection: {}",
                uuids);
        if (!uuids.isEmpty()) {
            logger.info("Purging savings stats for UUIDs: {}", uuids);
            savingsStore.deleteStats(uuids);
        }
    }
}
