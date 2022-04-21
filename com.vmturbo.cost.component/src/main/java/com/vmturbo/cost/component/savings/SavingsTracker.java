package com.vmturbo.cost.component.savings;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.SetUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
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
    private final StatsWriter statsWriter;

    /**
     * Supported provider types.
     */
    private final Set<Integer> supportedProviderTypes;

    /**
     * Creates a new tracker.
     *
     * @param billingRecordStore Store for billing records.
     * @param actionChainStore Action chain store.
     * @param statsWriter Writer for final stats.
     * @param supportedProviderTypes Provider types wer are interested in.
     */
    public SavingsTracker(@Nonnull final BillingRecordStore billingRecordStore,
            @Nonnull ActionChainStore actionChainStore,
            @Nonnull final StatsWriter statsWriter,
            @Nonnull final Set<Integer> supportedProviderTypes) {
        this.billingRecordStore = billingRecordStore;
        this.actionChainStore = actionChainStore;
        this.statsWriter = statsWriter;
        this.supportedProviderTypes = supportedProviderTypes;
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
        final Map<Long, NavigableSet<ActionSpec>> actionChains = actionChainStore
                .getActionChains(entityIds);

        final Set<Long> statTimes = processStates(entityIds, billingRecords, actionChains);

        // Save off the day stats timestamps for all stats written this time, used for rollups.
        savingsTimes.addAllDayStatsTimes(statTimes);
    }

    private Set<Long> processStates(@Nonnull final Set<Long> entityOids,
            Map<Long, Set<BillingRecord>> billingRecords,
            Map<Long, NavigableSet<ActionSpec>> actionChains) throws EntitySavingsException {
        final List<SavingsValues> allSavingsValues = new ArrayList<>();
        entityOids.forEach(entityId -> {
            Set<BillingRecord> entityBillingRecords = billingRecords.get(entityId);
            NavigableSet<ActionSpec> entityActionChain = actionChains.get(entityId);
            if (SetUtils.emptyIfNull(entityBillingRecords).isEmpty()
                    || SetUtils.emptyIfNull(entityActionChain).isEmpty()) {
                return;
            }
            final List<SavingsValues> values = Calculator.calculate(entityId, entityBillingRecords,
                    entityActionChain);
            logger.trace("{} savings values for entity {}, {} bill records, {} actions.",
                    values::size, () -> entityId, entityBillingRecords::size,
                    entityActionChain::size);
            allSavingsValues.addAll(values);
        });

        // Once we are done processing all the states for this period, we write stats.
        return statsWriter.writeDailyStats(allSavingsValues);
    }

    /**
     * Process given list of entity states. A chunk of states are processed at a time. This can
     * only be invoked when the ENABLE_SAVINGS_TEST_INPUT feature flag is enabled.
     *
     * @param participatingUuids list of UUIDs involved in the injected scenario
     * @param startTime starting time of the injected scenario
     * @param endTime ending time of the injected scenario
     */
    @Override
    public void processStates(@Nonnull Set<Long> participatingUuids,
            @Nonnull LocalDateTime startTime,
            @Nonnull LocalDateTime endTime) throws EntitySavingsException {
        logger.debug("Data injector invoked for the period of {} to {} on UUIDs: {}",
                startTime, endTime, participatingUuids);
        // Get billing records in this time range, mapped by entity id.
        final Map<Long, Set<BillingRecord>> billingRecords = new HashMap<>();
        billingRecordStore.getBillRecords(startTime, endTime, participatingUuids)
                .filter(record -> record.isValid(supportedProviderTypes))
                .forEach(record -> billingRecords.computeIfAbsent(record.getEntityId(), e -> new HashSet<>())
                            .add(record));

        // Get map of entity id to sorted list of actions for it, starting with first executed.
        final Map<Long, NavigableSet<ActionSpec>> actionChains = actionChainStore
                .getActionChains(participatingUuids);

        processStates(participatingUuids, billingRecords, actionChains);
    }

    /**
     * Purge state for the indicated UUIDs in preparation for processing injected data.  This can
     * only be invoked when the ENABLE_SAVINGS_TEST_INPUT feature flag is enabled.
     *
     * @param participatingUuids UUIDs to purge.
     */
    @Override
    public void purgeState(Set<Long> participatingUuids) {
        logger.debug("Purge state for UUIDs in preparation for data injection: {}",
                participatingUuids);
    }
}
