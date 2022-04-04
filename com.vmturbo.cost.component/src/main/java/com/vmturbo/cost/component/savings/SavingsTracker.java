package com.vmturbo.cost.component.savings;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
public class SavingsTracker {
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
     * Creates a new tracker.
     *
     * @param billingRecordStore Store for billing records.
     * @param actionChainStore Action chain store.
     * @param statsWriter Writer for final stats.
     */
    public SavingsTracker(@Nonnull final BillingRecordStore billingRecordStore,
            @Nonnull ActionChainStore actionChainStore,
            @Nonnull final StatsWriter statsWriter) {
        this.billingRecordStore = billingRecordStore;
        this.actionChainStore = actionChainStore;
        this.statsWriter = statsWriter;
    }

    /**
     * Process given list of entity states. A chunk of states are processed at a time.
     *
     * @param entityStates States to process this time.
     * @param savingsTimes Contains timing related info used for query, stores responses as well.
     * @param chunkCounter Counter for current chunk, for logging.
     * @throws EntitySavingsException Thrown on DB error.
     */
    void processStates(@Nonnull final List<EntityState> entityStates,
            @Nonnull final SavingsTimes savingsTimes, @Nonnull final AtomicInteger chunkCounter)
            throws EntitySavingsException {
        final List<Long> entityIds = entityStates.stream()
                .map(EntityState::getEntityId)
                .collect(Collectors.toList());

        long previousLastUpdated = savingsTimes.getPreviousLastUpdatedTime();
        long lastUpdatedEndTime = savingsTimes.getLastUpdatedEndTime();
        logger.trace("{}: Processing chunk of states: {} with last updated >= {} && < {}...",
                () -> chunkCounter, () -> entityStates, () -> previousLastUpdated,
                () -> lastUpdatedEndTime);

        // Get billing records in this time range, mapped by entity id.
        final Map<Long, NavigableSet<BillingChangeRecord>> billingRecords = new HashMap<>();

        // For this set of billing records, see if we have any last_updated times that are newer.
        final AtomicLong newLastUpdated = new AtomicLong(savingsTimes.getCurrentLastUpdatedTime());
        billingRecordStore.getBillingChangeRecords(previousLastUpdated, lastUpdatedEndTime, entityIds)
                .filter(BillingChangeRecord::isValid)
                .forEach(record -> {
                    if (record.getLastUpdated() != null
                            && record.getLastUpdated() > newLastUpdated.get()) {
                        newLastUpdated.set(record.getLastUpdated());
                    }
                    billingRecords.computeIfAbsent(
                                    record.getEntityId(),
                                    e -> new TreeSet<>(Comparator.comparing(
                                            BillingChangeRecord::getSampleTime)))
                            .add(record);
                });
        savingsTimes.setCurrentLastUpdatedTime(newLastUpdated.get());

        // Get map of entity id to sorted list of actions for it, starting with first executed.
        final Map<Long, NavigableSet<ActionSpec>> actionChains = actionChainStore
                .getActionChains(entityIds);

        final List<SavingsValues> allSavingsValues = new ArrayList<>();
        // Process one state at a time. State will get updated after calculation and will
        // contain stats that need to be written. State may also need to be written back.
        entityStates.forEach(state -> {
            long entityId = state.getEntityId();
            NavigableSet<BillingChangeRecord> entityBillingRecords = billingRecords.get(entityId);
            NavigableSet<ActionSpec> entityActionChain = actionChains.get(entityId);
            if (SetUtils.emptyIfNull(entityBillingRecords).isEmpty()
                    || SetUtils.emptyIfNull(entityActionChain).isEmpty()) {
                return;
            }
            final List<SavingsValues> values = Calculator.calculate(entityId, entityBillingRecords,
                    entityActionChain);
            logger.trace("{} savings values (c={}) for entity {}, {} bill records, {} actions.",
                    values::size, () -> chunkCounter, () -> entityId, entityBillingRecords::size,
                    entityActionChain::size);
            allSavingsValues.addAll(values);
        });

        // Once we are done processing all the states for this period, we write stats.
        // Save off the day stats timestamps for all stats written this time, used for rollups.
        savingsTimes.addAllDayStatsTimes(statsWriter.writeDailyStats(allSavingsValues));
    }
}
