package com.vmturbo.cost.component.savings;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;

/**
 * Interface for classes that can handle injected test data.
 */
public interface ScenarioDataHandler {
    /**
     * Process given list of entity states. A chunk of states are processed at a time. This can
     * only be invoked when the {@link ENABLE_SAVINGS_TEST_INPUT} feature flag is enabled.
     *
     * @param participatingUuids list of UUIDs involved in the injected scenario
     * @param startTime starting time of the injected scenario
     * @param endTime ending time of the injected scenario
     * @param actionChains action chains
     * @param billRecordsByEntity bill records of each entity
     * @throws EntitySavingsException Errors with generating or writing stats
     */
    void processStates(@Nonnull Set<Long> participatingUuids, @Nonnull LocalDateTime startTime,
            @Nonnull LocalDateTime endTime, @Nonnull Map<Long, NavigableSet<ExecutedActionsChangeWindow>> actionChains,
            @Nonnull Map<Long, Set<BillingRecord>> billRecordsByEntity) throws EntitySavingsException;

    /**
     * Purge state for the indicated UUIDs in preparation for processing injected data.  This can
     * only be invoked when the {@link ENABLE_SAVINGS_TEST_INPUT} feature flag is enabled.
     *
     * @param participatingUuids UUIDs to purge.
     */
    void purgeState(Set<Long> participatingUuids);
}
