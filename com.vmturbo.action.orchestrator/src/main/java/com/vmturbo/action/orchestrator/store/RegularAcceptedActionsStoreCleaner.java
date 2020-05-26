package com.vmturbo.action.orchestrator.store;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;

/**
 * Class checks accepted actions store and removed acceptance for action if it was expired.
 */
public class RegularAcceptedActionsStoreCleaner {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The time (in minutes) to delay first execution.
     */
    private static final Long INITIAL_DELAY = 10L;

    /**
     * Constructor of {@link RegularAcceptedActionsStoreCleaner}.
     *
     * @param scheduledExecutorService schedule executor service
     * @param acceptedActionsStore dao layer for working with accepted actions
     * @param actionAcceptanceTTL time to live for accepted actions
     * @param frequencyOfCleaningAcceptedActionsStore frequency of checking accepted actions
     * store for finding expired acceptances
     */
    public RegularAcceptedActionsStoreCleaner(
            @Nonnull final ScheduledExecutorService scheduledExecutorService,
            @Nonnull final AcceptedActionsDAO acceptedActionsStore, final long actionAcceptanceTTL,
            final long frequencyOfCleaningAcceptedActionsStore) {
        logger.info("Action acceptance TTL is set at {} minutes", actionAcceptanceTTL);
        scheduledExecutorService.scheduleWithFixedDelay(
                () -> removeExpiredActionsAcceptances(acceptedActionsStore, actionAcceptanceTTL),
                INITIAL_DELAY, frequencyOfCleaningAcceptedActionsStore, TimeUnit.MINUTES);
    }

    private static void removeExpiredActionsAcceptances(
            @Nonnull final AcceptedActionsDAO acceptedActionsStore, long minActionAcceptanceTTL) {
        try {
            acceptedActionsStore.removeExpiredActions(minActionAcceptanceTTL);
        } catch (DataAccessException ex) {
            logger.error("Failed to remove all action with expired acceptances", ex);
        }
    }
}
