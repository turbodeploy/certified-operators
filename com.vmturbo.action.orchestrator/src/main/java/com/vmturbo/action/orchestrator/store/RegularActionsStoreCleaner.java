package com.vmturbo.action.orchestrator.store;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.RejectedActionsDAO;

/**
 * Class checks accepted and rejected actions stores and removed acceptances/rejections for
 * action if it was expired.
 */
public class RegularActionsStoreCleaner {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The time (in minutes) to delay first execution.
     */
    private static final Long INITIAL_DELAY = 10L;

    /**
     * Constructor of {@link RegularActionsStoreCleaner}.
     *
     * @param scheduledExecutorService schedule executor service
     * @param acceptedActionsStore dao layer for working with accepted actions
     * @param rejectedActionsStore dao layer for working with rejected actions
     * @param actionAcceptanceTTL time to live for accepted actions
     * @param actionRejectionTTL time to live for rejected actions
     * @param frequencyOfCleaningAcceptedActionsStore frequency of checking accepted actions
     */
    public RegularActionsStoreCleaner(
            @Nonnull final ScheduledExecutorService scheduledExecutorService,
            @Nonnull final AcceptedActionsDAO acceptedActionsStore,
            @Nonnull final RejectedActionsDAO rejectedActionsStore,
            final long actionAcceptanceTTL, long actionRejectionTTL,
            final long frequencyOfCleaningAcceptedActionsStore) {
        logger.info("Action acceptance TTL is set at {} minutes", actionAcceptanceTTL);
        logger.info("Action rejection TTL is set at {} minutes", actionRejectionTTL);

        scheduledExecutorService.scheduleWithFixedDelay(
                () -> removeExpiredActionsAcceptancesAndRejections(acceptedActionsStore,
                        rejectedActionsStore, actionAcceptanceTTL, actionRejectionTTL),
                INITIAL_DELAY, frequencyOfCleaningAcceptedActionsStore, TimeUnit.MINUTES);
    }

    private static void removeExpiredActionsAcceptancesAndRejections(
            @Nonnull final AcceptedActionsDAO acceptedActionsStore,
            @Nonnull final RejectedActionsDAO rejectedActionsStore, long minActionAcceptanceTTL,
            long minActionRejectionTTL) {
        try {
            acceptedActionsStore.removeExpiredActions(minActionAcceptanceTTL);
            rejectedActionsStore.removeExpiredRejectedActions(minActionRejectionTTL);
        } catch (DataAccessException ex) {
            logger.error("Failed to remove all action with expired acceptances", ex);
        }
    }
}
