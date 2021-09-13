package com.vmturbo.extractor.topology;

import java.sql.SQLException;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification;
import com.vmturbo.common.protobuf.cost.CostNotificationOuterClass.CostNotification.CloudSavingsAvailable;
import com.vmturbo.cost.api.CostNotificationListener;
import com.vmturbo.extractor.topology.fetcher.CloudSavingsFetcher;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Listens for new cloud savings hourly data being written to MySQL entity_savings tables and writes
 * corresponding data to Postgres DB table.
 */
public class CloudSavingsListener implements CostNotificationListener {
    private static final Logger logger = LogManager.getLogger();

    private final CloudSavingsFetcher savingsFetcher;

    /**
     * Create a new instance.
     *
     * @param fetcher For actually querying cost component for stats, and then writing to local DB.
     */
    public CloudSavingsListener(@Nonnull final CloudSavingsFetcher fetcher) {
        this.savingsFetcher = fetcher;
        logger.info("Created Cloud Savings listener.");
    }

    /**
     * Receives notification from cost component (EntitySavingsProcessor::sendSavingsNotification)
     * that new hourly savings data has been written to the entity_savings DB table.
     * This then queries for that new data from MySQL DB and writes it to the local Postgres DB.
     *
     * @param costNotification The cloud savings available notification.
     */
    @Override
    public void onCostNotificationReceived(@Nonnull final CostNotification costNotification) {
        if (!costNotification.hasCloudSavingsAvailable()) {
            logger.trace("Ignoring non-savings notification: {}", costNotification);
            return;
        }
        final CloudSavingsAvailable cloudSavingsAvailable = costNotification.getCloudSavingsAvailable();
        long startDate = cloudSavingsAvailable.getStartDate();
        long endDate = cloudSavingsAvailable.getEndDate();

        logger.info("Received Cloud Savings notification, data between {} and {}...",
                startDate, endDate);
        try {
            savingsFetcher.syncCloudSavingsData(startDate, endDate);
        } catch (UnsupportedDialectException | SQLException | DataAccessException e) {
            logger.warn("Unable to read and write savings data between {} & {}.",
                    startDate, endDate, e);
        } catch (InterruptedException ie) {
            logger.warn("Interrupt during read and write savings data between {} & {}: {}",
                    startDate, endDate, ie.getMessage());
            Thread.currentThread().interrupt();
        }
    }
}
