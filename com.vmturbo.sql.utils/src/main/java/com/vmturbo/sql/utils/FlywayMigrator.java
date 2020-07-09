package com.vmturbo.sql.utils;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.callback.FlywayCallback;

/**
 * A utility class that attempts to connect to a database and run flyway migrations.
 * If the database is not accepting connections, retry the connection until the
 * {@link this#maximumDatabaseWaitTime} elapses.
 */
public class FlywayMigrator {
    private final Duration maximumDatabaseWaitTime;
    private final Duration retryInterval;
    private final Supplier<Flyway> flywayFactory;

    private static final Logger logger = LogManager.getLogger();
    public static final String DATABASE_CONNECTION_FAILURE_MESSAGE =
        "Unable to obtain Jdbc connection from DataSource";

    public FlywayMigrator(@Nonnull final Duration maximumDatabaseWaitTime,
                          @Nonnull final Duration retryInterval,
                          @Nonnull final String dbSchemaName,
                          @Nonnull final Optional<String> location,
                          @Nonnull final DataSource dataSource,
                          @Nonnull final FlywayCallback... callbacks) {
        this(maximumDatabaseWaitTime, retryInterval, () -> {
            final Flyway flyway = new Flyway();
            flyway.setSchemas(dbSchemaName);
            flyway.setDataSource(dataSource);
            flyway.setCallbacks(callbacks);
            location.ifPresent(flyway::setLocations);
            return flyway;
        });
    }

    public FlywayMigrator(@Nonnull final Duration maximumDatabaseWaitTime,
                   @Nonnull final Duration retryInterval,
                   @Nonnull final Supplier<Flyway> flywayFactory) {
        this.maximumDatabaseWaitTime = maximumDatabaseWaitTime;
        this.retryInterval = retryInterval;
        this.flywayFactory = flywayFactory;
    }

    /**
     * Attempt to use flyway to migrate the database.
     * If the database is not yet ready to accept connections, retry until a connection succeeds
     * or the {@link this#maximumDatabaseWaitTime} elapses.
     *
     * @return the {@link Flyway} instance used to migrate the database.
     */
    @Nonnull
    public Flyway migrate() {
        Duration waitForDatabaseDelay = maximumDatabaseWaitTime;

        while (true) {
            try {
                return migrateDatabase();
            } catch (FlywayException e) {
                // If migration failed due to database unavailability, wait a bit and retry.
                if (e.getMessage().equals(DATABASE_CONNECTION_FAILURE_MESSAGE)) {
                    waitForDatabaseDelay = waitForDatabaseDelay.minus(retryInterval);
                    if (waitForDatabaseDelay.isNegative()) {
                        throw e;
                    } else {
                        logger.info("Database connection unavailable. Retrying in {}", retryInterval);
                        sleepFor(retryInterval);
                    }
                    // If migration failed for some other reason (ie a bad migration script failed to execute),
                    // the problem should kill startup.
                } else {
                    throw e;
                }
            }
        }
    }

    /**
     * Attempt to validate that the database is up-to-date with discoverable migrations.
     *
     * <p>If the database is not yet ready to accept connections, retry until a connection
     * succeeds or the {@link this#maximumDatabaseWaitTime} elapses.</p>
     *
     * @return the {@link Flyway} instance used to validate the database
     */
    @Nonnull
    public Flyway validate() {
        Duration waitForDatabaseDelay = maximumDatabaseWaitTime;

        while (true) {
            try {
                return validateDatabase();
            } catch (FlywayException e) {
                // If migration failed due to database unavailability, wait a bit and retry.
                if (e.getMessage().equals(DATABASE_CONNECTION_FAILURE_MESSAGE)) {
                    waitForDatabaseDelay = waitForDatabaseDelay.minus(retryInterval);
                    if (waitForDatabaseDelay.isNegative()) {
                        throw e;
                    } else {
                        logger.info("Database connection unavailable. Retrying in {}", retryInterval);
                        sleepFor(retryInterval);
                    }
                    // If migration failed for some other reason (ie a bad migration script failed to execute),
                    // the problem should kill startup.
                } else {
                    throw e;
                }
            }
        }
    }

    private Flyway migrateDatabase() throws FlywayException {
        Flyway flyway = flywayFactory.get();

        flyway.migrate();
        return flyway;
    }

    private Flyway validateDatabase() throws FlywayException {
        Flyway flyway = flywayFactory.get();

        flyway.validate();
        return flyway;
    }

    private void sleepFor(@Nonnull final Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException interruptedException) {
            // If interrupted, re-throw the interrupt to cancel the startup sequence
            logger.error("Startup sequence interrupted", interruptedException);
            throw new RuntimeException(interruptedException);
        }
    }
}
