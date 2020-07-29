package com.vmturbo.arangodb;

import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.ArangoDBVersion;

import com.vmturbo.components.common.health.PollingHealthMonitor;
import com.vmturbo.components.common.health.SimpleHealthStatusProvider;

/**
 * ArangoHealthMonitor watches an ArangoDB instance and periodically checks it for availability by
 * calling the getVersion() function on it. A successful call is assumed to represent a healthy
 * connection with ArangoDB.
 *
 * ArangoHealthMonitor extends {@link SimpleHealthStatusProvider} for storing health status info.
 */
public class ArangoHealthMonitor extends PollingHealthMonitor {

    private final Supplier<ArangoDB> arangoDbSupplier;
    private final Optional<String> databaseNameOpt;

    /**
     * Create an ArangoHealthMonitor that will monitor the specified ArangoDB instance using the
     * specified polling interval
     *
     * @param pollingIntervalSecs  how often (in seconds) to check the ArangoDB instance for availability
     * @param arangoDbSupplier  factory that provides connections to the ArangoDB instance to check.
     * @param databaseNameOpt   optional database name to connect to.
     */
    public ArangoHealthMonitor(double pollingIntervalSecs, @Nonnull Supplier<ArangoDB> arangoDbSupplier,
            final Optional<String> databaseNameOpt) {
        super("ArangoDB",pollingIntervalSecs);
        this.databaseNameOpt = databaseNameOpt;
        this.arangoDbSupplier = arangoDbSupplier;
    }

    /**
     * ArangoDB health status check.
     */
    @Override
    public void updateHealthStatus() {
        try {
            final ArangoDB driver = arangoDbSupplier.get();
            if (databaseNameOpt.isPresent()) {
                final ArangoDatabase db = driver.db(databaseNameOpt.get());
                // If db name passed it, check connection on this db only.
                if (db.exists()) {
                    reportHealthy();
                } else {
                    reportUnhealthy(String.format("Health check failed: Arango DB %s is missing",
                            databaseNameOpt.get()));
                }
            } else {
                ArangoDBVersion version = driver.getVersion();
                // as long as we got any response from the call, we consider ourselves healthy.
                if (version != null) {
                    reportHealthy();
                } else {
                    reportUnhealthy("Health check failed: couldn't retrieve ArangoDB version at "
                            + Instant.now());
                }
            }
        } catch (Throwable t) {
            // Treat all errors on this check as a sign of unhealthiness -- we'll be checking
            // again later.
            reportUnhealthy("Error:" + t.toString());
        }
    }
}
