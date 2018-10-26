package com.vmturbo.influxdb;

import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Pong;

import com.vmturbo.components.common.health.PollingHealthMonitor;
import com.vmturbo.components.common.health.SimpleHealthStatusProvider;

/**
 * InfluxHealthMonitor watches an InfluxDB instance and periodically checks it for availability by
 * calling the getVersion() function on it. A successful call is assumed to represent a healthy
 * connection with InfluxDB.
 *
 * InfluxHealthMonitor extends {@link SimpleHealthStatusProvider} for storing health status info.
 */
public class InfluxHealthMonitor extends PollingHealthMonitor {

    private final Supplier<InfluxDB> InfluxDBSupplier;

    /**
     * Create an InfluxHealthMonitor that will monitor the specified InfluxDB instance using the
     * specified polling interval
     *
     * @param pollingIntervalSecs  how often (in seconds) to check the InfluxDB instance for availability
     * @param InfluxDBSupplier  factory that provides connections to the InfluxDB instance to check.
     */
    public InfluxHealthMonitor(double pollingIntervalSecs, @Nonnull Supplier<InfluxDB> InfluxDBSupplier) {
        super("InfluxDB",pollingIntervalSecs);

        this.InfluxDBSupplier = InfluxDBSupplier;
    }

    @Override
    public void updateHealthStatus() {
        final Optional<InfluxDB> db = Optional.ofNullable(InfluxDBSupplier.get());
        try {
            final Pong pong = db
                .map(InfluxDB::ping)
                .orElse(null);

            // as long as we got any response from the call, we consider ourselves healthy.
            if (pong != null) {
                reportHealthy();
            } else {
                reportUnhealthy("Health check failed: couldn't ping influxDB "+ Instant.now());
            }
        } catch(Throwable t) {
            // Treat all errors on this check as a sign of unhealthiness -- we'll be checking
            // again later.
            reportUnhealthy("Error:"+ t.toString());
        }
    }
}
