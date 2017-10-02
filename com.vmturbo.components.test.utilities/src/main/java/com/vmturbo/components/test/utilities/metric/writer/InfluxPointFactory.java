package com.vmturbo.components.test.utilities.metric.writer;

import javax.annotation.Nonnull;

import org.influxdb.dto.Point;

import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.MetricFamilyKey;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.MetricKey;

/**
 * A factory class mainly to help with unit testing the {@link InfluxMetricsWarehouseVisitor}.
 * <p>
 * The {@link Point} class' members are all private, so this is the best way we have to capture
 * what points are being created (other than comparing string serializations of points).
 */
public interface InfluxPointFactory {

    @Nonnull
    Point createPoint(@Nonnull final String measurement,
                      @Nonnull final String source,
                      @Nonnull final MetricFamilyKey familyKey,
                      @Nonnull final MetricKey metricKey,
                      final long timeMs,
                      final double value);
}
