package com.vmturbo.components.test.utilities.metric;

import javax.annotation.Nonnull;

import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.MetricFamilyKey;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.MetricKey;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.MetricMetadata;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.TimestampedSample;

/**
 * The {@link MetricsWarehouseVisitor} is used to collect all metrics from a single
 * {@link MetricsWarehouse} in a depth-first way.
 *
 * For the details of the data model see {@link MetricsScraper}.
 *
 * The order of calls should be:
 *
 * onStartWarehouse() <- the first call
 * for each {@link MetricsScraper} in the {@link MetricsWarehouse}:
 *    onStartHarvester()
 *    for each metric family collected from the harvester:
 *       onStartMetricFamily()
 *          for each metric in the metric family:
 *             onStartMetric()
 *             for each sample collected for the metric:
 *                onSample()
 *             onEndMetric()
 *       onEndMetricFamily()
 *    onEndHarvester()
 * onEndWarehouse() <- the last call
 */
public interface MetricsWarehouseVisitor {

    void onStartWarehouse(@Nonnull final String testName);

    void onEndWarehouse();

    void onStartHarvester(@Nonnull final String harvesterName);

    void onEndHarvester();

    void onStartMetricFamily(@Nonnull final MetricFamilyKey key);

    void onEndMetricFamily();

    void onStartMetric(@Nonnull final MetricKey key, @Nonnull final MetricMetadata metadata);

    void onEndMetric();

    void onSample(@Nonnull final TimestampedSample sample);
}
