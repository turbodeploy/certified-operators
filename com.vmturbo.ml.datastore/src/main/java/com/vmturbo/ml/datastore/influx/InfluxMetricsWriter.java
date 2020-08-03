package com.vmturbo.ml.datastore.influx;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;

import com.google.common.annotations.VisibleForTesting;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.ActionStateWhitelist.ActionState;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.ActionTypeWhitelist.ActionType;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.MetricTypeWhitelist.MetricType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Writes metrics to influx. Writes are batched according to the batch size configured
 * in the influx connection.
 *
 * See https://vmturbo.atlassian.net/wiki/spaces/Home/pages/827850762/Metron+data+collection#Metrondatacollection-Theschema
 * for a high-level description of the influx data schema used for metrics in influx.
 *
 *
 * TODO: Fix the key problem if necessary.
 */
public class InfluxMetricsWriter implements AutoCloseable {

    protected final InfluxDB influxConnection;

    protected final String database;
    protected final String retentionPolicy;
    /**
     * Whitelist for influx metrics. Only metrisc on the whitelist are stored.
     */
    protected final MetricsStoreWhitelist metricsStoreWhitelist;

    private static final Logger logger = LogManager.getLogger();

    @VisibleForTesting
    InfluxMetricsWriter(@Nonnull final InfluxDB influxConnection,
                        @Nonnull final String database,
                        @Nonnull final String retentionPolicy,
                        @Nonnull final MetricsStoreWhitelist metricsStoreWhitelist) {
        this.database = Objects.requireNonNull(database);
        this.retentionPolicy = Objects.requireNonNull(retentionPolicy);
        this.influxConnection = Objects.requireNonNull(influxConnection);
        this.metricsStoreWhitelist = Objects.requireNonNull(metricsStoreWhitelist);
    }

    /**
     * Get the influx connection used by the metrics writer.
     *
     * @return the influx connection used by the metrics writer.
     */
    @Nonnull
    public InfluxDB getInfluxConnection() {
        return influxConnection;
    }

    /**
     * Flush any buffered data points. That is, write them through to influx.
     */
    public void flush() {
        influxConnection.flush();
    }

    /**
     * Close the connection to influx.
     *
     * @throws Exception If anything goes wrong.
     */
    @Override
    public void close() throws Exception {
        influxConnection.close();
    }

    /**
     * The different statTypes that we have are bought and sold commodities,
     * cluster information and actions
     */
    public enum InfluxStatType
    {
        COMMODITY_BOUGHT, COMMODITY_SOLD, CLUSTER, ACTIONS;
    }

    /**
     * A small helper class to assist in writing data points to influx and tracking statistics
     * about the data written to influx.
     */
    protected static class WriterContext {
        public final InfluxDB influxConnection;
        Map<InfluxStatType, Map<String, Long>> stats;
        public final Set<Integer> commoditiesWhitelist;
        public final Set<MetricType> metricTypesWhitelist;
        public final Set<ActionType> actionTypesWhitelist;
        public final Set<ActionState> actionStateWhitelist;
        public final long timeMs;

        public WriterContext(@Nonnull final InfluxDB influxConnection,
                             @Nonnull final Map<InfluxStatType, Map<String, Long>> stats,
                             @Nonnull final MetricsStoreWhitelist whiteListStore,
                             final long timeMs) {
            this.influxConnection = Objects.requireNonNull(influxConnection);
            this.stats = Objects.requireNonNull(stats);
            this.timeMs = timeMs;
            this.commoditiesWhitelist = Objects.requireNonNull(whiteListStore.getWhitelistAsInteger(MetricsStoreWhitelist.COMMODITY_TYPE));
            this.metricTypesWhitelist = Objects.requireNonNull(whiteListStore.getWhitelist(MetricsStoreWhitelist.METRIC_TYPE));
            this.actionTypesWhitelist = Objects.requireNonNull(whiteListStore.getWhitelist(MetricsStoreWhitelist.ACTION_TYPE));
            this.actionStateWhitelist = Objects.requireNonNull(whiteListStore.getWhitelist(MetricsStoreWhitelist.ACTION_STATE));
        }

        /**
         * Increment the metric count for a statistic of a given type and name.
         *
         * @param metricName The name of the metric type (eg VCPU_USED)
         * @param statType type of the statistic to be incremented
         * @return The incremented count for the statistic.
         */
        public Long incrementMetricOfType(@Nonnull final String metricName, InfluxStatType statType) {
            return stats.get(statType).merge(metricName, 1L, Long::sum);
        }
    }

    /**
     * A small helper class used to assist in writing metrics to influx.
     */
    protected class MetricWriter {
        private final WriterContext writerContext;
        private final Point.Builder pointBuilder;
        private final InfluxStatType statType;
        private int fieldCount = 0;

        public MetricWriter(@Nonnull final WriterContext writerContext,
                            @Nonnull final Point.Builder pointBuilder,
                            @Nonnull final InfluxStatType statType) {
            this.writerContext = Objects.requireNonNull(writerContext);
            this.pointBuilder = Objects.requireNonNull(pointBuilder);
            this.statType = Objects.requireNonNull(statType);
        }

        public Point.Builder getPointBuilder() {
            return pointBuilder;
        }

        /**
         * Determine if a particular {@link MetricType} has been whitelisted to be written to influx.
         * If not, the metric will not be written.
         *
         * @param metricType The metric type to check.
         * @return Whether the metric type should be written to influx.
         */
        public boolean whitelisted(@Nonnull final MetricType metricType) {
            return writerContext.metricTypesWhitelist.contains(metricType);
        }

        /**
         * Determine if a particular {@link CommodityType} has been whitelisted (by its number) to
         * be written to influx. If not, the metric will not be written.
         *
         * @param commodityTypeNumber The number of the commodity type to check.
         * @return Whether the commodity type with the given number should be written to influx.
         */
        public boolean whitelisted(@Nonnull final Integer commodityTypeNumber) {
            return writerContext.commoditiesWhitelist.contains(commodityTypeNumber);
        }

        /**
         * Add a field value to the data point to be written with this metric writer.
         *
         * @param commodityType The commodity type of the field. (ie VCPU)
         * @param metricType The metric type of the field. (ie USED)
         * @param value The value for this field. (ie the value of VCPU_USED)
         */
        public void field(@Nonnull final CommodityType commodityType,
                          @Nonnull final MetricType metricType,
                          final double value) {
            final String metricName = metricValueName(commodityType, metricType);
            pointBuilder.addField(metricName, value);

            writerContext.incrementMetricOfType(metricName, statType);
            fieldCount++;
        }

        public void field(@Nonnull final String name,
                          @Nonnull final String value) {
            pointBuilder.addField(name, value);

            writerContext.incrementMetricOfType(name, statType);
            fieldCount++;
        }

        /**
         * Write the data point.
         * If there are no fields on the data, does not actually write anything.
         *
         * @return the number of fields written.
         */
        public int write() {
            if (fieldCount > 0) {
                writerContext.influxConnection.write(database, retentionPolicy, pointBuilder.build());
                return 1;
            }
            return 0;
        }
    }

    /**
     * Compose the name of a metric value by concatenating commodity and metric type names.
     *
     * @param commodityType The commodity type for the metric.
     * @param metricType The metric type for the metric (ie USED, CAPACITY, etc.)
     * @return The name for the combined metric.
     */
    public static String metricValueName(@Nonnull final CommodityType commodityType,
                                         @Nonnull final MetricType metricType) {
        return commodityType.name() + "_" + metricType.name();
    }
}
