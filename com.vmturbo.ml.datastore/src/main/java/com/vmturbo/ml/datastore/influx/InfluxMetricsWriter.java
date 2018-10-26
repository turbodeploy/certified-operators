package com.vmturbo.ml.datastore.influx;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.MetricTypeWhitelist.MetricType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTOOrBuilder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Writes metrics to influx. Writes are batched according to the batch size configured
 * in the influx connection.
 *
 * See https://vmturbo.atlassian.net/wiki/spaces/Home/pages/827850762/Metron+data+collection#Metrondatacollection-Theschema
 * for a high-level description of the influx data schema used for metrics in influx.
 *
 * Each commodity-metric type combination results in one field value written to influx.
 * (ie VCPU_USED, VCPU_PEAK, MEM_CAPACITY would be 3 field values)
 *
 * Each entity selling at least one commodity results in one data point. One data point
 * is written for each provider of a commodity bought for a service entity.
 *
 * Note that no commodity keys are written. This may potentially lead to issues with multiple
 * bundles of commodities bought from the same provider differentiated by keys.
 * TODO: Fix the key problem if necessary.
 */
public class InfluxMetricsWriter implements AutoCloseable {

    private final InfluxDB influxConnection;

    private final String database;
    private final String retentionPolicy;
    private final MetricsStoreWhitelist metricsStoreWhitelist;
    private final MetricJitter metricJitter;

    private static final Logger logger = LogManager.getLogger();

    @VisibleForTesting
    InfluxMetricsWriter(@Nonnull final InfluxDB influxConnection,
                        @Nonnull final String database,
                        @Nonnull final String retentionPolicy,
                        @Nonnull final MetricsStoreWhitelist metricsStoreWhitelist,
                        @Nonnull final MetricJitter metricJitter) {
        this.database = Objects.requireNonNull(database);
        this.retentionPolicy = Objects.requireNonNull(retentionPolicy);
        this.influxConnection = influxConnection;
        this.metricsStoreWhitelist = Objects.requireNonNull(metricsStoreWhitelist);
        this.metricJitter = Objects.requireNonNull(metricJitter);
    }

    /**
     * Write topology metrics to influx
     *
     * @param topologyChunk The chunk of the topology to be written to influx.
     * @param boughtStatistics A map of names of metrics to how many statistics of that type were written.
     *                         This map will be mutated by this method as it writes additional metrics to
     *                         count metrics written. This is the map for commodity bought statistics.
     * @param soldStatistics   A map of names of metrics to how many statistics of that type were written.
     *                         This map will be mutated by this method as it writes additional metrics to
     *                         count metrics written. This is the map for commodity sold statistics.
     *
     * @return The number of data points written.
     */
    public int writeTopologyMetrics(@Nonnull final Collection<TopologyEntityDTO> topologyChunk,
                                     final long timeMs,
                                     @Nonnull final Map<String, Long> boughtStatistics,
                                     @Nonnull final Map<String, Long> soldStatistics) {
        final WriterContext context = new WriterContext(influxConnection,
            boughtStatistics,
            soldStatistics,
            metricsStoreWhitelist.getWhitelistCommodityTypeNumbers(),
            metricsStoreWhitelist.getWhitelistMetricTypes(),
            timeMs);

        int numWritten = 0;
        for (TopologyEntityDTO entity : topologyChunk) {
            numWritten += writeEntityMetrics(entity, context);
        }
        return numWritten;
    }

    /**
     * Get the influx connection used by the metrics writer.
     *
     * @return the influx connection used by the metrics writer.
     */
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
     * Write metrics for a given {@link TopologyEntityDTO} to influx. Writes metrics for
     * commodities bought and sold. Use the given {@link WriterContext} to do the writing
     * and to track statistics about the data points and fields written.
     *
     * @param entity The entity whose metrics should be written.
     * @param context The {@link WriterContext} to be used to write to influx and track
     *                metric statistics.
     * @return The number of data points written to influx for this service entity.
     *         Write one data point for every provider this entity actively buys from.
     *         Write one data point if any commodities are actively sold.
     */
    private int writeEntityMetrics(@Nonnull final TopologyEntityDTO entity,
                                   @Nonnull final WriterContext context) {
        final EntityType entityType = EntityType.forNumber(entity.getEntityType());
        int numWritten = 0;

        for (CommoditiesBoughtFromProvider bought : entity.getCommoditiesBoughtFromProvidersList()) {
            numWritten += writeCommoditiesFromProvider(entity.getOid(), entityType, context,
                bought.getCommodityBoughtList(),
                bought.getProviderId());
        }

        // Write ACTIVE commodities sold
        final MetricWriter soldWriter = new MetricWriter(context,
            soldPointBuilder(entity.getOid(), entityType, context), context::incrementSoldMetric);
        entity.getCommoditySoldListList().stream()
            .filter(CommoditySoldDTO::getActive)
            .filter(sold -> sold.getCommodityType().hasType() &&
                soldWriter.whitelisted(sold.getCommodityType().getType()))
            .forEach(commoditySold -> writeCommoditySold(soldWriter, commoditySold));
        numWritten += soldWriter.write();

        return numWritten;
    }

    /**
     * Write commodities bought from a particular provider.
     *
     * For commodities bought we can store [USED, PEAK, and SCALING_FACTOR] depending on what is
     * on the whitelist.
     *
     * @param entityOid The oid of the entity buying the commodities.
     * @param entityType The type of the entity buying the commodities.
     * @param writerContext The {@link WriterContext} to be used to write to influx and track
     *                      metric statistics.
     * @param commoditiesBought The commodities bought from the given provider.
     * @param providerId The oid of the provider selling the commodities being bought by
     *                   the given service entity.
     * @return The number of data points written.
     *         This number will be 1 if there were any active commodities in the whitelist bought from
     *         this provider. It will be 0 if not.
     */
    private int writeCommoditiesFromProvider(final long entityOid,
                                              final EntityType entityType,
                                              @Nonnull final WriterContext writerContext,
                                              @Nonnull final List<CommodityBoughtDTO> commoditiesBought,
                                              final long providerId) {
        final MetricWriter metricWriter = new MetricWriter(writerContext,
            boughtPointBuilder(entityOid, entityType, writerContext), writerContext::incrementBoughtMetric);

        commoditiesBought.stream()
            .filter(CommodityBoughtDTO::getActive)
            .filter(bought -> bought.getCommodityType().hasType() &&
                metricWriter.whitelisted(bought.getCommodityType().getType()))
            .forEach(bought -> {
                final CommodityType commodityType = CommodityType.forNumber(bought.getCommodityType().getType());

                metricWriter.pointBuilder.tag("provider_id", Long.toString(providerId));
                if (bought.hasUsed() && metricWriter.whitelisted(MetricType.USED)) {
                    metricWriter.field(commodityType, MetricType.USED, metricJitter.jitter(bought.getUsed()));
                }
                if (bought.hasPeak() && metricWriter.whitelisted(MetricType.PEAK)) {
                    metricWriter.field(commodityType, MetricType.PEAK, metricJitter.jitter(bought.getPeak()));
                }
                if (bought.hasScalingFactor() && metricWriter.whitelisted(MetricType.SCALING_FACTOR)) {
                    metricWriter.field(commodityType, MetricType.SCALING_FACTOR, bought.getScalingFactor());
                }
            });

        return metricWriter.write();
    }

    /**
     * Write metrics for a commodity sold by a particular service entity.
     *
     * For commodities sold we can store [USED, PEAK, CAPACITY, and SCALING_FACTOR] depending
     * on what is on the whitelist.
     *
     * @param metricWriter The writer to use to write the metrics.
     * @param sold The commodity being sold.
     */
    private void writeCommoditySold(@Nonnull final MetricWriter metricWriter,
                                    @Nonnull final CommoditySoldDTOOrBuilder sold) {
        final CommodityType commodityType = CommodityType.forNumber(sold.getCommodityType().getType());

        if (sold.hasUsed() && metricWriter.whitelisted(MetricType.USED)) {
            metricWriter.field(commodityType, MetricType.USED, metricJitter.jitter(sold.getUsed()));
        }
        if (sold.hasPeak() && metricWriter.whitelisted(MetricType.PEAK)) {
            metricWriter.field(commodityType, MetricType.PEAK, metricJitter.jitter(sold.getPeak()));
        }
        if (sold.hasCapacity() && metricWriter.whitelisted(MetricType.CAPACITY)) {
            metricWriter.field(commodityType, MetricType.CAPACITY, sold.getCapacity());
        }
        if (sold.hasScalingFactor() && metricWriter.whitelisted(MetricType.SCALING_FACTOR)) {
            metricWriter.field(commodityType, MetricType.SCALING_FACTOR, sold.getScalingFactor());
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

    /**
     * Construct a builder for a commodity bought data point.
     *
     * @param entityOid The oid of the entity buying the commodity.
     * @param entityType The type of the entity buying the commodity.
     * @param writerContext The writer context to be used for writing the metrics.
     * @return a builder for a commodity bought data point.
     */
    private Point.Builder boughtPointBuilder(final long entityOid,
                                             final EntityType entityType,
                                             @Nonnull final WriterContext writerContext) {
        return Point.measurement("commodity_bought")
            .time(writerContext.timeMs, TimeUnit.MILLISECONDS)
            .tag("entity_type", entityType.name())
            .tag("oid", Long.toString(entityOid));
    }

    /**
     * Construct a builder for a commodity sold data point.
     *
     * @param entityOid The oid of the entity selling the commodity.
     * @param entityType The type of the entity selling the commodity.
     * @param writerContext The writer context to be used for writing the metrics.
     * @return a builder for a commodity sold data point.
     */
    private Point.Builder soldPointBuilder(final long entityOid,
                                           @Nonnull final EntityType entityType,
                                           @Nonnull final WriterContext writerContext) {
        return Point.measurement("commodity_sold")
            .time(writerContext.timeMs, TimeUnit.MILLISECONDS)
            .tag("entity_type", entityType.name())
            .tag("oid", Long.toString(entityOid));
    }

    /**
     * A small helper class to assist in writing data points to influx and tracking statistics
     * about the data written to influx.
     */
    private static class WriterContext {
        public final InfluxDB influxConnection;
        public final  Map<String, Long> boughtStats;
        public final  Map<String, Long> soldStats;
        public final long timeMs;

        public final Set<Integer> commoditiesWhitelist;
        public final Set<MetricType> metricTypesWhitelist;

        public WriterContext(@Nonnull final InfluxDB influxConnection,
                             @Nonnull final Map<String, Long> boughtStats,
                             @Nonnull final Map<String, Long> soldStats,
                             @Nonnull final Set<Integer> commoditiesWhitelist,
                             @Nonnull final Set<MetricType> metricTypesWhitelist,
                             final long timeMs) {
            this.influxConnection = Objects.requireNonNull(influxConnection);
            this.boughtStats = Objects.requireNonNull(boughtStats);
            this.soldStats = Objects.requireNonNull(soldStats);
            this.timeMs = timeMs;
            this.commoditiesWhitelist = Objects.requireNonNull(commoditiesWhitelist);
            this.metricTypesWhitelist = Objects.requireNonNull(metricTypesWhitelist);
        }

        /**
         * Increment the metric count for a commodity bought statistic with a given name.
         *
         * @param commodityType The type of the commodity whose bought metric is being recorded.
         * @param metricType The type of the metric bought being recorded.
         * @return The incremented count for the statistic.
         */
        public Long incrementBoughtMetric(@Nonnull final CommodityType commodityType,
                                          @Nonnull final MetricType metricType) {
            return boughtStats.merge(InfluxMetricsWriter.metricValueName(commodityType, metricType),
                1L, Long::sum);
        }

        /**
         * Increment the metric count for a commodity sold statistic with a given name.
         *
         * @param commodityType The type of the commodity whose sold metric is being recorded.
         * @param metricType The type of the metric sold being recorded.
         * @return The incremented count for the statistic.
         */
        public Long incrementSoldMetric(@Nonnull final CommodityType commodityType,
                                        @Nonnull final MetricType metricType) {
            return soldStats.merge(InfluxMetricsWriter.metricValueName(commodityType, metricType),
                1L, Long::sum);
        }
    }

    /**
     * A small helper class used to assist in writing metrics to influx.
     */
    private class MetricWriter {
        private final WriterContext writerContext;
        private final Point.Builder pointBuilder;
        private final BiConsumer<CommodityType, MetricType> metricStatsFunction;
        private int fieldCount = 0;

        public MetricWriter(@Nonnull final WriterContext writerContext,
                            @Nonnull final Point.Builder pointBuilder,
                            @Nonnull final BiConsumer<CommodityType, MetricType> metricStatsFunction) {
            this.writerContext = Objects.requireNonNull(writerContext);
            this.pointBuilder = Objects.requireNonNull(pointBuilder);
            this.metricStatsFunction = Objects.requireNonNull(metricStatsFunction);
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
            pointBuilder.addField(metricValueName(commodityType, metricType), value);

            metricStatsFunction.accept(commodityType, metricType);
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
}
