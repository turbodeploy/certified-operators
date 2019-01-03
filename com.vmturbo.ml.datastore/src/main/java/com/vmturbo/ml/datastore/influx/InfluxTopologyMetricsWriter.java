package com.vmturbo.ml.datastore.influx;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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
 * Also writes cluster membership information if that is enabled.
 *
 * Note that no commodity keys are written. This may potentially lead to issues with multiple
 * bundles of commodities bought from the same provider differentiated by keys.
 * TODO: Fix the key problem if necessary.
 */
public class InfluxTopologyMetricsWriter extends InfluxMetricsWriter implements AutoCloseable {

    public static final String COMPUTE_CLUSTER_TYPE = "COMPUTE_CLUSTER";
    public static final String STORAGE_CLUSTER_TYPE = "STORAGE_CLUSTER";

    public static final String SOLD_SIDE = "SOLD";
    public static final String BOUGHT_SIDE = "BOUGHT";

    /**
     * Jitter that can be applied to metric values. Only used in testing.
     */
    protected final MetricJitter metricJitter;
    /**
     * Obfuscator used to obfuscate sensitive customer data (ie commoditiy keys which may contain
     * sensitive information such as host-names, ip addresses, etc.)
     */
    protected final Obfuscator obfuscator;

    private static final Logger logger = LogManager.getLogger();

    @VisibleForTesting
    InfluxTopologyMetricsWriter(@Nonnull final InfluxDB influxConnection,
                                @Nonnull final String database,
                                @Nonnull final String retentionPolicy,
                                @Nonnull final MetricsStoreWhitelist metricsStoreWhitelist,
                                @Nonnull final MetricJitter metricJitter,
                                @Nonnull final Obfuscator obfuscator) {
        super(influxConnection, database, retentionPolicy, metricsStoreWhitelist);
        this.metricJitter = Objects.requireNonNull(metricJitter);
        this.obfuscator = Objects.requireNonNull(obfuscator);
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
     * @param clusterStatistics A map of cluster type names to how many statstics of that type were written.
     *                          This map will be mutated by this method as it writes additional metrics to
     *                          count metrics written.
     *
     * @return The number of data points written.
     */
    public int writeMetrics(@Nonnull final Collection<TopologyEntityDTO> topologyChunk,
                                     final long timeMs,
                                     @Nonnull final Map<String, Long> boughtStatistics,
                                     @Nonnull final Map<String, Long> soldStatistics,
                                     @Nonnull final Map<String, Long> clusterStatistics) {
        Map<InfluxStatType, Map<String, Long>> stats = new HashMap<>();
        stats.put(InfluxStatType.COMMODITY_BOUGHT, boughtStatistics);
        stats.put(InfluxStatType.COMMODITY_SOLD, soldStatistics);
        stats.put(InfluxStatType.CLUSTER, clusterStatistics);
        final WriterContext context = new WriterContext(influxConnection,
            stats,
            metricsStoreWhitelist,
            timeMs);

        int numWritten = 0;
        for (TopologyEntityDTO entity : topologyChunk) {
            numWritten += writeEntityMetrics(entity, context);
            numWritten += writeClusterMemberships(entity, context);
        }
        return numWritten;
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
            soldPointBuilder(entity.getOid(), entityType, context), InfluxStatType.COMMODITY_SOLD);
        entity.getCommoditySoldListList().stream()
            .filter(CommoditySoldDTO::getActive)
            .filter(sold -> sold.getCommodityType().hasType() &&
                soldWriter.whitelisted(sold.getCommodityType().getType()))
            .forEach(commoditySold -> writeCommoditySold(soldWriter, commoditySold));
        numWritten += soldWriter.write();

        return numWritten;
    }

    /**
     * Write cluster membership information.
     * Writes both compute and storage cluster information.
     *
     * @param entity The entity whose cluster membership information should be written.
     * @param context The {@link WriterContext} to be used to write to influx and track
     *                metric statistics.
     * @return The number of data points written to influx for this service entity.
     *         Write one data point for every provider this entity actively buys from.
     *         Write one data point if any commodities are actively sold.
     */
    private int writeClusterMemberships(@Nonnull final TopologyEntityDTO entity,
                                        @Nonnull final WriterContext context) {
        if (metricsStoreWhitelist.getClusterSupport()) {
            final EntityType entityType = EntityType.forNumber(entity.getEntityType());
            final MetricWriter clusterWriter = new MetricWriter(context,
                clusterPointBuilder(entity.getOid(), entityType, context), InfluxStatType.CLUSTER);

            addClusterBoughtFields(entity.getCommoditiesBoughtFromProvidersList(), clusterWriter);
            addClusterSoldFields(entity.getCommoditySoldListList(), clusterWriter);
            return clusterWriter.write();
        } else {
            return 0;
        }
    }

    /**
     * Add metric fields for cluster membership to a {@link MetricWriter} for cluster membership info.
     * Note that cluster commodities are access commodities not written as part of the regular
     * commodities. They contain information in their keys indicating which clusters they belong to
     * that does not fit in the schema for metric-based commodities.
     *
     * @param commoditiesBought The commodities bought containing cluster membership information.
     * @param clusterWriter Writer for cluster membership data.
     */
    private void addClusterBoughtFields(@Nonnull final List<CommoditiesBoughtFromProvider> commoditiesBought,
                                        @Nonnull final MetricWriter clusterWriter) {
        commoditiesBought.stream()
            .forEach(boughtFromProvider -> {
                boughtFromProvider.getCommodityBoughtList().stream()
                    .filter(CommodityBoughtDTO::getActive)
                    .filter(CommodityBoughtDTO::hasCommodityType)
                    .filter(commodity -> {
                        final int type = commodity.getCommodityType().getType();
                        return type == CommodityType.CLUSTER_VALUE || type == CommodityType.STORAGE_CLUSTER_VALUE;
                    }).forEach(commodity -> clusterWriter.field(
                        clusterTypeName(commodity.getCommodityType().getType(), BOUGHT_SIDE),
                        obfuscator.obfuscate(commodity.getCommodityType().getKey()))
                    );
            });
    }

    /**
     * Add metric fields for cluster membership to a {@link MetricWriter} for cluster membership info.
     * Note that cluster commodities are access commodities not written as part of the regular
     * commodities. They contain information in their keys indicating which clusters they belong to
     * that does not fit in the schema for metric-based commodities.
     *
     * @param commoditiesSold The commodities sold containing cluster membership information.
     * @param clusterWriter Writer for cluster membership data.
     */
    private void addClusterSoldFields(@Nonnull final List<CommoditySoldDTO> commoditiesSold,
                                      @Nonnull final MetricWriter clusterWriter) {
        commoditiesSold.stream()
            .filter(CommoditySoldDTO::getActive)
            .filter(CommoditySoldDTO::hasCommodityType)
            .filter(commodity -> {
                final int type = commodity.getCommodityType().getType();
                return type == CommodityType.CLUSTER_VALUE || type == CommodityType.STORAGE_CLUSTER_VALUE;
            }).forEach(commodity -> clusterWriter.field(
                    clusterTypeName(commodity.getCommodityType().getType(), SOLD_SIDE),
                    obfuscator.obfuscate(commodity.getCommodityType().getKey()))
            );
    }

    private String clusterTypeName(final int type,
                                   @Nonnull final String side) {
        return (type == CommodityType.CLUSTER_VALUE ? COMPUTE_CLUSTER_TYPE : STORAGE_CLUSTER_TYPE)
            + "_" + side;
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
            boughtPointBuilder(entityOid, entityType, writerContext), InfluxStatType.COMMODITY_BOUGHT);

        commoditiesBought.stream()
            .filter(CommodityBoughtDTO::getActive)
            .filter(bought -> bought.getCommodityType().hasType() &&
                metricWriter.whitelisted(bought.getCommodityType().getType()))
            .forEach(bought -> {
                final CommodityType commodityType = CommodityType.forNumber(bought.getCommodityType().getType());

                metricWriter.getPointBuilder().tag("provider_id", Long.toString(providerId));
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
     * Construct a builder for a cluster membership data point.
     *
     * @param entityOid The oid of the entity belonging to the clutser.
     * @param entityType The type of the entity belonging to the cluster.
     * @param writerContext The writer context to be used for writing the metrics.
     * @return a builder for a commodity sold data point.
     */
    private Point.Builder clusterPointBuilder(final long entityOid,
                                              @Nonnull final EntityType entityType,
                                              @Nonnull final WriterContext writerContext) {
        return Point.measurement("cluster_membership")
            .time(writerContext.timeMs, TimeUnit.MILLISECONDS)
            .tag("entity_type", entityType.name())
            .tag("oid", Long.toString(entityOid));
    }

}
