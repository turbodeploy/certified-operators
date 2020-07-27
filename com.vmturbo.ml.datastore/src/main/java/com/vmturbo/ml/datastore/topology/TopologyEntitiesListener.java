package com.vmturbo.ml.datastore.topology;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.opentracing.SpanContext;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.ml.datastore.influx.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.influxdb.InfluxDB;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.ml.datastore.influx.InfluxMetricsWriterFactory.InfluxUnavailableException;
import com.vmturbo.proactivesupport.DataMetricGauge;
import com.vmturbo.proactivesupport.DataMetricHistogram;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.processor.api.EntitiesListener;

/**
 * Handler for entity information coming in from the Topology Processor.
 *
 * Listens only to the live topology.
 *
 * Writes metrics from the topology to influx for use in training ML data models.s
 */
public class TopologyEntitiesListener implements EntitiesListener {
    private final Logger logger = LogManager.getLogger();

    /**
     * Connection factory used to establish connections to influx.
     */
    private final InfluxMetricsWriterFactory connectionFactory;

    /**
     * Whitelist of commodities and metric types to write.
     */
    private final MetricsStoreWhitelist metricsStoreWhitelist;

    /**
     * Jitter to apply to metrics. Only used in development.
     */
    private final MetricJitter metricJitter;

    /**
     * Used to mask sensitive customer information (ie host or cluster names, ip addresses, etc.)
     */
    private final Obfuscator obfuscator;

    /**
     * Present if the listener has an open connection to influx.
     * May be closed and re-opened if an error occurs writing to influx.
     */
    private Optional<InfluxTopologyMetricsWriter> metricsWriter;

    /**
     * How long it takes to write topology metrics to influx.
     */
    public static final DataMetricHistogram TOPOLOGY_METRIC_WRITE_TIME_HISTOGRAM = DataMetricHistogram.builder()
        .withName("mldata_topology_metric_write_seconds")
        .withHelp("How long it takes to write the metrics for a topology.")
        .withBuckets(1, 10, 60, 120, 180, 240, 360, 480, 600, 1200)
        .build();

    public static final DataMetricGauge TOPOLOGY_METRIC_FIELDS_WRITTEN = DataMetricGauge.builder()
        .withName("mldata_topology_metric_field_count")
        .withHelp("The number of metric fields written for the last broadcast.")
        .build();

    public TopologyEntitiesListener(@Nonnull final InfluxMetricsWriterFactory connectionFactory,
                                    @Nonnull final MetricsStoreWhitelist metricsStoreWhitelist,
                                    @Nonnull final MetricJitter metricJitter,
                                    @Nonnull final Obfuscator obfuscator) {
        metricsWriter = Optional.empty();

        this.connectionFactory = Objects.requireNonNull(connectionFactory);
        this.metricsStoreWhitelist = Objects.requireNonNull(metricsStoreWhitelist);
        this.metricJitter = Objects.requireNonNull(metricJitter);
        this.obfuscator = Objects.requireNonNull(obfuscator);
    }

    /**
     * {@inheritDoc}
     *
     * Writes topology metrics to influx. Metrics are written as we receive topology chunks
     * so that we never have to retain the full topology in memory, reducing the memory
     * footprint of this component.
     */
    @Override
    public void onTopologyNotification(@Nonnull final TopologyInfo topologyInfo,
                                       @Nonnull final RemoteIterator<TopologyDTO.Topology.DataSegment> entityIterator,
                                       @Nonnull final SpanContext tracingContext) {
        final long topologyContextId = topologyInfo.getTopologyContextId();
        final long topologyId = topologyInfo.getTopologyId();
        final long timeMs = topologyInfo.getCreationTime();
        final Map<String, Long> boughtStatistics = new HashMap<>();
        final Map<String, Long> soldStatistics = new HashMap<>();
        final Map<String, Long> clusterStatistics = new HashMap<>();

        logger.info("Received topology {} context {} creation time {} type {}.",
            topologyInfo.getTopologyId(),
            topologyInfo.getTopologyContextId(),
            topologyInfo.getCreationTime(),
            topologyInfo.getTopologyType());

        long totalDataPointsWritten = 0;
        try {
            final InfluxTopologyMetricsWriter metricsWriter = getOrCreateMetricsWriter();

            final DataMetricTimer topologyWriterTimer = TOPOLOGY_METRIC_WRITE_TIME_HISTOGRAM.startTimer();
            while (entityIterator.hasNext()) {
                Collection<TopologyEntityDTO> dtos =
                    entityIterator.nextChunk().stream()
                                  .filter(TopologyDTO.Topology.DataSegment::hasEntity)
                                  .map(TopologyDTO.Topology.DataSegment::getEntity)
                                  .collect(Collectors.toList());
                totalDataPointsWritten += metricsWriter.writeMetrics(dtos, timeMs,
                    boughtStatistics,
                    soldStatistics,
                    clusterStatistics);
            }

            // Flush to write through any cached data points after we have received the full topology.
            metricsWriter.flush();

            // Log statistics about the metrics written.
            final double timeTaken = topologyWriterTimer.observe();
            logStatistics(timeTaken, boughtStatistics, soldStatistics,
                clusterStatistics, totalDataPointsWritten);
            TOPOLOGY_METRIC_FIELDS_WRITTEN.setData((double)totalDataPointsWritten);
        } catch (CommunicationException | TimeoutException e) {
            logger.error("Error occurred while receiving topology " + topologyId +
                " for context " + topologyContextId, e);

            // Close the connection to influx. It will be re-created on receiving the next topology.
            metricsWriter = Optional.empty();

            // Attempt to create a new connection to influx.
        } catch (InterruptedException e) {
            logger.info("Thread interrupted receiving topology " + topologyId +
                " for context " + topologyContextId, e);
        } catch (InfluxUnavailableException e) {
            // TODO: If influx is unavailable, wait for it to be available again before handling
            // broadcasts.
            logger.error("Influx unavailable: ", e);
        } catch (RuntimeException e) {
            logger.error("Unexpected error occurred while receiving topology " + topologyId +
                " for context " + topologyContextId, e);

            // Close the connection to influx. It will be re-created on receiving the next topology.
            metricsWriter = Optional.empty();
            throw e; // Re-throw the exception to abort reading the topology.
        }
    }

    /**
     * Get the InfluxDB connection to be used to write metric data to influx.
     * If no such connection exists, returns {@link Optional#empty()}
     *
     * @return the InfluxDB connection to be used to write metric data to influx.
     * @throws InfluxUnavailableException if no connection to influx can be established.
     */
    @Nonnull
    public InfluxDB getInfluxConnection() throws InfluxUnavailableException {
        return metricsWriter.orElseGet(() -> getOrCreateMetricsWriter()).getInfluxConnection();
    }

    /**
     * Log statistics about the metrics from the topology that were written to influx after
     * completely receiving a topology broadcast.
     *
     * @param timeTakenSeconds The amount of time in seconds it took to receive the broadcast
     *                         and write the contained metrics to influx.
     * @param boughtStatistics Statistics about the commodities bought metrics written to influx.
     *                         Each commodity-metric type combination results in one field value written to influx.
     *                         (ie VCPU_USED, VCPU_PEAK, MEM_CAPACITY would be 3 field values)
     * @param soldStatistics Statistics about the commodities sold metrics written to influx.
     *                       Each commodity-metric type combination results in one field value written to influx.
     *                       (ie VCPU_USED, VCPU_PEAK, MEM_CAPACITY would be 3 field values)
     * @param clusterStatistics Statistics about the cluster memberships written to influx.
     *                          Each cluster membership results in one field value written to influx.
     *                          (ie STORAGE_CLUSTER and COMPUTE_CLUSTER would be 2 field values)
     * @param totalDataPointsWritten The total number of data points written to influx.
     *                               Each entity selling at least one commodity results in one data point.
     *                               One data point is written for each provider of a commodity bought
     *                               for a service entity.
     */
    private void logStatistics(final double timeTakenSeconds,
                               @Nonnull final Map<String, Long> boughtStatistics,
                               @Nonnull final Map<String, Long> soldStatistics,
                               @Nonnull final Map<String, Long> clusterStatistics,
                               final long totalDataPointsWritten) {
        final String timeTaken = TimeUtil.humanReadable(
            Duration.ofSeconds((long) timeTakenSeconds, (long) ((timeTakenSeconds % 1) * 1e9)));
        final long totalBought = totalWritten(boughtStatistics);
        final long totalSold = totalWritten(soldStatistics);
        final long totalCluster = totalWritten(clusterStatistics);

        logger.info(" Took {} to write {} metric fields ({} data points) to db \"{}\" " +
                "and retention policy \"{}\". Wrote:" +
                "\n\tBought: Total {} {}" +
                "\n\tSold: Total {} {}" +
                "\n\tCluster Membership: Total {} {}",
            timeTaken,
            totalBought + totalSold + totalCluster,
            totalDataPointsWritten,
            connectionFactory.getDatabase(),
            connectionFactory.getRetentionPolicyName(),
            totalBought, boughtStatistics,
            totalSold, soldStatistics,
            totalCluster, clusterStatistics);
    }

    /**
     * Return a metrics writer capable of writing to influx.  If we already have such a metrics
     * writer and the connection is healthy, re-use it, otherwise create a new one.
     *
     * @return a metrics writer capable of writing to influx.
     * @throws InfluxUnavailableException if no connection to influx can be established.
     */
    @Nonnull
    private InfluxTopologyMetricsWriter getOrCreateMetricsWriter() throws InfluxUnavailableException {
        if (!metricsWriter.isPresent() || !connectionIsHealthy()) {
            metricsWriter = Optional.of(
                    connectionFactory.createTopologyMetricsWriter(metricsStoreWhitelist, metricJitter, obfuscator));
        }

        return metricsWriter.get();
    }

    private boolean connectionIsHealthy() {
        try {
            if (metricsWriter.isPresent()) {
                metricsWriter.get().getInfluxConnection().ping();
                return true;
            }
        } catch (Exception e) {
            logger.error("Influx connection is unhealthy. Will attempt to recreate connection.");
        }

        return false;
    }

    /**
     * Compute the total number of metrics written as the sum of all the individual metric types
     * in the map.
     *
     * @param stats The map of metric types to the number of metrics of that type that were written.
     * @return the total number of metrics written.
     */
    final long totalWritten(@Nonnull final Map<String, Long> stats) {
        return stats.values().stream()
            .mapToLong(l -> l)
            .sum();
    }
}
