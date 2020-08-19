package com.vmturbo.ml.datastore.topology;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import io.opentracing.SpanContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.influxdb.InfluxDB;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan.ActionPlanType;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO;
import com.vmturbo.common.protobuf.ml.datastore.MLDatastore.ActionStateWhitelist.ActionState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.ml.datastore.influx.InfluxActionsWriter;
import com.vmturbo.ml.datastore.influx.InfluxMetricsWriter;
import com.vmturbo.ml.datastore.influx.InfluxMetricsWriterFactory;
import com.vmturbo.ml.datastore.influx.InfluxMetricsWriterFactory.InfluxUnavailableException;
import com.vmturbo.ml.datastore.influx.MetricsStoreWhitelist;
import com.vmturbo.proactivesupport.DataMetricGauge;
import com.vmturbo.proactivesupport.DataMetricHistogram;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Handler for entity information coming in from the Topology Processor.
 *
 * Listens only to the live topology.
 *
 * Writes metrics from the topology to influx for use in training ML data models.s
 */
public class ActionMetricsListener implements com.vmturbo.action.orchestrator.api.ActionsListener,
        com.vmturbo.market.component.api.ActionsListener {
    private final Logger logger = LogManager.getLogger();

    private static final long REALTIME_CONTEXT_ID = 777777;
    /**
     * Connection factory used to establish connections to influx.
     */
    private final InfluxMetricsWriterFactory connectionFactory;

    /**
     * Whitelist of commodities and metric types to write.
     */
    private final MetricsStoreWhitelist metricsStoreWhitelist;

    /**
     * Present if the listener has an open connection to influx.
     * May be closed and re-opened if an error occurs writing to influx.
     */
    private Optional<InfluxActionsWriter> metricsWriter;

    /**
     * How long it takes to write topology metrics to influx.
     */
    public static final DataMetricHistogram ACTIONS_METRIC_WRITE_TIME_HISTOGRAM = DataMetricHistogram.builder()
            .withName("mldata_action_metric_write_seconds")
            .withHelp("How long it takes to write the metrics for actions.")
            .withBuckets(1, 10, 60, 120, 180, 240, 360, 480, 600, 1200)
            .build();

    public static final DataMetricGauge ACTION_METRIC_FIELDS_WRITTEN = DataMetricGauge.builder()
            .withName("mldata_action_metric_field_count")
            .withHelp("The number of metric fields written for the last broadcast.")
            .build();

    public ActionMetricsListener(@Nonnull final InfluxMetricsWriterFactory connectionFactory,
                                 @Nonnull final MetricsStoreWhitelist metricsStoreWhitelist) {
        metricsWriter = Optional.empty();

        this.connectionFactory = Objects.requireNonNull(connectionFactory);
        this.metricsStoreWhitelist = Objects.requireNonNull(metricsStoreWhitelist);
    }

    @Override
    public void onActionsReceived(@Nonnull final ActionDTO.ActionPlan actionPlan,
                                  @Nonnull final SpanContext tracingContext) {
        final Map<String, Long> actionsStatistics = new HashMap<>();
        if (ActionDTOUtil.getActionPlanType(actionPlan.getInfo()) != ActionPlanType.MARKET) {
            // We only care about actions.
            return;
        }

        final TopologyInfo topologyInfo = actionPlan.getInfo().getMarket().getSourceTopologyInfo();
        final long topologyContextId = topologyInfo.getTopologyContextId();
        // process just realtime actions
        if (topologyContextId != REALTIME_CONTEXT_ID) {
            return;
        }
        final long topologyId = topologyInfo.getTopologyId();

        long totalDataPointsWritten = 0;
        try {
            final InfluxActionsWriter metricsWriter = getOrCreateMetricsWriter();

            final DataMetricTimer actionsWriterTimer = ACTIONS_METRIC_WRITE_TIME_HISTOGRAM.startTimer();
            totalDataPointsWritten = metricsWriter.writeMetrics(actionPlan,
                                                actionsStatistics, ActionState.RECOMMENDED_ACTIONS);

            // Flush to write through any cached data points after we have received the full topology.
            metricsWriter.flush();

            // Log statistics about the metrics written.
            final double timeTaken = actionsWriterTimer.observe();
            logStatistics(timeTaken, actionsStatistics, totalDataPointsWritten);
            ACTION_METRIC_FIELDS_WRITTEN.setData((double)totalDataPointsWritten);
        } catch (InfluxUnavailableException e) {
            // TODO: If influx is unavailable, wait for it to be available again before handling
            // broadcasts.
            logger.error("Influx unavailable: ", e);
        } catch (RuntimeException e) {
            logger.error("Unexpected error occurred while receiving topology " + topologyId +
                    " for context " + topologyContextId, e);

            // Close the connection to influx. It will be re-created on receiving the next topology.
            metricsWriter = Optional.empty();
            throw e; // Re-throw the exception to abort reading the actions.
        }
    }

    @Override
    public void onActionProgress(@Nonnull ActionNotificationDTO.ActionProgress actionProgress) {

    }

    @Override
    public void onActionSuccess(@Nonnull ActionNotificationDTO.ActionSuccess actionSuccess) {

        final InfluxActionsWriter metricsWriter = getOrCreateMetricsWriter();
        metricsWriter.writeCompletedActionMetrics(actionSuccess);
    }

    @Override
    public void onActionFailure(@Nonnull ActionNotificationDTO.ActionFailure actionFailure) {

    }

    /**
     * Get the InfluxDB connection to be used to write metric data to influx.
     * If no such connection exists, returns {@link Optional#empty()}
     *
     * @return the InfluxDB connection to be used to write metric data to influx.
     */
    public Optional<InfluxDB> getInfluxConnection() {
        return metricsWriter
                .map(InfluxMetricsWriter::getInfluxConnection);
    }

    /**
     * Log statistics about the metrics from the topology that were written to influx after
     * completely receiving a topology broadcast.
     *
     * @param timeTakenSeconds The amount of time in seconds it took to receive the broadcast
     *                         and write the contained metrics to influx.
     * @param actionsStatistics Statistics about the actions metrics written to influx.
     * @param totalDataPoitnsWritten The total number of data points written to influx.
     *                               Each actions results in 1 data point
     */
    private void logStatistics(final double timeTakenSeconds,
                               @Nonnull final Map<String, Long> actionsStatistics,
                               final long totalDataPoitnsWritten) {
        final String timeTaken = TimeUtil.humanReadable(
                Duration.ofSeconds((long) timeTakenSeconds, (long) ((timeTakenSeconds % 1) * 1e9)));
        final long totalActions = totalWritten(actionsStatistics);

        logger.info(" Took {} to write {} metric fields ({} data points) to db \"{}\" " +
                        "and retention policy \"{}\". Wrote:" +
                        "\n\tActions: Total {} {}",
                timeTaken,
                totalActions,
                totalDataPoitnsWritten,
                connectionFactory.getDatabase(),
                connectionFactory.getRetentionPolicyName(),
                totalActions, actionsStatistics);
    }

    /**
     * Return a metrics writer capable of writing to influx.
     *
     * If we already have such a metrics writer, re-use it, otherwise create a new one.
     *
     * @return a metrics writer capable of writing to influx.
     * @throws InfluxUnavailableException if no connection to influx can be established.
     */
    private InfluxActionsWriter getOrCreateMetricsWriter() throws InfluxUnavailableException {
        if (!metricsWriter.isPresent() || !connectionIsHealthy()) {
            // obfuscator and metricJitter will are NULL for ActionsListener
            metricsWriter = Optional.of(
                    connectionFactory.createActionMetricsWriter(metricsStoreWhitelist));
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
