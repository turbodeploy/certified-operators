package com.vmturbo.proactivesupport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;
import com.google.common.annotations.VisibleForTesting;

/**
 * The DataCollectorFramework implements Local Data Collection Framework (LDCF), as specified in
 * the design document. <br>
 * https://vmturbo.atlassian.net/wiki/pages/viewpage
 * .action?spaceKey=Home&title=Turbonomic+Remote+Telemetry
 */
@Component
public class DataCollectorFramework implements Runnable {
    /**
     * The logger.
     */
    private final Logger logger_ = LogManager.getLogger(DataCollectorFramework.class);

    /**
     * The singleton instance.
     */
    @VisibleForTesting
    static DataCollectorFramework instance_ = null;

    /**
     * The urgent collection interval setting.
     */
    private long collectionIntervalUrgent_;

    /**
     * The offline collection interval setting.
     */
    private long collectionIntervalOffline_;

    /**
     * The urgent data collectors.
     */
    @VisibleForTesting final Collection<DataMetric> collectorsUrgent_;

    /**
     * The offline data collectors.
     */
    @VisibleForTesting final Collection<DataMetric> collectorsOffline_;

    /**
     * The data analyzers.
     */
    @VisibleForTesting final Collection<DataAnalyzer> analyzers_;

    /**
     * The executing thread.
     */
    @VisibleForTesting
    Thread runningThread;

    /**
     * The aggregator bridge.
     */
    private AggregatorBridge aggregatorBridge_;

    /**
     * The list of DataMetricCollectionListeners to notify when metrics are added / removed.
     */
    private final List<DataMetricCollectionListener> metricCollectionListeners_;

    /**
     * The key/value store.
     */
    private @Nullable KVCollector kvCollector_;

    /**
     * The global enabled flag.
     */
    private boolean enabled_;

    /**
     * Constructs the LDCF.
     */
    @VisibleForTesting DataCollectorFramework() {
        collectorsUrgent_ = Collections.synchronizedList(new ArrayList<>());
        collectorsOffline_ = Collections.synchronizedList(new ArrayList<>());
        analyzers_ = Collections.synchronizedList(new ArrayList<>());
        metricCollectionListeners_ = Collections.synchronizedList(new ArrayList<>());
        enabled_ = true;
    }

    /**
     * Sets the key/value collector.
     *
     * @param kvCollector The key/value collector.
     */
    public void setKeyValueCollector(final @Nonnull KVCollector kvCollector) {
        kvCollector_ = Objects.requireNonNull(kvCollector);
    }

    /**
     * Sets the aggregator bridge for the LDCF.
     *
     * @param aggregatorBridge The aggregator bridge.
     */
    public void setAggregatorBridge(final @Nonnull AggregatorBridge aggregatorBridge) {
        aggregatorBridge_ = Objects.requireNonNull(aggregatorBridge);
    }

    /**
     * Returns the singleton instance of the LDCF.
     *
     * @return The singleton instance of the LDCF.
     */
    public static synchronized DataCollectorFramework instance() {
        if (instance_ == null) {
            instance_ = new DataCollectorFramework();
        }
        return instance_;
    }

    /**
     * Add a new DataMetricCollectionListener to the collection
     *
     * @param listener
     */
    public void registerMetricCollectionListener(@Nonnull DataMetricCollectionListener listener) {
        metricCollectionListeners_.add(listener);
    }

    /**
     * Remove a DataMetricCollectionListener
     *
     * @param listener
     */
    public void unregisterMetricCollectionListener(@Nonnull DataMetricCollectionListener listener) {
        metricCollectionListeners_.remove(listener);
    }

    /**
     * Starts the collections.
     *
     * @param collectionIntervalUrgent  The urgent metrics collection interval.
     * @param collectionIntervalOffline The offline metrics collection interval.
     */
    public void start(long collectionIntervalUrgent,
                      long collectionIntervalOffline) {
        collectionIntervalUrgent_ = collectionIntervalUrgent;
        collectionIntervalOffline_ = collectionIntervalOffline;

        runningThread = new Thread(this);
        runningThread.setDaemon(true);
        runningThread.setName("LDCF collection thread");
        runningThread.start();
    }

    /**
     * Stops the collections.
     */
    public void stop() {
        if (runningThread == null || !runningThread.isAlive()) {
            return;
        }
        runningThread.interrupt();
        try {
            runningThread.join();
        } catch (InterruptedException e) {
            logger_.debug("The LDCF collection thread termination wait has been interrupted.");
        }
    }

    /**
     * Collect the urgent data from all Data Collectors.
     *
     * @return The collected data in form of @{code Map<type, message>}.
     */
    private Map<String, DataMetric> collectUrgent() {
        Collection<DataMetric> collectorsToScan;
        synchronized (collectorsUrgent_) {
            collectorsToScan = new ArrayList<>(collectorsUrgent_);
        }

        Map<String, DataMetric> collected = new HashMap<>();
        for (DataMetric collector : collectorsToScan) {
            collected.put(collector.getName(), collector);
        }
        return collected;
    }

    /**
     * Collect the offline data from all Data Collectors.
     *
     * @return The collected data.
     */
    private Collection<DataMetric> collectOffline() {
        synchronized (collectorsOffline_) {
            return new ArrayList<>(collectorsOffline_);
        }
    }

    /**
     * Performs the analysis on the collected data.
     *
     * @param collected The collected data.
     * @return The collection of all messages, including the ones produced by analysers.
     */
    private Collection<DataMetric> analyze(
            final @Nonnull Map<String, DataMetric> collected) {
        Collection<DataMetric> allMessages = new ArrayList<>(collected.values());

        Collection<DataAnalyzer> analyzersToInvoke;
        synchronized (analyzers_) {
            analyzersToInvoke = new ArrayList<>(analyzers_);
        }

        for (DataAnalyzer analyzer : analyzersToInvoke) {
            Map<String, DataMetric> needed = new HashMap<>();
            for (String type : analyzer.getCollectorTypes()) {
                DataMetric msg = collected.get(type);
                if (msg != null) {
                    needed.put(type, msg);
                }
            }
            // Add the result of the analysis.
            if (!needed.isEmpty()) {
                try {
                    allMessages.add(analyzer.analyze(needed));
                } catch (IOException e) {
                    logger_.error("Error executing data analyzer. Skipping.", e);
                }
            }
        }
        return allMessages;
    }

    /**
     * Checks whether LDCF is enabled.
     * The enabled state may be changed dynamically.
     *
     * @return {@code true} iff enabled.
     */
    private boolean checkEnabled() {
        enabled_ = (kvCollector_ == null) ? enabled_ : kvCollector_.isEnabled();
        return enabled_;
    }

    /**
     * Sleeps for the urgent collection interval.
     *
     * @return {@code true} if the run continues, {@code false} otherwise.
     */
    private boolean sleepUrgentInterval() {
        try {
            Thread.sleep(collectionIntervalUrgent_);
            return true;
        } catch (InterruptedException e) {
            logger_.warn("The LDCF collection thread has been interrupted. Aborting collection.");
            return false;
        }
    }

    /**
     * Runs the periodic collection of the telemetry data for this Turbonomic component.
     * The collected data will be sent over to the Data Aggregator..
     */
    public void run() {
        long lastCollected = System.currentTimeMillis();
        for (; ; ) {
            // Check whether we are enabled.
            // The side effects are:
            // 1. Thread that sleeps 10 minutes and checks the flag.
            // 2. The small overhead of Consul check. We have health checks already, so that's
            //    not a great burden.
            if (!checkEnabled()) {
                if (!sleepUrgentInterval()) {
                    break;
                }
                continue;
            }
            // Scan and collect the messages.
            Map<String, DataMetric> collected = collectUrgent();
            Collection<DataMetric> all = analyze(collected);
            // Forward the data.
            aggregatorBridge_.sendUrgent(all);
            // Collect offline as needed
            if (System.currentTimeMillis() - lastCollected >= collectionIntervalOffline_) {
                aggregatorBridge_.sendOffline(collectOffline());
                lastCollected = System.currentTimeMillis();
            }
            // Wait for next cycle.
            if (!sleepUrgentInterval()) {
                break;
            }
        }
    }

    /**
     * Adds the data collector.
     *
     * @param metric The data metric to be added.
     * @return This instance, so that fluent API style invocation would be possible.
     */
    public DataCollectorFramework registerMetric(final @Nonnull DataMetric metric) {
        Collection<DataMetric> list = metric.isUrgent() ? collectorsUrgent_ : collectorsOffline_;
        list.add(Objects.requireNonNull(metric));
        // fire metric added event.
        metricCollectionListeners_.forEach(listener -> listener.onDataMetricRegistered(metric));
        return this;
    }

    /**
     * Remove a data collector.
     *
     * @param metric The data metric to be removed.
     * @return This instance, so that fluent API style invocation would be possible.
     */
    public DataCollectorFramework unregisterMetric(final @Nonnull DataMetric metric) {
        Collection<DataMetric> list = metric.isUrgent() ? collectorsUrgent_ : collectorsOffline_;
        if (list.remove(metric)) {
            // fire metric removed event.
            metricCollectionListeners_.forEach(l -> l.onDataMetricUnregistered(metric));
        }
        return this;
    }

    /**
     * Adds the data analyzer.
     *
     * @param analyzer The data analyzer to be added.
     * @return This instance, so that fluent API style invocation would be possible.
     */
    public DataCollectorFramework addAnalyzer(final @Nonnull DataAnalyzer analyzer) {
        synchronized (analyzers_) {
            analyzers_.add(Objects.requireNonNull(analyzer));
        }
        return this;
    }

    /**
     * Make all of the metrics registered with the framework available to the caller. This is
     * being returned as a Stream of DataMetric instances.
     *
     * @return a Steam of the DataMetric instances registered with the collector.
     */
    public Stream<DataMetric> getAllMetrics() {
        return Stream.concat(collectorsUrgent_.stream(), collectorsOffline_.stream());
    }

    /**
     * Unregister all of the metrics registered with the framework.
     */
    public void clearAllMetrics() {
        // unregister all of the metrics
        getAllMetrics().collect(Collectors.toList()).forEach(this::unregisterMetric);
    }

    /**
     * The K/V collector that checks whether collection is enabled.
     */
    public interface KVCollector {
        boolean isEnabled();
    }
}
