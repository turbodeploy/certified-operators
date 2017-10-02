package com.vmturbo.proactivesupport;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.annotation.Nonnull;

import com.google.gson.annotations.SerializedName;
import com.vmturbo.gson.GsonCallbacks;

/**
 * The {@link DataMetricBase} implements the basic DataMetric. It also supports listeners on the
 * metric, that will get called whenever a change to the metric is made.
 * NOTE: the listeners are not synchronized to the update methods and are not guaranteed to be
 * called in the order the values are changed. We are not using the listeners at present and are
 * leaving this open for future correction. In the future, we should probably either commit to
 * the listeners and synchronize the update w/the observer callbacks, or remove the listeners
 * altogether.
 *
 * @param <MetricType> The DataMetric type
 * @param <ValueType> The Data Holder type -- stores per-label-set values associated with the data metric
 * @param <ListenerType> The Listener interface that can be used to follow changes on the metric data
 */
public abstract class DataMetricBase<MetricType, ValueType extends DataMetricBase.LabeledMetricData,
        ListenerType> implements DataMetric<MetricType>,GsonCallbacks {
    /**
     * The metric type
     */
    private final DataMetric.MetricType type_;

    /**
     * The metric name.
     */
    private final String name_;

    /**
     * The metric set of labels.
     */
    private final String[] labelNames_;

    /**
     * The help.
     */
    private final String help_;

    /**
     * The metric severity.
     */
    private final Severity severity_;

    /**
     * The metric urgency.
     */
    private final boolean urgent_;

    /**
     * The labels metrics.
     */
    @SerializedName("labeledMetrics_")
    private final Map<List<String>, ValueType> labeledMetrics_;

    /**
     * A collection of listeners that will watch this metric for updates. This collection is
     * transient to avoid serialization.
     *
     * We expect the number of modifications (i.e. listeners being added or removed) to the list
     * to be very low, while the number of reads on the list (which would happen every time the
     * metric is updated) to be very high and potentially concurrent. So a
     * CopyOnWriteArrayList is used to back this list.
     */
    transient final private List<ListenerType> listeners;

    /**
     * A read-only wrapper around the listeners list that is shared with the metric data instances.
     * Also transient.
     */
    transient final private List<ListenerType> listenersView;

    /**
     * No-args constructor for GSON deserialization. GSON will not initialize the transient variables correctly
     * without this. Note: although we are also initializing final non-transient variables here, GSON may still
     * overwrite them during deserialization (via reflection).
     */
    protected DataMetricBase() {
        type_ = null;
        name_ = null;
        labelNames_ = new String[0];
        labeledMetrics_ = new HashMap<>();
        help_ = null;
        severity_ = null;
        urgent_ = false;
        listeners = new CopyOnWriteArrayList<>();
        listenersView = Collections.unmodifiableList(listeners);
    }


    /**
     * Constructs the base data metric.
     *
     * @param type     The metric type.
     * @param name     The metric name.
     * @param labelNames   The metric label names.
     * @param help     The metric help.
     * @param severity The metric severity.
     * @param urgent   The urgent flag.
     */
    protected DataMetricBase(final @Nonnull DataMetric.MetricType type,
                             final @Nonnull String name,
                             final @Nonnull String[] labelNames,
                             final @Nonnull String help,
                             final @Nonnull Severity severity,
                             final boolean urgent) {
        type_ = Objects.requireNonNull(type);
        name_ = Objects.requireNonNull(name);
        labelNames_ = Objects.requireNonNull(labelNames);
        help_ = Objects.requireNonNull(help);
        severity_ = Objects.requireNonNull(severity);
        urgent_ = urgent;
        labeledMetrics_ = new HashMap<>();
        listeners = new CopyOnWriteArrayList<>();
        listenersView = Collections.unmodifiableList(listeners);
    }

    /**
     * Returns the metric type.
     *
     * @return The metric type.
     */
    public @Nonnull
    DataMetric.MetricType getType() {
        return type_;
    }

    /**
     * Returns the metric labels.
     *
     * @return The metric labels.
     */
    public @Nonnull String[] getLabelNames() {
        return labelNames_;
    }

    /**
     * Returns the metric help.
     *
     * @return The metric help.
     */
    public @Nonnull String getHelp() {
        return help_;
    }

    /**
     * Returns the data collector id.
     * For example, it could be "heap", "disk_space", etc.
     *
     * @return The metric name.
     */
    public @Nonnull String getName() {
        return name_;
    }

    /**
     * Returns the severity.
     *
     * @return The metric severity.
     */
    public @Nonnull Severity getSeverity() {
        return severity_;
    }

    /**
     * Returns the urgency flag.
     *
     * @return The metric urgency.
     */
    public boolean isUrgent() {
        return urgent_;
    }

    /**
     * Register with the LDCF.
     */
    public MetricType register() {
        DataCollectorFramework.instance().registerMetric(this);
        return (MetricType)this;
    }

    /**
     * Composes the key from the array of labels.
     *
     * @param labels The labels.
     * @return The key.
     */
    protected List<String> composeKey(String[] labels) {
        return Arrays.asList(labels);
    }

    /**
     * Returns the metric data associated with the specified labels.
     * <p>
     * Note that to preserve the key:value relationship between label names and values, we
     * require the number of labels
     * passed in the parameters to equal the number of label names associated with the metric.
     *
     * @param labels The label values associated with the data being referenced
     * @return The metric data associated with the label values
     */
    public ValueType labels(final @Nonnull String... labels) {
        if (labels.length != labelNames_.length) {
            throw new IllegalArgumentException("This metric requires "+ labelNames_.length +" labels to be specified " +
                    "per value.");
        }

        List<String> metricKey = composeKey(labels);
        synchronized (labeledMetrics_) {
            return labeledMetrics_.computeIfAbsent(metricKey, k -> constructDefault( labels ));
        }
    }

    /**
     * Returns the labelled metrics.
     *
     * @return The copy of labels metrics.
     */
    public Map<List<String>, ValueType> getLabeledMetrics() {
        synchronized (labeledMetrics_) {
            return new HashMap<>(labeledMetrics_);
        }
    }

    /**
     * Constructs the instance of the metric with all the default values.
     *
     * @return The instance of the metric with all the default values.
     */
    protected abstract ValueType constructDefault(String[] labels );

    /**
     * postDeserialize() is a post-gson-deserialiation hook that will be called if you install the
     * {@link com.vmturbo.gson.CallbackTriggeringTypeAdapterFactory} into the gson builder.
     *
     * Gson does not preserve outer class references when deserializing pure inner classes, so the data holder inner
     * classes are static, with their own copies of some metric metadata and explicit parent reference variables.
     *
     * So that we don't need to serialize the extra copies of this metadata, we are providing this post-deserialization
     * function that will repopulate the transient data in the data holder instances.
     */
    public void postDeserialize() {
        // repopulate the transient owner references and label value lists on my child data
        labeledMetrics_.forEach((labels,data) -> {
            data.setLabels(labels.toArray(new String[ labels.size() ]));
            data.setMetric(this);
        } );
    }

    /**
     * Add a new listener to this metric
     * @param listener
     */
    public void addListener(final @Nonnull ListenerType listener ) {
        listeners.add(listener);
    }

    /**
     * Remove an listener from this metric
     * @param listener
     */
    public void removeListener( final @Nonnull ListenerType listener ) {
        if ( listeners.contains(listener) ) {
            listeners.remove(listener);
        }
    }

    /**
     * Get access to the read-only list of listeners of this metric
     * @return
     */
    public List<ListenerType> getListeners() {
        return listenersView;
    }

    /**
     * Abstract base class representing a piece of labels metric data.
     *
     * Originally this class was a pure inner class reading the listeners directly from the outer class. GSON
     * deserialization doesn't reconstruct the inner->outer class relationship properly and also doesn't like circular
     * references (owner -> data -> owner in this case), so this class is now a static inner class with an explicit
     * transient reference ("metric" instead of "outer.this") to the metric that owns the data.
     *
     * This reference will get recreated during deserialization when a call to metric.postDeserialization() is made.
     */
    static public abstract class LabeledMetricData<MetricType> {

        /**
         * The list of labels associated with this particular value
         */
        transient private String[] labels_;

        /**
         * The DataMetricBase owner of this data
         */
        transient protected MetricType metric;

        /**
         * no-args constructor for GSON
         */
        public LabeledMetricData() {
            labels_ = new String[0]; // initialize with empty string
        }

        /**
         * Constructor used when metrics are created during runtime.
         * @param labels
         */
        public LabeledMetricData(MetricType owner, String[] labels) {
            metric = owner;
            labels_ = labels;
        }

        public MetricType getMetric() { return metric; }
        public void setMetric(MetricType newMetric) { metric = newMetric; }

        public String[] getLabels() { return labels_; }
        public void setLabels(String[] newLabels) { labels_ = newLabels; }
    }
}
