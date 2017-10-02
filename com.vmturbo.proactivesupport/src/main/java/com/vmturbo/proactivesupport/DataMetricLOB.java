package com.vmturbo.proactivesupport;

import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The {@link DataMetricLOB} implements the BLOB metric.
 * For example, the binary logs serve as an example of such.
 * The term "metric" might not be the most comfortable in relation
 * to data such as logs, but that fits well into the whole telemetry paradigm.
 */
@NotThreadSafe
public class DataMetricLOB extends DataMetricBase<DataMetricLOB,DataMetricLOB.LOBData,DataMetricLOB.LOBListener> {

    /**
     * Constructs the base data metric.
     *
     * @param type     The metric type.
     * @param name     The metric name.
     * @param labelNames   The metric label names.
     * @param help     The metric help.
     * @param severity The metric severity.
     * @param data     The collected data.
     * @param urgent   The urgent flag.
     */
    protected DataMetricLOB(final @Nonnull MetricType type,
                            final @Nonnull String name,
                            final @Nonnull String[] labelNames,
                            final @Nonnull String help,
                            final @Nonnull Severity severity,
                            final @Nullable InputStream data,
                            final boolean urgent) {
        super(type, name, labelNames, help, severity, urgent);
        if ( null != data ) { // initialize the unlabeled LOB instance if this LOB is constructed with data
            labels().setData(data);
        }
    }

    /**
     * No-args constructor for GSON.
     */
    private DataMetricLOB() {
        super();
    }

    /**
     * Returns the collected data.
     * Will be closed by the reader, and must be fully available until then.
     *
     * @return The collected data.
     * @throws IOException In case of an error getting the data.
     */
    public @Nonnull InputStream getData() throws IOException {
        return labels().getData();
    }

    /**
     * Constructs the instance of the metric with all the default values.
     *
     * @return The instance of the metric with all the default values.
     */
    protected LOBData constructDefault( String[] labels) {
        return new LOBData(this, labels);
    }

    /**
     * Returns the builder.
     *
     * @return The builder.
     */
    public static @Nonnull Builder builder() {
        return new Builder();
    }

    /**
     * The scalar type builder.
     */
    public static class Builder extends DataMetricBuilder<Builder> {

        /**
         * The scalar value
         */
        private InputStream in_;

        /**
         * Sets the LOB metric.
         *
         * @param data The data.
         * @return The metric.
         */
        public Builder withData(final @Nullable InputStream data) {
            in_ = data;
            return this;
        }

        /**
         * Constructs the metric.
         *
         * @return The metric.
         */
        public DataMetricLOB build() {
            return new DataMetricLOB(MetricType.LOB, name_, labelNames_, help_,
                                     severity_, in_, urgent_);
        }
    }

    /**
     * Implements the {@code null} input stream, whereby the read will return the EOF.
     */
    static class NullInputStream extends InputStream {
        /**
         * Returns The <code>-1</code> to indicate the end of the
         * stream is reached.
         *
         * @return The <code>-1</code> to indicate the end of the
         * stream is reached.
         * @throws IOException if an I/O error occurs.
         */
        @Override
        public int read() throws IOException {
            return -1;
        }
    }

    /**
     * Holder of an LOB InputStream. To facilitate management of multiple labels input streams per
     * LOB metric.
     */
    static public class LOBData extends DataMetricBase.LabeledMetricData<DataMetricLOB> {
        /**
         * The data.
         */
        private InputStream data_;

        /**
         * Constructs a LOBData instance belonging to the specified metric, and labels with
         * the specified set of label values.
         *
         * @param owner  The metric that owns this LOB data
         * @param labels  The list of label values associated with this data. This should correspond
         *                to the list of label names defined in the metric owner.
         */
        public LOBData(DataMetricLOB owner, String[] labels) {
            super(owner, labels);
            data_ = new NullInputStream(); // initialize with null input stream
        }

        /**
         * Returns the collected data.
         * Will be closed by the reader, and must be fully available until then.
         *
         * @return The collected data.
         * @throws IOException In case of an error getting the data.
         */
        public @Nonnull InputStream getData() throws IOException {
            return data_;
        }

        /**
         * Sets the input stream to the specified value.
         *
         * @param newStream the new InputStream this data should reference.
         */
        public void setData( @Nonnull InputStream newStream ) {
            data_ = newStream;
            metric.getListeners().forEach(observer -> observer.LOBStreamChanged( this, newStream ));
        }
    }

    /**
     * Observer interface for monitoring changes to the LOB InputStream
     */
    public interface LOBListener {

        /**
         * Fired when the DataMetricLOB stream has been set.
         *
         * @param data  the LOBData instance that was updated
         * @param updatedStream  the new InputStream value
         */
        void LOBStreamChanged(final @Nonnull LOBData data, final InputStream updatedStream );
    }
}
