package com.vmturbo.clustermgr.aggregator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.TimeZone;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;
import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.proactivesupport.DataMetric;
import com.vmturbo.proactivesupport.DataMetricLOB;
import com.vmturbo.proactivesupport.communications.ServerChannelListener;

/**
 * The DataAggregator implements proactive support data aggregator.
 * The data aggregator will start both local and remote receivers.
 * The local receiver will accept local calls. It is specific to the Cluster Manager
 * component.
 * The current base will not be recovered after the restart, since we do not have a guaranteed delivery.
 * TODO(MB): Make sure that has been taken care of in case guaranteed delivery becomes a requirement.
 */
@Component
@ThreadSafe
public class DataAggregator {
    /**
     * The logger.
     */
    private final Logger logger_ = LogManager.getLogger(DataAggregator.class);

    /**
     * The data aggregation location for the current collection.
     */
    @VisibleForTesting
    File currentBase_;

    private final String aggregatorBasePath;

    /**
     * Constructs the data aggregator.
     * @param aggregatorBasePath Base path for the aggregation files.
     */
    public DataAggregator(String aggregatorBasePath) {
        this.aggregatorBasePath = aggregatorBasePath;
        startAggregationInterval();
    }

    /**
     * Returns the current base.
     *
     * @return The current base.
     */
    public synchronized File getCurrentBase() {
        return currentBase_;
    }

    /**
     * Starts the new aggregation interval.
     *
     * @return The previous base point.
     */
    public synchronized File startAggregationInterval() {
        File current = getCurrentBase();
        Calendar calendar = Calendar.getInstance();
        currentBase_ = new File(aggregatorBasePath + "/" +
                                calendar.get(Calendar.YEAR) + "_" +
                                calendar.get(Calendar.MONTH) + "_" +
                                calendar.get(Calendar.DAY_OF_MONTH) + "_" +
                                calendar.get(Calendar.HOUR_OF_DAY) + "_" +
                                calendar.get(Calendar.MINUTE));
        if (!currentBase_.isDirectory() && !currentBase_.mkdirs()) {
            throw new IllegalStateException("Unable to create required base directory: "
                                            + currentBase_.getAbsolutePath());
        }
        return current;
    }

    /**
     * Returns the output file for the file name.
     *
     * @param name The file name.
     * @return The file.
     */
    private @Nonnull synchronized File outputFile(final @Nonnull String name) {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateFormat = new SimpleDateFormat("dd_MM_yyyy_HH_mm_ss");
        dateFormat.setTimeZone(calendar.getTimeZone());
        String datePrefix = dateFormat.format(calendar.getTime());
        return new File(currentBase_.getAbsolutePath() + "/" + datePrefix + "_" + name);
    }

    /**
     * Constructs the file output stream for binary data.
     *
     * @param file The file.
     * @return The file output stream.
     * @throws IOException If the writer could not be constructed.
     */
    private FileOutputStream constructOutputStream(final @Nonnull File file) throws IOException {
        return new FileOutputStream(file);
    }

    /**
     * Persists the LOB message.
     * Each message will persisted as a separate file.
     *
     * @param metric The LOB metric.
     */
    @VisibleForTesting
    void persistLOBMessage(final @Nonnull DataMetric metric) {
        try (FileOutputStream out = constructOutputStream(outputFile(metric.getName()));
             InputStream in = ((DataMetricLOB)metric).getData()) {
            IOUtils.copy(in, out);
        } catch (IOException e) {
            logger_.error("Unable to persist the data for {}", metric.getName());
        }
    }

    /**
     * Receives the offline collection of messages over to the Data Aggregator.
     *
     * @param messages The collection of messages.
     */
    public void receiveLocalOffline(final @Nonnull Collection<DataMetric> messages) {
        // Persist the data for each message
        messages.forEach(this::persistLOBMessage);
    }

    /**
     * Returns the listener instance.
     *
     * @return The listener instance.
     */
    public ServerChannelListener listenerInstance() {
        return new CallbackListener();
    }

    /**
     * The Listener implements the server channel listener and handles the transmission
     * from a single target.
     */
    @NotThreadSafe
    private class CallbackListener implements ServerChannelListener {
        /**
         * The target file.
         */
        private File file_;

        /**
         * The output stream built from the {@link #file_}.
         */
        private FileOutputStream out_;

        /**
         * Opens the file with the appropriate start.
         * The start acts as a file name where the content of the message will be stored.
         *
         * @param id The received start.
         * @throws IOException In case of an error opening the file for writing.
         */
        @Override
        public void start(final @Nonnull String id) throws IOException {
            if (file_ != null || out_ != null) {
                return;
            }
            file_ = outputFile(id);
            out_ = constructOutputStream(file_);
        }

        /**
         * Writes the next frame into the file.
         *
         * @param data The next received frame of data.
         * @throws IOException In case of an error writing the data.
         */
        @Override
        public void nextFrame(@Nonnull byte[] data) throws IOException {
            try {
                out_.write(data, 0, data.length);
            } catch (IOException e) {
                error();
                throw e;
            }
        }

        /**
         * Closes the file.
         *
         * @throws IOException In case of an error (won't happen here).
         */
        @Override
        public void complete() throws IOException {
            IOUtils.closeQuietly(out_);
            out_ = null;
            file_ = null;
        }

        /**
         * Closes and deletes the file.
         *
         * @throws IOException In case of an error (won't happen here).
         */
        @Override
        public void error() throws IOException {
            IOUtils.closeQuietly(out_);
            out_ = null;
            if (!file_.delete()) {
                logger_.debug("Unable to delete file {}", file_.getAbsolutePath());
            }
            file_ = null;
        }
    }
}
