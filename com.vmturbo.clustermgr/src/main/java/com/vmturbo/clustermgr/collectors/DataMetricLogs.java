package com.vmturbo.clustermgr.collectors;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;

import com.vmturbo.clustermgr.ClusterMgrService;
import com.vmturbo.proactivesupport.DataMetricLOB;

import org.springframework.http.MediaType;

/**
 * The {@link DataMetricLogs} implements the log file data collector.
 */
public class DataMetricLogs extends DataMetricLOB {
    /**
     * The cluster manager service
     */
    private final ClusterMgrService clusterMgrService_;

    /**
     * The accepted types for the REST calls.
     */
    private static final String ACCEPT_TYPES = MediaType.toString(ImmutableList.of(
            MediaType.valueOf("application/zip"),
            MediaType.APPLICATION_OCTET_STREAM));

    /**
     * Constructs the log data collector.
     *
     * @param clusterMgrService The cluster manager service.
     */
    public DataMetricLogs(final ClusterMgrService clusterMgrService) {
        super(MetricType.LOB, "logs.tar.gz", new String[]{"logs"}, "The log files.", Severity.INFO,
              null, false);
        clusterMgrService_ = clusterMgrService;
    }

    /**
     * Returns the collected data.
     * Will be closed by the reader, and must be fully available until then.
     *
     * @return The collected data.
     * @throws IOException In case of an error getting the data.
     */
    public @Nonnull
    InputStream getData() throws IOException {
        final File f = new File("/tmp/logs_collection.tar.gz");
        try (FileOutputStream fileOutputStream = new FileOutputStream(f, false)) {
            clusterMgrService_.getRsyslogProactive(fileOutputStream, ACCEPT_TYPES);
        }
        return new LogInputStream(f);
    }

    /**
     * The LogInputStream implements a file input stream.
     * The file will be deleted when closed.
     */
    static class LogInputStream extends FileInputStream {

        /**
         * The original file.
         */
        private final File file_;

        /**
         * Constructs the input stream.
         *
         * @param file The file to be read.
         * @throws FileNotFoundException If the file is not found.
         */
        public LogInputStream(File file) throws FileNotFoundException {
            super(file);
            file_ = file;
        }

        @Override
        public void close() throws IOException {
            super.close();
            file_.delete();
        }
    }
}
