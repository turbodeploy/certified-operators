package com.vmturbo.topology.processor.ldcf;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nonnull;

import com.vmturbo.proactivesupport.DataMetricLOB;
import com.vmturbo.topology.processor.diagnostics.TopologyProcessorDiagnosticsHandler;

/**
 * The {@link DataMetricTopology} implements the topology related metrics.
 */
public class DataMetricTopology extends DataMetricLOB {
    /**
     * The diagnostics handler.
     */
    private final TopologyProcessorDiagnosticsHandler handler_;

    /**
     * The anonymized flag.
     */
    private final boolean anonymized_;

    /**
     * Constructs the topology data collector.
     *
     * @param handler The diagnostic handler.
     */
    public DataMetricTopology(final @Nonnull TopologyProcessorDiagnosticsHandler handler,
                              final boolean anonymized) {
        super(MetricType.LOB, "topology.zip", new String[]{"topology"}, "The topology files.",
              Severity.INFO, null, false);
        anonymized_ = anonymized;
        handler_ = handler;
    }

    /**
     * Returns the collected data.
     * Will be closed by the reader, and must be fully available until then.
     *
     * @return The collected data.
     * @throws IOException In case of an error getting the data.
     */
    public @Nonnull InputStream getData() throws IOException {
        final File file = new File("/home/turbonomic/data/topology_collection.zip");
        try (FileOutputStream out = new FileOutputStream(file, false);
             ZipOutputStream zipOutputStream = new ZipOutputStream(out)) {
            if (anonymized_) {
                handler_.dumpAnonymizedDiags(zipOutputStream);
            } else {
                handler_.dump(zipOutputStream);
            }
        }
        return new AutoDeleteInputStream(file);
    }

    /**
     * The LogInputStream implements a file input stream.
     * The file will be deleted when closed.
     */
    static class AutoDeleteInputStream extends FileInputStream {

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
        public AutoDeleteInputStream(File file) throws FileNotFoundException {
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
