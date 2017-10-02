package com.vmturbo.proactivesupport;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * The DataAnalyzer implements the data analyzer.
 */
public interface DataAnalyzer {
    /**
     * Returns the collector types this analyzer is interested in.
     *
     * @return The collector types this analyzer is interested in.
     */
    @Nonnull Iterable<String> getCollectorTypes();

    /**
     * Performs the data analysis.
     *
     * @param data The input data.
     * @return The data collector message.
     * @throws IOException In the case of an error reading the data.
     */
    @Nonnull DataMetric analyze(final @Nonnull Map<String, DataMetric> data)
            throws IOException;
}
