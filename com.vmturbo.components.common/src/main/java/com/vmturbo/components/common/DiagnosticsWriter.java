package com.vmturbo.components.common;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.common.diagnostics.DiagnosticsException;

/**
 * Handle the writing of diagnostics data.
 */
public class DiagnosticsWriter {

    private static final byte[] NL = "\n".getBytes();
    private final Logger logger = LogManager.getLogger();

    public static final String PROMETHEUS_METRICS_FILE_NAME = "PrometheusMetrics.diags";

    /**
     * Write a list of strings to a new {@link ZipEntry} in a zip output stream.
     * When the zip output stream gets written to a zip file, that zip file will
     * contain one text file for each zip entry created through this method, and
     * the text file will contain the strings added, one per line.
     * @param entryName the name of the text file to be created in the zip file.
     * @param values a {@link List} of {@link String}s to write
     * @param zos the {@link ZipOutputStream} to write to
     * @exception DiagnosticsException if there is an error creating the collection or writing to
     * to the stream
     */
    public void writeZipEntry(String entryName, List<String> values, ZipOutputStream zos)
        throws DiagnosticsException {
        writeZipEntry(entryName, values.stream(), zos);
    }

    /**
     * Write a stream of strings to a new {@link ZipEntry} in a zip output stream.
     * When the zip output stream gets written to a zip file, that zip file will
     * contain one text file for each zip entry created through this method, and
     * the text file will contain the strings added, one per line.
     *
     * @param entryName the name of the text file to be created in the zip file.
     * @param values a {@link Stream} of {@link String}s to write
     * @param zos the {@link ZipOutputStream} to write to
     * @exception DiagnosticsException if there is an error creating the collection or writing to
     * to the stream
     */
    public void writeZipEntry(String entryName, Stream<String> values, ZipOutputStream zos)
        throws DiagnosticsException {
        try {
            logger.info("Creating zip entry " + entryName);
            ZipEntry ze = new ZipEntry(entryName);
            ze.setTime(System.currentTimeMillis());
            zos.putNextEntry(ze);
            values.forEach(s -> {
                try {
                    zos.write(s.getBytes());
                    zos.write(NL);
                } catch (IOException e) {
                    logger.error(String.format("Exception trying to write \"%s\" to %s",
                        s, entryName), e);
                    throw new WriteZipEntryException(e);
                }
            });
            zos.closeEntry();
        } catch (WriteZipEntryException | IOException e) {
            final String errorMessage = String.format("Exception trying to create entry %s", entryName);
            logger.error(errorMessage, e);
            throw new DiagnosticsException(errorMessage, e);
        }
    }

    /**
     * Like {@link DiagnosticsWriter#writeZipEntry(String, List, ZipOutputStream)}, but writes
     * a byte array directly instead of a list of strings.
     *
     * @param entryName name for this entry in the zip stream
     * @param bytes the bytes to write to the zip entry
     * @param zos the zip output stream to write onto
     * @exception DiagnosticsException if there is an error creating the collection or writing to
     * to the stream
     */
    public void writeZipEntry(String entryName, byte[] bytes, ZipOutputStream zos)
        throws DiagnosticsException {
        try {
            logger.info("Creating zip entry " + entryName + " with byte array value.");
            ZipEntry ze = new ZipEntry(entryName);
            ze.setTime(System.currentTimeMillis());
            zos.putNextEntry(ze);
            zos.write(bytes);
            zos.closeEntry();
        } catch (IOException e) {
            final String errorMessage = String.format("Exception trying to create entry %s", entryName);
            logger.error(errorMessage, e);
            throw new DiagnosticsException(errorMessage);
        }
    }

    /**
     * Write prometheus metrics to the diagnosticsZip.
     *
     * @param collectorRegistry The registry of prometheus metrics whose contents should be written.
     * @param diagnosticZip the ZipOutputStream to dump diags to
     * @exception DiagnosticsException if there is an error creating the collection or writing to
     * to the stream
     * @exception DiagnosticsException if there is an error creating the collection or writing to
     * to the stream
     */
    public void writePrometheusMetrics(@Nonnull final CollectorRegistry collectorRegistry,
                                       @Nonnull final ZipOutputStream diagnosticZip)
        throws DiagnosticsException {
        try (StringWriter writer = new StringWriter()) {
            logger.info("Creating prometheus log entry " + PROMETHEUS_METRICS_FILE_NAME);
            TextFormat.write004(writer, collectorRegistry.metricFamilySamples());

            writeZipEntry(PROMETHEUS_METRICS_FILE_NAME,
                Collections.singletonList(writer.toString()),
                diagnosticZip);
        } catch (IOException e) {
            logger.error(e);
            throw new DiagnosticsException("Error writing Prometheus metrics.", e);
        }
    }

    /**
     * This unchecked exception is used to break out of the writeZipEntry stream processing lambda.
     */
    private static class WriteZipEntryException extends RuntimeException {
        WriteZipEntryException(Exception e) {
            super(e);
        }
    }
}
