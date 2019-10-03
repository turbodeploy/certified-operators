package com.vmturbo.components.common;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;

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
     * @throws IOException if an I/O error occurs
     */
    public void writeZipEntry(String entryName, List<String> values, ZipOutputStream zos) {
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
     * @throws IOException if an I/O error occurs
     */
    public void writeZipEntry(String entryName, Stream<String> values, ZipOutputStream zos) {
        try {
            logger.debug("Creating zip entry " + entryName + " with a stream of values.");
            ZipEntry ze = new ZipEntry(entryName);
            ze.setTime(System.currentTimeMillis());
            zos.putNextEntry(ze);
            values.forEach(s -> writeString(entryName, s, zos)); // entryName passed just for logging
            zos.closeEntry();
        } catch (IOException e) {
            logger.error(String.format("Exception trying to create entry %s", entryName), e);
        }
    }

    /**
     * Like {@link DiagnosticsWriter#writeZipEntry(String, List, ZipOutputStream)}, but writes
     * a byte array directly instead of a list of strings.
     */
    public void writeZipEntry(String entryName, byte[] bytes, ZipOutputStream zos) {
        try {
            logger.debug("Creating zip entry " + entryName + " with byte array value.");
            ZipEntry ze = new ZipEntry(entryName);
            ze.setTime(System.currentTimeMillis());
            zos.putNextEntry(ze);
            zos.write(bytes);
            zos.closeEntry();
        } catch (IOException e) {
            logger.error(String.format("Exception trying to create entry %s", entryName), e);
        }
    }

    /**
     * Write prometheus metrics to the diagnosticsZip.
     *
     * @param collectorRegistry The registry of prometheus metrics whose contents should be written.
     * @param diagnosticZip the ZipOutputStream to dump diags to
     */
    public void writePrometheusMetrics(@Nonnull final CollectorRegistry collectorRegistry,
                                       @Nonnull final ZipOutputStream diagnosticZip) {
        try (StringWriter writer = new StringWriter()) {
            TextFormat.write004(writer, collectorRegistry.metricFamilySamples());

            writeZipEntry(PROMETHEUS_METRICS_FILE_NAME,
                Collections.singletonList(writer.toString()),
                diagnosticZip);
        } catch (IOException e) {
            logger.error(e);
        }
    }

    private void writeString(String entryName, String s, ZipOutputStream zos) {
        try {
            zos.write(s.getBytes());
            zos.write(NL);
        } catch (IOException e) {
            logger.error(String.format("Exception trying to write \"%s\" to %s", s, entryName), e);
        }
    }

}
