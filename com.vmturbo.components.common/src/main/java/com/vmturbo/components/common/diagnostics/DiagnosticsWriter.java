package com.vmturbo.components.common.diagnostics;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.ComponentGsonFactory;

/**
 * Handle the writing of diagnostics data.
 */
public class DiagnosticsWriter {

    private static final byte[] NL = "\n".getBytes();
    private static final int WRITE_CHUNK_SIZE = 64 * 1024;
    private static final Gson GSON = ComponentGsonFactory.createGson();
    private final Logger logger = LogManager.getLogger();

    @VisibleForTesting
    static final String PROMETHEUS_METRICS_FILE_NAME = "PrometheusMetrics.diags";

    /**
     * Write a single string to a new {@link ZipEntry} in a zip output stream.
     * When the zip output stream gets written to a zip file, that zip file will
     * contain one text file for each zip entry created through this method, and
     * the text file will contain the strings added, one per line.
     * @param entryName the name of the text file to be created in the zip file.
     * @param value a {@link String} to write
     * @param zos the {@link ZipOutputStream} to write to
     * @exception DiagnosticsException if there is an error creating the collection or writing to
     * to the stream
     */
    public void writeZipEntry(String entryName, String value, ZipOutputStream zos)
        throws DiagnosticsException {
        writeZipEntry(entryName, Stream.of(value), zos);
    }

    /**
     * Write a list of strings to a new {@link ZipEntry} in a zip output stream.
     * When the zip output stream gets written to a zip file, that zip file will
     * contain one text file for each zip entry created through this method, and
     * the text file will contain the strings added, one per line.
     * @deprecated should be replaced by call to pass a Stream of objects to be serialized
     * @param entryName the name of the text file to be created in the zip file.
     * @param values a {@link List} of {@link String}s to write
     * @param zos the {@link ZipOutputStream} to write to
     * @exception DiagnosticsException if there is an error creating the collection or writing to
     * to the stream
     */
    @Deprecated
    public void writeZipEntry(String entryName, List<String> values, ZipOutputStream zos)
        throws DiagnosticsException {
        writeZipEntry(entryName, values.stream(), zos);
    }

    /**
     * Write a stream of strings to a new {@link ZipEntry} in a zip output stream.
     * When the zip output stream gets written to a zip file, that zip file will
     * contain one text file for each zip entry created through this method, and9
     * the text file will contain the strings added, one per line.
     *
     * <p>Note that a String in the stream may be very long, so to avoid overflowing
     * buffers we break the string into chunks.
     *
     * @deprecated replace calls with the next form with a stream of objects to be
     * lazily serialized, rather than stream of strings
     * @param entryName the name of the text file to be created in the zip file.
     * @param values a {@link Stream} of {@link String}s to write
     * @param zos the {@link ZipOutputStream} to write to
     * @exception DiagnosticsException if there is an error creating the collection or writing to
     * to the stream
     */
    @Deprecated()
    public void writeZipEntry(String entryName, Stream<String> values, ZipOutputStream zos)
        throws DiagnosticsException {
        try {
            logger.info("Creating zip entry {} with stream of strings", entryName);
            ZipEntry ze = new ZipEntry(entryName);
            ze.setTime(System.currentTimeMillis());
            zos.putNextEntry(ze);
            values.forEach(s -> {
                try {
                    for (String chunk : Splitter.fixedLength(WRITE_CHUNK_SIZE).split(s)) {
                        zos.write(chunk.getBytes());
                    }
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
     * Write a stream of strings to a new {@link ZipEntry} in a zip output stream.
     * When the zip output stream gets written to a zip file, that zip file will
     * contain one text file for each zip entry created through this method, and
     * the text file will contain the strings added, one per line.
     *
     * <p>Note that a String in the stream may be very long, so to avoid overflowing
     * buffers we break the string into chunks.
     *
     * @param <T> The type of the object in the stream
     * @param entryName the name of the text file to be created in the zip file.
     * @param objectsToWrite a {@link Stream} of objects of the given type to write
     * @param typeOfObject the datatype of the objects in the stream; passed  to GSON.toJson()
     * @param zos the {@link ZipOutputStream} to write to
     * @exception DiagnosticsException if there is an error creating the collection or writing to
     * to the stream
     */
    public <T> void writeZipEntry(@Nonnull final String entryName,
                              @Nonnull final Stream<T> objectsToWrite,
                              @Nonnull final Class<T> typeOfObject,
                              @Nonnull final ZipOutputStream zos) throws DiagnosticsException {
        try {
            logger.info("writing zip entry: '{}'  [stream of {}]", entryName,
                typeOfObject.getCanonicalName());
            ZipEntry ze = new ZipEntry(entryName);
            ze.setTime(System.currentTimeMillis());
            zos.putNextEntry(ze);
            JsonWriter writer = new JsonWriter(new OutputStreamWriter(zos, StandardCharsets.UTF_8));
            objectsToWrite.forEach((objectToWrite) -> {
                GSON.toJson(objectToWrite, typeOfObject, writer);
                try {
                    writer.flush();
                    // add a newline between JSON objects
                    zos.write("\n".getBytes());
                } catch (IOException e) {
                    throw new WriteZipEntryException(e);
                }
            });
            writer.flush();
            zos.closeEntry();
        } catch (IOException | WriteZipEntryException e) {
            throw new DiagnosticsException("Error writing object of type " +
                typeOfObject.getTypeName(), e);
        }
    }

    /**
     * Like {@link DiagnosticsWriter#writeZipEntry(String, Stream, ZipOutputStream)},
     * but writes a byte array directly instead of a list of strings.
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
            logger.info("Creating zip entry {} with byte array value [length={}].", entryName,
                bytes.length);
            ZipEntry ze = new ZipEntry(entryName);
            ze.setTime(System.currentTimeMillis());
            zos.putNextEntry(ze);
            for (int offset = 0; offset < bytes.length; offset += WRITE_CHUNK_SIZE) {
                int toWrite = Math.min(bytes.length - offset, WRITE_CHUNK_SIZE);
                zos.write(bytes, offset, toWrite);
            }
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
                writer.toString(),
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
