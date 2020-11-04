package com.vmturbo.components.common.diagnostics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import com.google.protobuf.Message;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Handle the writing of diagnostics data. All the diagnostics errors are accumulated in
 * a local field {@code errors} for further processing.
 */
public class DiagnosticsWriter {

    private static final int WRITE_CHUNK_SIZE = 64 * 1024;

    private final Logger logger = LogManager.getLogger(getClass());
    private final ZipOutputStream zipStream;
    private final List<String> errors;

    /**
     * Constructs a writer to append to a specified ZIP stream.
     *
     * @param outputStream stream to append diagnostics data to
     */
    public DiagnosticsWriter(@Nonnull ZipOutputStream outputStream) {
        this.zipStream = Objects.requireNonNull(outputStream);
        errors = new ArrayList<>();
    }

    /**
     * Dumps data from a specified diagnosable object into the ZIP stream.
     *
     * @param diagnosable object to dump diagnostics information from
     */
    public void dumpDiagnosable(@Nonnull StringDiagnosable diagnosable) {
        final String zipEntry = getZipEntryName(diagnosable);
        logger.info("Creating zip entry {} with stream of strings", zipEntry);
        try (DiagnosticsAppenderImpl appender = new DiagnosticsAppenderImpl(zipStream, zipEntry)) {
            diagnosable.collectDiags(appender);
        } catch (DiagnosticsException e) {
            logger.error("Failed create dump for file " + diagnosable.getFileName()
                    + ". Will continue with others.", e);
            errors.addAll(e.getErrors());
        } catch (IOException e) {
            logger.error("Failed dumping diags into file " + diagnosable.getFileName(), e);
            errors.add(e.getLocalizedMessage());
        }
    }

    /**
     * Dumps data from a specified diagnosable object into the ZIP stream.
     *
     * @param diagnosable object to dump diagnostics information from
     */
    public void dumpDiagnosable(@Nonnull BinaryDiagnosable diagnosable) {
        final String zipEntry = getZipEntryName(diagnosable);
        logger.info("Creating zip entry {} with binary stream", zipEntry);
        try {
            final ZipEntry ze = new ZipEntry(zipEntry);
            ze.setTime(System.currentTimeMillis());
            zipStream.putNextEntry(ze);
            diagnosable.collectDiags(zipStream);
            zipStream.closeEntry();
        } catch (DiagnosticsException e) {
            logger.error("Failed create dump for file " + diagnosable.getFileName()
                    + ". Will continue with others.", e);
            errors.addAll(e.getErrors());
        } catch (IOException e) {
            logger.error("Failed dumping diags into file " + diagnosable.getFileName(), e);
            errors.add(e.getLocalizedMessage());
        }
    }

    /**
     * Returns ZIP entry name for the specified {@link Diagnosable}. Some diagnosable have
     * specific entry name suffixes in order to be parsed correctly by {@link DiagsZipReader}.
     * This method adds the suffix if required. Method simply returns
     * {@link Diagnosable#getFileName()} if the {@link Diagnosable} is not expected to be imported
     * later to restore component's state
     *
     * @param diagnosable diagnosable to get file name of
     * @return zip entry file name
     * @see Diagnosable#getFileName()
     * @see DiagsZipReader
     */
    @Nonnull
    public static String getZipEntryName(@Nonnull Diagnosable diagnosable) {
        if (diagnosable instanceof DiagsRestorable) {
            return diagnosable.getFileName() + DiagsZipReader.TEXT_DIAGS_SUFFIX;
        } else if (diagnosable instanceof BinaryDiagsRestorable) {
            return diagnosable.getFileName() + DiagsZipReader.BINARY_DIAGS_SUFFIX;
        } else {
            return diagnosable.getFileName();
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
     * <p>Make sure to add {@link DiagsZipReader#TEXT_DIAGS_SUFFIX} to the file name
     * if the data is expected to be used for restoring from diags.
     *
     * @param filename the name of the text file to be created in the zip file.
     * @param strings a {@link Iterator} of strings to write
     */
    public void writeZipEntry(@Nonnull String filename, @Nonnull Iterator<String> strings) {
        dumpDiagnosable(new StringDiagnosticsProvider(filename, strings));
    }

    /**
     * Write ZIP entry with the specified binary data to append.
     *
     * @param entryName ZIP entry name
     * @param bytes bytes to write
     */
    public void writeZipEntry(@Nonnull String entryName, @Nonnull byte[] bytes) {
        try {
            logger.info("Creating zip entry {} with byte array value [length={}].", entryName,
                    bytes.length);
            ZipEntry ze = new ZipEntry(entryName);
            ze.setTime(System.currentTimeMillis());
            zipStream.putNextEntry(ze);
            for (int offset = 0; offset < bytes.length; offset += WRITE_CHUNK_SIZE) {
                int toWrite = Math.min(bytes.length - offset, WRITE_CHUNK_SIZE);
                zipStream.write(bytes, offset, toWrite);
            }
            zipStream.closeEntry();
        } catch (IOException e) {
            final String errorMessage =
                    String.format("Exception trying to create entry %s", entryName);
            logger.error(errorMessage, e);
            errors.add(e.getLocalizedMessage());
        }
    }

    /**
     * Write ZIP entry with the specified protobuf message.
     *
     * @param entryName ZIP entry name
     * @param message protobuf message to write
     */
    public void writeZipEntry(@Nonnull String entryName, @Nonnull Message message) {
        try {
            logger.info("Creating zip entry {}.", entryName);
            ZipEntry ze = new ZipEntry(entryName);
            ze.setTime(System.currentTimeMillis());
            zipStream.putNextEntry(ze);
            message.writeTo(zipStream);
            zipStream.closeEntry();
        } catch (IOException e) {
            final String errorMessage =
                String.format("Exception trying to create entry %s", entryName);
            logger.error(errorMessage, e);
            errors.add(e.getLocalizedMessage());
        }
    }

    /**
     * Write entries based on the logic in
     * the {@link CustomDiagHandler#dumpToStream(ZipOutputStream)}.
     *
     * @param diagHandler CustomDiagHandler with logic to copy the zip entries
     */
    public void writeCustomEntries(CustomDiagHandler diagHandler) {
        diagHandler.dumpToStream(this.zipStream);
    }

    /**
     * Returns a list of errors faced during all the lifecycle of this writer.
     *
     * @return list of errors faced
     */
    @Nonnull
    public List<String> getErrors() {
        return Collections.unmodifiableList(errors);
    }

    /**
     * Internal class to wrap a stream of strings.
     */
    private static class StringDiagnosticsProvider implements StringDiagnosable {
        private final String filename;
        private final Iterator<String> data;

        StringDiagnosticsProvider(@Nonnull String filename, @Nonnull Iterator<String> data) {
            this.filename = Objects.requireNonNull(filename);
            this.data = Objects.requireNonNull(data);
        }

        @Override
        public void collectDiags(@Nonnull DiagnosticsAppender sink) throws DiagnosticsException {
            while (data.hasNext()) {
                final String string = data.next();
                sink.appendString(string);
            }
        }

        @Nonnull
        @Override
        public String getFileName() {
            return filename;
        }
    }
}
