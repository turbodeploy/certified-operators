package com.vmturbo.components.common.diagnostics;

import java.io.IOException;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Diagnostics appender implementation putting the diagnostics in to a ZIP stream.
 */
public class DiagnosticsAppenderImpl implements DiagnosticsAppender, AutoCloseable {

    private static final byte[] NL = "\n".getBytes();
    private static final int WRITE_CHUNK_SIZE = 64 * 1024;
    private final Logger logger = LogManager.getLogger(getClass());

    private final ZipOutputStream stream;

    /**
     * Constructs appender for put data into the ZIP string with a specified ZIP entry name
     * (file name).
     *
     * @param stream strean to send output to
     * @param entryName zip entry name - file name inside the ZIP archive
     * @throws IOException on errors operating with the ZIP.
     */
    public DiagnosticsAppenderImpl(@Nonnull ZipOutputStream stream, @Nonnull String entryName)
            throws IOException {
        this.stream = Objects.requireNonNull(stream);
        logger.info("Creating zip entry {} with stream of strings", entryName);
        final ZipEntry ze = new ZipEntry(entryName);
        ze.setTime(System.currentTimeMillis());
        stream.putNextEntry(ze);
    }

    @Override
    public void appendString(@Nonnull String string) throws DiagnosticsException {
        try {
            for (String chunk : Splitter.fixedLength(WRITE_CHUNK_SIZE).split(string)) {
                stream.write(chunk.getBytes(Charsets.UTF_8));
            }
            stream.write(NL);
        } catch (IOException e) {
            throw new DiagnosticsException("Error writing string to output stream: " + string, e);
        }
    }

    @Override
    public void close() throws IOException {
        stream.closeEntry();
    }
}
