package com.vmturbo.components.common.diagnostics;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.annotation.Nonnull;

import com.google.common.base.Charsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.common.diagnostics.RecursiveZipIterator.WrappedZipEntry;

/**
 * Class to scan a {@link ZipInputStream} and deliver its entries, along with the entries of any embedded
 * zip files, and so on.
 *
 * <p> Entries are returned in the form of {@link WrappedZipEntry} instances, which are just
 * {@link ZipEntry} instances with a method to retrieve their content. (A normal {@link ZipEntry}
 * does not provide a means to access its underlying {@link ZipInputStream}, which must be read to
 * obtain the entry's content.)</p>
 */
public class RecursiveZipIterator implements Iterator<WrappedZipEntry>, AutoCloseable {
    private static final Logger logger = LogManager.getLogger();

    public static final String ZIP_EXTENSION = ".zip";

    private final ZipInputStream zipStream;

    // this should be the next value to return, or null if we're done (or, temporarily, if we
    // haven't yet computed the next value)
    private WrappedZipEntry nextValue = null;

    // if not null, this is an iterator that can deliver entries from an embedded zip file that
    // is the most recent entry encountered in this stream
    private RecursiveZipIterator subIterator = null;

    /**
     * Create an iterator for the given zip stream.
     *
     * <p>The stream is closed when the iterator is exhausted, or
     * at finalization if the iterator is abandoned before exhaustion.</p>
     *
     * @param zipStream the zip stream whose entries are to be delivered
     */
    RecursiveZipIterator(@Nonnull ZipInputStream zipStream) {
        this.zipStream = Objects.requireNonNull(zipStream);
    }

    @Override
    public boolean hasNext() {
        if (nextValue == null) {
            produceNextValue();
        }
        return nextValue != null;
    }

    @Override
    public WrappedZipEntry next() {
        if (hasNext()) {
            try {
                return nextValue;
            } finally {
                nextValue = null;
            }
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    protected void finalize() {
        // make sure we close any open streams, in case we're abandoned before we've delivered all
        // our entries
        this.close();
    }

    /**
     * This method must either extract the next entry from this stream and leave it as the value of
     * nextValue, or leave nextValue set to null.
     *
     * <p>If an entry in this stream represents an embedded Zip file, a subordinate
     * {@link RecursiveZipIterator} is created, and while it has entries we deliver them instead
     * of advancing through our own stream.</p>
     */
    private void produceNextValue() {
        while (nextValue == null) {
            if (subIterator != null) {
                // check subordinate iterator if one is active
                if (subIterator.hasNext()) {
                    // use next subiterator value if it has one
                    this.nextValue = subIterator.next();
                } else {
                    // but if it's exhausted, drop it and loop to try again
                    this.subIterator = null;
                }
            } else {
                try {
                    // no subiterator, get next entry from our stream
                    ZipEntry nextEntry = zipStream.getNextEntry();
                    if (nextEntry != null) {
                        // got something - wrap it so we retain ability to load its content
                        WrappedZipEntry wrappedEntry = new WrappedZipEntry(nextEntry);
                        if (isEmbeddedZip(nextEntry)) {
                            // embedded ZIP file - get its content as an input stream
                            final ByteArrayInputStream baiStream =
                                new ByteArrayInputStream(wrappedEntry.getBytes());
                            // and create an iterator to get its entries
                            this.subIterator = new RecursiveZipIterator(new ZipInputStream(baiStream));
                            // leave nextValue null, so next loop iteration will ask the new
                            // subIterator to supply a value
                        } else {
                            // we got a regular entry - tee it up for retrieval
                            this.nextValue = wrappedEntry;
                        }
                    } else {
                        // nothing left in this stream, leave nextValue null
                        return;
                    }
                } catch (IOException e) {
                    logger.warn("Failed to process zip file; discarding any remaining entries: {}",
                        e.toString());
                    // send back an exception entry so the caller can see the exception
                    nextValue = new WrappedZipEntry(e);
                }
            }
        }
    }

    @Override
    public void close() {
        // recursively close all active subiterators
        if (subIterator != null) {
            subIterator.close();
        }
        // and close our own stream
        closeStream();
    }

    /**
     * Try to close our input stream if it's still active
     */
    private void closeStream() {
        try {
            zipStream.close();
        } catch (IOException e) {
            // we tried...
        }
    }

    /**
     * Check if this entry represents an embedded zip file
     *
     * <p>A non-directory with an entry name that ends with ".zip" (case-insensitive) is treated
     * as an embedded zip file</p>
     *
     * @param entry the entry to check
     * @return true if it's an embedded zip file
     */
    private static boolean isEmbeddedZip(ZipEntry entry) {
        return !entry.isDirectory() && entry.getName().toLowerCase().endsWith(ZIP_EXTENSION);
    }

    /**
     * This class extends {@link ZipEntry} with a method to retrieve its content from the
     * {@link ZipInputStream} in which it was found.
     *
     * <p>This is achieved by reading from the stream saved in the enclosing class's zipStream
     * field.</p>
     *
     * <p>Note that if the content is to be retrieved, that must happen before the stream has been
     * used to deliver its next entry, or before any other operation that would move the stream
     * pointer is performed. Ths is unfortunately not something that can be checked, so
     * unpredictable results could arise in code that violates this requirement.</p>
     *
     * <p>This class is also capable of wrapping an exception, which should be detected by the
     * consumer and not processed as a normal entry. The iterator will never deliver another value
     * after such a value. The appearance of an "exception" value is a signal to the consumer
     * that the iterator terminated abnormally, and the wrapped exception is the cause of that
     * termination.</p>
     */
    public class WrappedZipEntry extends ZipEntry {
        // a reasonable-seeming size for a zip entry reading buffer
        private static final int BUFFER_SIZE = 16 * 1024;
        // exception that's responsible for abnormal termination, if that happens
        private Throwable throwable = null;

        /**
         * Create a new wrapped entry identical to a given entry
         *
         * @param entry the entry to wrap
         */
        WrappedZipEntry(final ZipEntry entry) {
            // Use ZipEntry's existing cloning constructor
            super(entry);
        }

        /**
         * Create an entry that's really a marker for an exception that was caught when scanning
         * for entries or retrieving entry content.
         *
         * <p>The Iterator and Iterable interfaces don't provide any means to propagate anything
         * other than runtime exceptions, so all information conveyed by an iterator must be
         * passed via delivered elements. This hack gives us a way to do that in this case.</p>
         *
         * <p>Of course, callers must check retrieved entries before processing them as if they're
         * normal entries, using {#isException()}.</p>
         *
         * @param throwable the exception to wrap
         */
        WrappedZipEntry(Throwable throwable) {
            super("_");
            this.throwable = throwable;
        }

        /**
         * Retrieve the content of this entry in binary form, by reading from the underlying
         * {@link ZipInputStream}.
         *
         * @return the entry's content, as a byte array
         * @throws IOException if we fail to obtain the content from the zip stream
         */
        public byte[] getBytes() throws IOException {
            if (throwable != null) {
                throw new IOException(
                    "Attempted to read ZipEntry content from exception-bearing fake entry",
                    throwable);
            }
            ByteArrayOutputStream baoStream =
                new ByteArrayOutputStream((int) Math.max(BUFFER_SIZE, getSize()));
            int count;
            byte[] buffer = new byte[1024];
            while ((count = zipStream.read(buffer, 0, buffer.length)) != -1) {
                baoStream.write(buffer, 0, count);
            }
            return baoStream.toByteArray();
        }

        /**
         * Get the lines stream from the current ZipEntry.
         *
         * @return lines in the entry, as a stream of strings
         * @throws IOException if we fail to obtain the content from the zip stream
         */
        public Stream<String> getLines() throws IOException {
            if (throwable != null) {
                throw new IOException(
                        "Attempted to read ZipEntry content from exception-bearing fake entry",
                        throwable);
            }
            return new BufferedReader(new InputStreamReader(zipStream, Charsets.UTF_8)).lines();
        }

        /**
         * Check whether this is a special exception-bearing entry.
         *
         * @return true of so, else it's a normal entry
         */
        public boolean isExceptionEntry() {
            return throwable != null;
        }

        /**
         * Return the exception borne by an exception-bearing entry.
         *
         * @return exception, or null if this is a normal entry
         */
        public Throwable getException() {
            return throwable;
        }
    }
}
