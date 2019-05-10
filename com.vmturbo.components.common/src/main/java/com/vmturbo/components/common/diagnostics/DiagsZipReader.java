package com.vmturbo.components.common.diagnostics;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;

import com.google.common.base.Charsets;

import com.vmturbo.components.common.diagnostics.RecursiveZipIterator.WrappedZipEntry;

/**
 * This class reads Diags entries from a diags file and creates an iterable that delivers them.
 *
 * <p>The outermost file is expected to be a ZIP file. File entries are treated based on their
 * file extension, as in:
 * <dl>
 *     <dt>.binary</dt>
 *     <dd>Content is a byte array comprising a binary diags object</dd>
 *     <dt>.diags</dt>
 *     <dd>Content is UTF-8 text comprising a text diags object</dd>
 *     <dt>.zip</dt>
 *     <dd>This is an embedded zip file that is scanned for diags (and further embedded zips), which
 *     are delivered before continuing with this file's diags objects.</dd>
 * </dl>
 * Other entries, including directory entries, are ignored.
 * </p>
 * <p>If an exception is encountered during this processing, the iterable stops delivering new
 * diags objects, and the exception can be obtained using the {@link #getException} method.
 * Otherwise this case is indistinguishable from coming having fully scanned the input.
 * </p>
 *
 */
public class DiagsZipReader implements Iterable<Diags> {

    public static final String TEXT_DIAGS_SUFFIX = ".diags";
    public static final String BINARY_DIAGS_SUFFIX = ".binary";
    private final DiagsIterator diagsIterator;

    /**
     * Create a new instance to scan the given input stream.
     *
     * @param inputStream input stream to be scanned
     */
    public DiagsZipReader(InputStream inputStream) {
        final RecursiveZipIterator zipIterator = new RecursiveZipIterator(new ZipInputStream(inputStream));
        this.diagsIterator = new DiagsIterator(zipIterator);
    }

    @Override
    public Iterator<Diags> iterator() {
        return diagsIterator;
    }

    /**
     * Obtain the exception that caused the zip iterator to terminate early, if that happened.
     *
     * @return iterator-terminating exception, or null if that did not happen
     */
    public Throwable getException() {
        return diagsIterator.getException();
    }

    /**
     * Iterator class supporting the top-level Iterable
     */
    private static class DiagsIterator implements Iterator<Diags> {

        private RecursiveZipIterator zipIterator;
        private Diags nextValue = null;
        private Throwable exception = null;

        /**
         * Create a new instance, given a zip file iterator.
         *
         * @param zipIterator iterator to deliver zip file entries
         */
        DiagsIterator(RecursiveZipIterator zipIterator) {
            this.zipIterator = zipIterator;
            // tee up first value for delivery
            produceNextValue();
        }

        @Override
        public boolean hasNext() {
            return nextValue != null;
        }

        @Override
        public Diags next() {
            if (nextValue != null) {
                try {
                    return nextValue;
                } finally {
                    // tee up next value to deliver, while returning current value
                    produceNextValue();
                }
            } else {
                throw new NoSuchElementException();
            }
        }

        private void produceNextValue() {
            nextValue = null;
            while (nextValue == null && zipIterator.hasNext()) {
                // Get next entry from zip file (or recursively embedded zip file)
                WrappedZipEntry nextEntry = zipIterator.next();
                if (nextEntry.isExceptionEntry()) {
                    // exception reported by zip iterator, save exception and terminate iterator
                    handleException(nextEntry.getException());
                    return;
                }
                if (isDiagsEntry(nextEntry)) {
                    try {
                        // parse the diags entry and stage it for delivery
                        nextValue = toDiags(nextEntry);
                    } catch (IOException e) {
                        // failed to parse diags entry - terminate the iterator early
                        handleException(e);
                        return;
                    }
                }
            }
            if (nextValue == null) {
                // tie off zip stream once we've gotten everything
                zipIterator.close();
            }
        }

        /**
         * Check whether the given zip file entry looks like a diags entry
         *
         * <p>Any normal file entry with one of our recognized extensions qualiifies.</p>
         *
         * @param entry the entryt to be checked
         * @return true if it appears to be a diags entry
         */
        private boolean isDiagsEntry(WrappedZipEntry entry) {
            return !entry.isExceptionEntry() && !entry.isDirectory() &&
                (isTextDiags(entry) || isBinaryDiags(entry));
        }

        /**
         * Create a {@link Diags} object from the given zip file entry.
         *
         * @param entry the zip entry
         * @return the parsed Diags object
         * @throws IOException if the conversion fails
         */
        private Diags toDiags(WrappedZipEntry entry) throws IOException {
            byte[] content = entry.getContent();
            if (isTextDiags(entry)) {
                // text diags object encapsulates a list of lines of text
                List<String> lines = IOUtils.readLines(new ByteArrayInputStream(content),
                    Charsets.UTF_8);
                return new Diags(entry.getName(), lines);

            } else if (isBinaryDiags(entry)) {
                // a binary diags object encompases the raw zip entry content
                return new Diags(entry.getName(), content);
            } else {
                // should never happen, but if it does, treat it as an iterator-terminating condition
                handleException(new IllegalStateException(("Invalid file extension for diags entry")));
                return null;
            }
        }

        /**
         * Shut down the iterator and stash the exception that's the casue.
         *
         * @param e exception causing the iterator to terminate early
         */
        private void handleException(Throwable e) {
            this.exception = e;
            // close the zip file iterator and make sure we don't try to use it again
            zipIterator.close();
            this.zipIterator = null;
        }

        /**
         * Return the exception that caused this iterator to terminate early
         *
         * @return the terminating exception, or null if that didn't happen
         */
        public Throwable getException() {
            return exception;
        }

        /**
         * Check whether the given zip file entry looks like a text diags entry
         *
         * @param entry the entry to be checked, which must be a normal file entry
         * @return true if it looks like a text diags entry
         */
        private static boolean isTextDiags(ZipEntry entry) {
            return entry.getName().toLowerCase().endsWith(TEXT_DIAGS_SUFFIX);
        }

        /**
         * Check whether the given zip file entry looks like a binray diags entry
         *
         * @param entry the entry to be checked, which must be a normal file entry
         * @return true if it looks like a binary diags entry
         */
        private static boolean isBinaryDiags(ZipEntry entry) {
            return entry.getName().toLowerCase().endsWith(BINARY_DIAGS_SUFFIX);
        }
    }
}
